use anyhow::{Ok, Result};
use aws_sdk_s3::Client as S3Client;
use colored::Colorize;

#[cfg(not(feature = "with-clap"))]
use inquire::{Confirm, Text};

#[cfg(feature = "with-clap")]
use clap::{Parser, Subcommand};
use rust_cdc_validator::{
    cdc::{
        cdc_operator::CDCOperator, cdc_operator_payload::CDCOperatorPayload,
        snapshot_payload::CDCOperatorSnapshotPayload, validate_payload::CDCOperatorValidatePayload,
    },
    dataframe::dataframe_ops::DataframeOperatorImpl,
    postgres::{
        postgres_config::PostgresConfig, postgres_operator::PostgresOperator,
        postgres_ops::PostgresOperatorImpl,
    },
    s3::s3_ops::S3OperatorImpl,
};
use tracing::info;

#[cfg(feature = "with-clap")]
#[derive(Parser)]
#[command(author, version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[cfg(feature = "with-clap")]
#[derive(Subcommand)]
enum Commands {
    Validate {
        /// S3 Bucket name where the CDC files are stored
        #[arg(long, required = true)]
        bucket_name: String,
        /// S3 Prefix where the files are stored
        /// Example: data/landing/rds/mydb
        #[arg(long, required = true)]
        s3_prefix: String,
        /// Url of the database to validate the CDC files
        /// Example: postgres://postgres:postgres@localhost:5432/mydb
        #[arg(long, required = true)]
        source_postgres_url: String,
        /// Url of the target database to import the parquet files
        /// Example: postgres://postgres:postgres@localhost:5432/mydb
        #[arg(long, required = true)]
        target_postgres_url: String,
        /// Schema of database to validate against S3 files
        #[arg(long, required = false, default_value = "public")]
        database_schema: String,
        /// List of table names to validate against S3 files
        #[arg(long, value_delimiter = ',', num_args = 0.., required = true)]
        table_names: Vec<String>,
        /// Start date to filter the Parquet files
        /// Example: 2024-02-14T10:00:00Z
        #[arg(long, required = true, default_value = "2024-02-14T10:00:00Z")]
        start_date: String,
        /// Stop date to filter the Parquet files
        /// Example: 2024-02-14T10:00:00Z
        #[arg(long, required = false)]
        stop_date: Option<String>,
        /// Datadiff chunk size
        #[arg(long, required = false, default_value = "1000")]
        chunk_size: i64,
        /// Maximum connection pool size
        #[arg(long, required = false, default_value = "100")]
        max_connections: u32,
        /// Datadiff start position
        #[arg(long, required = false, default_value = "0")]
        start_position: i64,
        /// Run only the datadiff
        #[arg(
            long,
            required = false,
            default_value = "false",
            conflicts_with("only_snapshot")
        )]
        only_datadiff: bool,
        /// Take only a snapshot from S3 to target DB
        #[arg(
            long,
            required = false,
            default_value = "false",
            conflicts_with("only_datadiff")
        )]
        only_snapshot: bool,
    },
}

#[cfg(feature = "with-clap")]
fn main_clap() -> Result<ValidatorPayload> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Validate {
            bucket_name,
            s3_prefix,
            source_postgres_url,
            target_postgres_url,
            database_schema,
            table_names,
            start_date,
            stop_date,
            chunk_size,
            max_connections,
            start_position,
            only_datadiff,
            only_snapshot,
        } => {
            let payload = ValidatorPayload::new(
                bucket_name,
                s3_prefix,
                source_postgres_url,
                target_postgres_url,
                database_schema,
                table_names,
                start_date,
                stop_date,
                chunk_size,
                max_connections,
                start_position,
                only_datadiff,
                only_snapshot,
            );

            Ok(payload)
        }
    }
}

#[cfg(not(feature = "with-clap"))]
fn main_inquire() -> Result<CDCOperatorPayload> {
    let bucket_name = Text::new("S3 Bucket name")
        .with_default("bucket_name")
        .with_help_message("Enter the S3 bucket where the CDC files are stored")
        .prompt()?;

    let s3_prefix = Text::new("S3 Prefix")
        .with_default("data/landing/rds/mydb")
        .with_help_message("Enter the S3 prefix where the files are stored")
        .prompt()?;

    let source_postgres_url = Text::new("Postgres URL")
        .with_default("postgres://postgres:postgres@localhost:5432/mydb")
        .with_help_message("Enter the URL of the source database to validate the CDC files")
        .prompt()?;

    let target_postgres_url = Text::new("Target Postgres URL")
        .with_default("postgres://postgres:postgres@localhost:5438/mydb")
        .with_help_message("Enter the URL of the target database to import the parquet files")
        .prompt()?;

    let database_schema = Text::new("Database Schema")
        .with_default("public")
        .with_help_message("Enter the schema of the database of the database")
        .prompt()?;

    let table_names = Text::new("Tables to include")
        .with_default("table1 table2")
        .with_help_message(
            "Enter the list of table names to validate against S3 files (comma separated)",
        )
        .prompt()?;

    let start_date = Text::new("Start date")
        .with_default("2024-01-01T12:00:00Z")
        .with_help_message("Enter the start date to filter the Parquet files")
        .prompt()?;

    let stop_date = Text::new("Stop date")
        .with_default("")
        .with_help_message("Enter the stop date to filter the Parquet files")
        .prompt()?;

    let chunk_size = Text::new("Number of rows to compare (in batches)")
        .with_default("1000")
        .with_help_message("Enter the chunk size for the data comparison")
        .prompt()?;

    let max_connections = Text::new("Maximum connection pool size")
        .with_default("100")
        .with_help_message("Enter the maximum connection connections for the Postgres pool")
        .prompt()?;

    let start_position = Text::new("Start position")
        .with_default("0")
        .with_help_message("Enter the start position for the data comparison")
        .prompt()?;

    let only_datadiff = Confirm::new("Run only the data comparison")
        .with_default(false)
        .with_help_message("Run only the pgdatadiff tool (no snapshot)")
        .prompt()?;

    let only_snapshot = Confirm::new("Take only a snapshot")
        .with_default(false)
        .with_help_message("Take only a snapshot from S3 to target DB (no data comparison)")
        .prompt()?;

    let payload = CDCOperatorPayload::new(
        bucket_name,
        s3_prefix,
        source_postgres_url,
        target_postgres_url,
        database_schema,
        table_names.split_whitespace().collect(),
        start_date,
        if stop_date.is_empty() {
            None
        } else {
            Some(stop_date)
        },
        chunk_size.parse::<i64>().unwrap(),
        max_connections.parse::<u32>().unwrap(),
        start_position.parse::<i64>().unwrap(),
        only_datadiff,
        only_snapshot,
    );

    Ok(payload)
}

#[::tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let cdc_operator_payload;

    #[cfg(feature = "with-clap")]
    {
        cdc_operator_payload = main_clap()?;
    }
    #[cfg(not(feature = "with-clap"))]
    {
        cdc_operator_payload = main_inquire()?;
    }

    // Connect to the Postgres database
    info!("{}", "Connecting to source Postgres DB".bold().green());
    let db_client = PostgresConfig::new(
        cdc_operator_payload.source_postgres_url(),
        cdc_operator_payload.database_name(),
        cdc_operator_payload.table_names().to_vec().clone(),
        cdc_operator_payload.max_connections(),
    );
    let pg_pool = db_client.connect_to_postgres().await;
    // Create a PostgresOperatorImpl instance
    let postgres_operator = PostgresOperatorImpl::new(pg_pool);

    info!("{}", "Connecting to target Postgres DB".bold().green());
    let target_db_client: PostgresConfig = PostgresConfig::new(
        cdc_operator_payload.target_postgres_url(),
        "public",
        cdc_operator_payload.table_names().to_vec().clone(),
        cdc_operator_payload.max_connections(),
    );
    let target_pg_pool = target_db_client.connect_to_postgres().await;
    // Create a PostgresOperatorImpl instance for the target database
    let target_postgres_operator = PostgresOperatorImpl::new(target_pg_pool);

    // Create an S3 client
    info!("{}", "Creating S3 client".bold().green());
    let client = create_s3_client().await;
    // Create an S3OperatorImpl instance
    let s3_operator = S3OperatorImpl::new(&client);

    let dataframe_operator = DataframeOperatorImpl::new(&client);

    let cdc_operator_snapshot_payload = CDCOperatorSnapshotPayload::new(
        cdc_operator_payload.bucket_name(),
        cdc_operator_payload.s3_prefix(),
        cdc_operator_payload.database_name(),
        cdc_operator_payload.schema_name(),
        cdc_operator_payload.table_names().to_vec(),
        cdc_operator_payload.start_date(),
        cdc_operator_payload.stop_date().map(|x| x.to_string()),
    );

    if !cdc_operator_payload.only_datadiff() {
        info!("{}", "Running snapshot...".bold().blue());
        let _ = CDCOperator::snapshot(
            cdc_operator_snapshot_payload,
            &postgres_operator,
            &target_postgres_operator,
            s3_operator,
            dataframe_operator,
        )
        .await;
    }

    if cdc_operator_payload.only_snapshot() {
        info!("{}", "Skipping validation...".bold().blue());
        return Ok(());
    }

    let cdc_operator_validate_payload = CDCOperatorValidatePayload::new(
        cdc_operator_payload.source_postgres_url(),
        cdc_operator_payload.target_postgres_url(),
        cdc_operator_payload.table_names().to_vec(),
        cdc_operator_payload.schema_name(),
        cdc_operator_payload.chunk_size(),
        cdc_operator_payload.start_position(),
    );

    let _ = CDCOperator::validate(cdc_operator_validate_payload).await;

    // Close the connection pool
    info!("{}", "Closing connection pool".bold().green());
    postgres_operator.close_connection_pool().await;
    target_postgres_operator.close_connection_pool().await;

    Ok(())
}

async fn create_s3_client() -> S3Client {
    let config = aws_config::load_from_env().await;
    S3Client::new(&config)
}

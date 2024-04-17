use crate::validate::validator::Validator;
use anyhow::{Ok, Result};
mod postgres;
mod s3;
mod validate;
#[cfg(not(feature = "with-clap"))]
use inquire::{Confirm, Text};

#[cfg(feature = "with-clap")]
use clap::{Parser, Subcommand};

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
        postgres_url: String,
        /// Url of the local database to import the parquet files
        /// Example: postgres://postgres:postgres@localhost:5432/mydb
        #[arg(long, required = true)]
        local_postgres_url: String,
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
        /// Take only a snapshot from S3 to local DB
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
async fn main_clap() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Validate {
            bucket_name,
            s3_prefix,
            postgres_url,
            local_postgres_url,
            database_schema,
            table_names,
            start_date,
            stop_date,
            chunk_size,
            start_position,
            only_datadiff,
            only_snapshot,
        } => {
            let validator = Validator::new(
                bucket_name,
                s3_prefix,
                postgres_url,
                local_postgres_url,
                database_schema,
                table_names,
                start_date,
                stop_date,
                chunk_size,
                start_position,
                only_datadiff,
                only_snapshot,
            );

            let _ = validator.validate().await;
            Ok(())
        }
    }
}

#[cfg(not(feature = "with-clap"))]
async fn main_inquire() -> Result<()> {
    let bucket_name = Text::new("S3 Bucket name")
        .with_default("bucket_name")
        .with_help_message("Enter the S3 bucket where the CDC files are stored")
        .prompt()?;

    let s3_prefix = Text::new("S3 Prefix")
        .with_default("data/landing/rds/mydb")
        .with_help_message("Enter the S3 prefix where the files are stored")
        .prompt()?;

    let postgres_url = Text::new("Postgres URL")
        .with_default("postgres://postgres:postgres@localhost:5432/mydb")
        .with_help_message("Enter the URL of the database to validate the CDC files")
        .prompt()?;

    let local_postgres_url = Text::new("Local Postgres URL")
        .with_default("postgres://postgres:postgres@localhost:5438/mydb")
        .with_help_message("Enter the URL of the local database to import the parquet files")
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
        .with_help_message("Take only a snapshot from S3 to local DB (no data comparison)")
        .prompt()?;

    let validator = Validator::new(
        bucket_name,
        s3_prefix,
        postgres_url,
        local_postgres_url,
        database_schema,
        table_names.split_whitespace().collect(),
        start_date,
        if stop_date.is_empty() {
            None
        } else {
            Some(stop_date)
        },
        chunk_size.parse::<i64>().unwrap(),
        start_position.parse::<i64>().unwrap(),
        only_datadiff,
        only_snapshot,
    );

    let _ = validator.validate().await;

    Ok(())
}

#[::tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    #[cfg(feature = "with-clap")]
    {
        _ = main_clap().await;
    }
    #[cfg(not(feature = "with-clap"))]
    {
        _ = main_inquire().await;
    }

    Ok(())
}

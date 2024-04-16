use crate::validate::validator::Validator;
use anyhow::{Ok, Result};
use clap::{Parser, Subcommand};
mod postgres;
mod s3;
mod validate;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

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

#[::tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

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

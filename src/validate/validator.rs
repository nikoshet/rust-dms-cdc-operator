use colored::Colorize;
use log::info;
use rust_pgdatadiff::diff::diff_ops::Differ;
use rust_pgdatadiff::diff::diff_payload::DiffPayload;
use std::borrow::Borrow;

use crate::postgres::postgres_config::PostgresConfig;
use crate::postgres::postgres_ops::{PostgresOperator, PostgresOperatorImpl};
use crate::s3::s3_ops::create_s3_client;
use crate::s3::s3_ops::{S3Operator, S3OperatorImpl};

const EMPTY_STRING_VEC: Vec<String> = Vec::new();

/// Represents a validator that validates the data between S3 and a local database.
pub struct Validator {
    bucket_name: String,
    s3_prefix: String,
    postgres_url: String,
    local_postgres_url: String,
    database_schema: String,
    table_name: String,
    start_date: String,
    chunk_size: i64,
    only_datadiff: bool,
    only_snapshot: bool,
}

impl Validator {
    /// Creates a new validator.
    ///
    /// # Arguments
    ///
    /// * `bucket_name` - The name of the S3 bucket
    /// * `s3_prefix` - The prefix of the S3 bucket
    /// * `postgres_url` - The URL of the Postgres database
    /// * `local_postgres_url` - The URL of the local Postgres database
    /// * `database_schema` - The schema of the database
    /// * `table_name` - The name of the table
    /// * `start_date` - The start date
    /// * `chunk_size` - The chunk size
    /// * `only_datadiff` - Whether to only validate the data difference
    /// * `only_snapshot` - Whether to only validate the snapshot
    ///
    /// # Returns
    ///
    /// A new validator instance.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        bucket_name: impl Into<String>,
        s3_prefix: impl Into<String>,
        postgres_url: impl Into<String>,
        local_postgres_url: impl Into<String>,
        database_schema: impl Into<String>,
        table_name: impl Into<String>,
        start_date: impl Into<String>,
        chunk_size: i64,
        only_datadiff: bool,
        only_snapshot: bool,
    ) -> Self {
        if only_datadiff && only_snapshot {
            panic!("Cannot run both only_datadiff and only_snapshot at the same time");
        }

        Validator {
            bucket_name: bucket_name.into(),
            s3_prefix: s3_prefix.into(),
            postgres_url: postgres_url.into(),
            local_postgres_url: local_postgres_url.into(),
            database_schema: database_schema.into(),
            table_name: table_name.into(),
            start_date: start_date.into(),
            chunk_size,
            only_datadiff,
            only_snapshot,
        }
    }

    /// Validates the data between S3 and a local database.
    pub async fn validate(&self) {
        info!("{}", "Starting validation".bold().purple());

        // Connect to the Postgres database
        info!("{}", "Connecting to Postgres DB".bold().green());
        let db_client = PostgresConfig::new(
            self.postgres_url.clone(),
            self.database_schema.clone(),
            self.table_name.clone(),
        );
        let pg_pool = db_client.connect_to_postgres().await;
        // Create a PostgresOperatorImpl instance
        let postgres_operator = PostgresOperatorImpl::new(pg_pool);

        info!("{}", "Connecting to local Postgres DB".bold().green());
        let local_db_client = PostgresConfig::new(
            self.local_postgres_url.clone(),
            "public",
            self.table_name.clone(),
        );
        let local_pg_pool = local_db_client.connect_to_postgres().await;
        // Create a PostgresOperatorImpl instance for the local database
        let local_postgres_operator = PostgresOperatorImpl::new(local_pg_pool);

        // Create an S3 client
        info!("{}", "Creating S3 client".bold().green());
        let client = create_s3_client().await;
        // Create an S3OperatorImpl instance
        let s3_operator = S3OperatorImpl::new(client);

        // Check if only_datadiff is true
        if !self.only_datadiff {
            // Get the table columns
            info!("{}", "Getting table columns".bold().green());
            let table_columns = postgres_operator
                .get_table_columns(db_client.schema_name(), db_client.table_name())
                .await
                .unwrap();
            info!(
                "Number of columns: {}, Columns: {:?}",
                table_columns.len(),
                table_columns
            );

            // Get the primary key for the table
            info!("{}", "Getting primary key".bold().green());
            let primary_key = postgres_operator
                .get_primary_key(db_client.table_name())
                .await
                .unwrap();
            info!("Primary key: {:?}", primary_key);

            // Create the table in the local database
            info!("{}", "Creating table in the local DB".bold().green());
            let _ = local_postgres_operator
                .create_table(
                    &table_columns,
                    &primary_key,
                    db_client.schema_name(),
                    db_client.table_name(),
                )
                .await;

            // Get the list of Parquet files from S3
            info!("{}", "Getting list of Parquet files from S3".bold().green());
            let parquet_files = s3_operator
                .get_list_of_parquet_files_from_s3(
                    self.bucket_name.clone(),
                    self.s3_prefix.clone(),
                    db_client.database_name(),
                    self.database_schema.clone(),
                    self.table_name.clone(),
                    self.start_date.clone(),
                )
                .await;

            // Read the Parquet files from S3
            info!("{}", "Reading Parquet files from S3".bold().green());

            for file in &parquet_files.unwrap() {
                let bucket_name = self.bucket_name.clone();
                let table_name = self.table_name.clone();
                let primary_key = primary_key.clone();
                let local_postgres_operator = local_postgres_operator.borrow();
                let s3_operator = s3_operator.borrow();

                let current_df = s3_operator
                    .read_parquet_file_from_s3(&bucket_name, file)
                    .await
                    .map_err(|e| {
                        panic!("Error reading Parquet file: {:?}", e);
                    })
                    .unwrap();

                //task::block_in_place(move || {
                //    Handle::current().block_on(async move {
                if file
                    .split('/')
                    .collect::<Vec<&str>>()
                    .last()
                    .unwrap()
                    .contains("LOAD")
                {
                    info!("Processing LOAD file: {:?}", file);
                    local_postgres_operator
                        .insert_dataframe_in_local_db(current_df, &table_name)
                        .await
                        .unwrap_or_else(|_| {
                            panic!("Failed to insert LOAD file {:?} into table", file)
                        })
                } else {
                    info!("Processing CDC file: {:?}", file);
                    local_postgres_operator
                        .upsert_dataframe_in_local_db(current_df, &table_name, &primary_key)
                        .await
                        .unwrap_or_else(|_| {
                            panic!("Failed to upsert CDC file {:?} into table", file)
                        })
                }
            }

            // Drop the columns added by DMS
            info!("{}", "Dropping columns added by DMS".bold().green());
            let _ = local_postgres_operator
                .drop_dms_columns(db_client.schema_name(), db_client.table_name())
                .await;
        }

        if !self.only_snapshot {
            // Run rust-pgdatadiff
            info!(
                "{} {}",
                "Running pgdatadiff with chunk size".bold().green(),
                self.chunk_size.to_string().bold().green()
            );
            let payload = DiffPayload::new(
                db_client.connection_string(),
                local_db_client.connection_string(),
                true,            //only-tables
                false,           //only-sequences
                false,           //only-count
                self.chunk_size, //chunk-size
                100,             //max-connections
                vec![db_client.table_name()],
                EMPTY_STRING_VEC,
                db_client.schema_name(),
            );
            let diff_result = Differ::diff_dbs(payload).await;
            if diff_result.is_err() {
                panic!("Failed to run pgdatadiff: {:?}", diff_result.err().unwrap());
            }
        }

        // Close the connection pool
        info!("{}", "Closing connection pool".bold().green());
        postgres_operator.close_connection_pool().await;
        local_postgres_operator.close_connection_pool().await;

        info!("{}", "Validation complete".bold().purple());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[should_panic = "Cannot run both only_datadiff and only_snapshot at the same time"]
    async fn test_validate() {
        let bucket_name = "test-bucket";
        let s3_prefix = "test-prefix";
        let postgres_url = "postgres://postgres:postgres@localhost:5432/mydb";
        let local_postgres_url = "postgres://postgres:postgres@localhost:5432/mydb";
        let database_schema = "public";
        let table_name = "test_table";
        let start_date = "2021-01-01";
        let chunk_size = 1000;
        let only_datadiff = true;
        let only_snapshot = true;

        let _validator = Validator::new(
            bucket_name,
            s3_prefix,
            postgres_url,
            database_schema,
            table_name,
            local_postgres_url,
            start_date,
            chunk_size,
            only_datadiff,
            only_snapshot,
        );
    }
}

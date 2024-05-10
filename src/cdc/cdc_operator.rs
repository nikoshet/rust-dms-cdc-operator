use colored::Colorize;
use log::info;
use rust_pgdatadiff::diff::diff_ops::Differ;
use rust_pgdatadiff::diff::diff_payload::DiffPayload;
use std::time::Instant;

use super::snapshot_payload::CDCOperatorSnapshotPayload;
use super::validate_payload::CDCOperatorValidatePayload;

use crate::dataframe::dataframe_ops::{CreateDataframePayload, DataframeOperator};
use crate::postgres::postgres_operator::{
    InsertDataframePayload, PostgresOperator, UpsertDataframePayload,
};
use crate::s3::s3_operator::{LoadParquetFilesPayload, S3Operator};

/// Represents a CDC Operator that validates the data between S3 and a target database.
pub struct CDCOperator;

impl CDCOperator {
    /// Takes a snpashot of the data stored in S3 and replicates them in a target database.
    pub async fn snapshot(
        cdc_operator_snapshot_payload: CDCOperatorSnapshotPayload,
        source_postgres_operator: &impl PostgresOperator,
        target_postgres_operator: &impl PostgresOperator,
        s3_operator: impl S3Operator,
        dataframe_operator: impl DataframeOperator,
    ) {
        info!("{}", "Creating schema in the target DB".bold().green());
        let _ = target_postgres_operator
            .create_schema(cdc_operator_snapshot_payload.schema_name().as_str())
            .await;

        // Check if only_datadiff is true
        info!("{}", "Starting snapshotting...".bold().blue());

        // Find the tables for snapshotting
        let table_list = source_postgres_operator
            .get_tables_in_schema(
                cdc_operator_snapshot_payload.schema_name().as_str(),
                cdc_operator_snapshot_payload.included_tables().as_slice(),
                cdc_operator_snapshot_payload.excluded_tables().as_slice(),
                &cdc_operator_snapshot_payload.table_mode(),
            )
            .await
            .unwrap();

        for table_name in &table_list {
            let start = Instant::now();
            info!(
                "{}",
                format!("Running for table: {}", table_name)
                    .bold()
                    .magenta()
            );

            // Get the table columns
            info!("{}", "Getting table columns".bold().green());
            let source_table_columns: indexmap::IndexMap<String, String> = source_postgres_operator
                .get_table_columns(
                    cdc_operator_snapshot_payload.schema_name().as_str(),
                    table_name,
                )
                .await
                .unwrap();
            info!(
                "Number of columns: {}, Columns: {:?}",
                source_table_columns.len(),
                source_table_columns
            );

            // Get the primary key for the table
            info!("{}", "Getting primary key".bold().green());
            let primary_key_list = source_postgres_operator
                .get_primary_key(
                    table_name,
                    cdc_operator_snapshot_payload.schema_name().as_str(),
                )
                .await
                .unwrap();
            info!("Primary key(s): {:?}", primary_key_list);

            // Create the table in the target database
            info!("{}", "Creating table in the target DB".bold().green());
            let _ = target_postgres_operator
                .create_table(
                    &source_table_columns,
                    primary_key_list.as_slice(),
                    cdc_operator_snapshot_payload.schema_name().as_str(),
                    table_name,
                )
                .await;

            // Get the list of Parquet files from S3
            info!("{}", "Getting list of Parquet files from S3".bold().green());

            let load_parquet_files_payload = if let Some(start_date) =
                cdc_operator_snapshot_payload.start_date()
            {
                LoadParquetFilesPayload::DateAware {
                    bucket_name: cdc_operator_snapshot_payload.bucket_name(),
                    s3_prefix: cdc_operator_snapshot_payload.key(),
                    database_name: cdc_operator_snapshot_payload.database_name(),
                    schema_name: cdc_operator_snapshot_payload.schema_name(),
                    table_name: table_name.to_string(),
                    start_date,
                    stop_date: cdc_operator_snapshot_payload
                        .stop_date()
                        .map(|s| s.to_string()),
                }
            } else {
                LoadParquetFilesPayload::AbsolutePath(cdc_operator_snapshot_payload.key().clone())
            };

            let parquet_files = s3_operator
                .get_list_of_parquet_files_from_s3(&load_parquet_files_payload)
                .await;

            // Read the Parquet files from S3
            info!("{}", "Reading Parquet files from S3".bold().green());

            for file in &parquet_files.unwrap() {
                let create_dataframe_payload = CreateDataframePayload {
                    bucket_name: cdc_operator_snapshot_payload.bucket_name(),
                    key: file.file_name.to_string(),
                    database_name: cdc_operator_snapshot_payload.database_name(),
                    schema_name: cdc_operator_snapshot_payload.schema_name(),
                    table_name: table_name.clone(),
                };

                let current_df = dataframe_operator
                    .create_dataframe_from_parquet_file(&create_dataframe_payload)
                    .await
                    .map_err(|e| {
                        panic!("Error reading Parquet file: {:?}", e);
                    })
                    .unwrap()
                    .unwrap();

                if file.is_load_file() {
                    info!("Processing LOAD file: {:?}", file);
                    // Check if the schema of the table is the same as the schema of the Parquet file
                    // in case of altered column names or dropped columns
                    let df_column_fields = current_df.get_columns();
                    let has_schema_diff = df_column_fields
                        .iter()
                        .filter(|field| {
                            field.name() != "Op" && field.name() != "_dms_ingestion_timestamp"
                        })
                        .any(|field| !source_table_columns.contains_key(field.name()));

                    if has_schema_diff {
                        panic!("Schema of table is not the same as the schema of the Parquet file");
                    }

                    let insert_dataframe_payload = InsertDataframePayload {
                        database_name: cdc_operator_snapshot_payload.database_name(),
                        schema_name: cdc_operator_snapshot_payload.schema_name(),
                        table_name: table_name.clone(),
                    };

                    target_postgres_operator
                        .insert_dataframe_in_target_db(&current_df, &insert_dataframe_payload)
                        .await
                        .unwrap_or_else(|_| {
                            panic!("Failed to insert LOAD file {:?} into table", file)
                        })
                } else {
                    info!("Processing CDC file: {:?}", file);
                    let primary_keys = primary_key_list.clone().as_slice().join(",");

                    let upsert_dataframe_payload = UpsertDataframePayload {
                        database_name: cdc_operator_snapshot_payload.database_name(),
                        schema_name: cdc_operator_snapshot_payload.schema_name(),
                        table_name: table_name.clone(),
                        primary_key: primary_keys.clone(),
                    };

                    target_postgres_operator
                        .upsert_dataframe_in_target_db(&current_df, &upsert_dataframe_payload)
                        .await
                        .unwrap_or_else(|_| {
                            panic!("Failed to upsert CDC file {:?} into table", file)
                        })
                }
            }

            let elapsed = start.elapsed();
            info!(
                "{}",
                format!(
                    "Snapshot completed for table {} in: {}ms",
                    table_name,
                    elapsed.as_millis()
                )
                .yellow()
                .bold(),
            );
        }

        info!("{}", "Snapshotting completed...".bold().blue());
    }

    /// Validates the data between S3 and a target database.
    pub async fn validate(cdc_operator_validate_payload: CDCOperatorValidatePayload) {
        info!("{}", "Starting pgdatadiff...".bold().blue());

        // Run rust-pgdatadiff
        info!(
            "{}",
            format!(
                "Running pgdatadiff with chunk size {}",
                cdc_operator_validate_payload.chunk_size()
            )
            .bold()
            .green()
        );
        let payload = DiffPayload::new(
            cdc_operator_validate_payload.source_postgres_url(),
            cdc_operator_validate_payload.target_postgres_url(),
            true,                                           //only-tables
            false,                                          //only-sequences
            false,                                          //only-count
            cdc_operator_validate_payload.chunk_size(),     //chunk-size
            cdc_operator_validate_payload.start_position(), //start-position
            100,                                            //max-connections
            cdc_operator_validate_payload.included_tables().to_vec(),
            cdc_operator_validate_payload.excluded_tables().to_vec(),
            cdc_operator_validate_payload.schema_name(),
        );
        let diff_result = Differ::diff_dbs(payload).await;
        if diff_result.is_err() {
            panic!("Failed to run pgdatadiff: {:?}", diff_result.err().unwrap());
        }

        info!("{}", "Pgdatadiff completed!".bold().blue());
    }
}

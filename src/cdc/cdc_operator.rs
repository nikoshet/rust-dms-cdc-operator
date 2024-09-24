use aws_sdk_s3::Client as S3Client;
use colored::Colorize;
use log::info;
use rust_pgdatadiff::diff::diff_ops::Differ;
use rust_pgdatadiff::diff::diff_payload::DiffPayload;
use std::env;
use std::sync::Arc;
use std::time::Instant;

use super::snapshot_payload::CDCOperatorSnapshotPayload;
use super::validate_payload::CDCOperatorValidatePayload;

use crate::dataframe::dataframe_ops::{
    CreateDataframePayload, DataframeOperator, DataframeOperatorImpl,
};
use crate::postgres::postgres_operator::{
    InsertDataframePayload, PostgresOperator, UpsertDataframePayload,
};
use crate::s3::s3_operator::{LoadParquetFilesPayload, S3Operator, S3OperatorImpl};

/// Represents a CDC Operator that validates the data between S3 and a target database.
pub struct CDCOperator;

impl CDCOperator {
    /// Takes a snpashot of the data stored in S3 and replicates them in a target database.
    pub async fn snapshot(
        cdc_operator_snapshot_payload: &CDCOperatorSnapshotPayload,
        source_postgres_operator: &(impl PostgresOperator + Sync),
        target_postgres_operator: &(impl PostgresOperator + Sync),
        s3_client: &S3Client,
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

        let cdc_operator_snapshot_payload: Arc<&CDCOperatorSnapshotPayload> =
            Arc::new(cdc_operator_snapshot_payload);
        let client = s3_client.clone();
        let s3_operator = Arc::new(S3OperatorImpl::new(&client));
        let dataframe_operator = Arc::new(DataframeOperatorImpl::new(s3_client));

        let tables = table_list
            .iter()
            .map(|table_name| {
                let payload = Arc::clone(&cdc_operator_snapshot_payload);
                let s3_operator = Arc::clone(&s3_operator);
                let dataframe_operator = Arc::clone(&dataframe_operator);

                async move {
                    let payload = Arc::clone(&payload);

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
                            payload.schema_name.as_str(),
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
                            payload.schema_name.as_str(),
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
                            payload.schema_name.clone().as_str(),
                            table_name,
                        )
                        .await;

                    // Get the list of Parquet files from S3
                    info!("{}", "Getting list of Parquet files from S3".bold().green());

                    // Check if mode is DateAware and start_date is not None
                    if payload.mode_is_date_aware() && payload.start_date.is_none() {
                        panic!("start_date is required for DateAware mode");
                    }

                    let load_parquet_files_payload
                    = if payload.mode_is_date_aware(){
                            LoadParquetFilesPayload::DateAware {
                                bucket_name: payload.bucket_name.clone(),
                                s3_prefix: payload.key.clone(),
                                database_name: payload.database_name.clone(),
                                schema_name: payload.schema_name.clone(),
                                table_name: table_name.to_string(),
                                start_date: payload.start_date.clone().unwrap(),
                                stop_date: payload
                                    .stop_date.clone(),
                            }
                        }
                    else if payload.mode_is_full_load_only() {
                        LoadParquetFilesPayload::FullLoadOnly {
                            bucket_name: payload.bucket_name.clone(),
                            s3_prefix: payload.key.clone(),
                                database_name: payload.database_name.clone(),
                                schema_name: payload.schema_name.clone(),
                                table_name: table_name.to_string(),
                        }
                    } else {
                        LoadParquetFilesPayload::AbsolutePath(payload.key.clone())
                    };

                    // Read the Parquet files from S3
                    info!("{}", "Reading Parquet files from S3".bold().green());

                    let parquet_files = s3_operator
                        .get_list_of_parquet_files_from_s3(&load_parquet_files_payload)
                        .await
                        .unwrap_or_else(|_| {
                            info!("No available Parquet files from S3 for table {} to process", table_name);
                            Vec::new()
                        });

                    for file in &parquet_files {
                        let create_dataframe_payload = CreateDataframePayload {
                            bucket_name: payload.bucket_name.clone(),
                            key: file.file_name.to_string(),
                            database_name: payload.database_name.clone(),
                            schema_name: payload.schema_name.clone(),
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
                                database_name: payload.database_name.clone(),
                                schema_name: payload.schema_name.clone(),
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
                                database_name: payload.database_name.clone(),
                                schema_name: payload.schema_name.clone(),
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
            })
            .collect::<Vec<_>>();

        use futures::stream::{self};
        use futures::FutureExt;
        use futures::StreamExt;

        let num_of_buffers = env::var("NUM_OF_BUFFERS")
            .unwrap_or_else(|_| "80".to_string())
            .parse::<usize>()
            .unwrap();

        // Convert the Vec into a stream
        let stream = stream::iter(tables)
            .map(|future| future.boxed())
            .buffer_unordered(num_of_buffers);

        // Collect results, ensuring at most 80 futures run concurrently
        stream.for_each(|_| async {}).await;

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
            cdc_operator_validate_payload.accept_invalid_certs_first_db(),
            cdc_operator_validate_payload.accept_invalid_certs_second_db(),
        );
        let diff_result = Differ::diff_dbs(payload).await;
        if diff_result.is_err() {
            panic!("Failed to run pgdatadiff: {:?}", diff_result.err().unwrap());
        }

        info!("{}", "Pgdatadiff completed!".bold().blue());
    }
}

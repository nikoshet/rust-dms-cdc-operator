use anyhow::Result;
use async_trait::async_trait;
use indexmap::IndexMap;
use log::debug;
use polars::prelude::*;

use polars_core::export::num::ToPrimitive;

use sqlx::{Pool, Postgres, Row};
use std::{fmt::Display, time::Instant};

use tracing::{info, instrument};
use TableQuery::*;

pub(crate) use super::postgres_operator::PostgresOperator;
use super::{
    postgres_operator::{InsertDataframePayload, UpsertDataframePayload},
    table_query::TableQuery,
};

use crate::postgres::postgres_row_struct::RowStruct;
use crate::postgres::table_mode::TableMode;

/// Represents the data type of a column in a table.
enum ColumnDataType {
    Array,
    Rest(String),
}

impl Display for ColumnDataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ColumnDataType::Array => write!(f, "text[]"),
            ColumnDataType::Rest(data_type) => write!(f, "{}", data_type),
        }
    }
}

pub struct PostgresOperatorImpl {
    db_client: Pool<Postgres>,
}

impl PostgresOperatorImpl {
    pub fn new(db_client: Pool<Postgres>) -> Self {
        Self { db_client }
    }
}

#[async_trait]
impl PostgresOperator for PostgresOperatorImpl {
    async fn get_table_columns(
        &self,
        schema_name: &str,
        table_name: &str,
    ) -> Result<IndexMap<String, String>> {
        let pg_pool = self.db_client.clone();

        // Prepare the query to get all columns for a table
        let query = FindAllColumns(schema_name.to_string(), table_name.to_string());

        // Fetch columns for the table
        let rows = sqlx::query(&query.to_string()).fetch_all(&pg_pool).await?;
        let mut res = IndexMap::new();
        for row in rows {
            let column_name: String = row.get("column_name");
            let data_type: String = row.get("data_type");
            if data_type.eq("ARRAY") {
                res.insert(column_name, ColumnDataType::Array.to_string());
            } else {
                res.insert(column_name, ColumnDataType::Rest(data_type).to_string());
            }
        }

        Ok(res)
    }

    async fn get_primary_key(&self, table_name: &str, schema_name: &str) -> Result<Vec<String>> {
        let pg_pool = self.db_client.clone();

        // Prepare the query to get the primary key for a table
        let query = FindPrimaryKey(table_name.to_string(), schema_name.to_string());
        // Fetch the primary key for the table
        let row = sqlx::query(&query.to_string())
            .fetch_all(&pg_pool)
            .await
            .unwrap_or(vec![]);

        // Map query results to [Vec<String>]
        let primary_key_list = row
            .iter()
            .map(|row| row.get("attname"))
            .collect::<Vec<String>>();

        Ok(primary_key_list)
    }

    async fn create_schema(&self, schema_name: &str) -> Result<()> {
        let pg_pool = self.db_client.clone();

        // Prepare the query to create a schema
        let query = CreateSchema(schema_name.to_string());
        sqlx::query(&query.to_string())
            .execute(&pg_pool)
            .await
            .expect("Failed to create schema");

        Ok(())
    }

    async fn get_tables_in_schema(
        &self,
        schema_name: &str,
        included_tables: &[String],
        excluded_tables: &[String],
        table_mode: &TableMode,
    ) -> Result<Vec<String>> {
        let pg_pool = self.db_client.clone();

        let subquery = match table_mode {
            TableMode::IncludeTables => {
                format!(
                    "AND table_name IN ({})",
                    included_tables
                        .iter()
                        .map(|table| format!("'{}'", table))
                        .collect::<Vec<String>>()
                        .join(", ")
                )
            }
            TableMode::ExcludeTables => {
                format!(
                    "AND table_name NOT IN ({})",
                    excluded_tables
                        .iter()
                        .map(|table| format!("'{}'", table))
                        .collect::<Vec<String>>()
                        .join(", ")
                )
            }
            TableMode::AllTables => "".to_string(),
        };

        let query = FindTablesForSchema(schema_name.to_string(), subquery);
        let rows = sqlx::query(&query.to_string())
            .fetch_all(&pg_pool)
            .await
            .expect("Failed to fetch tables");

        let tables = rows
            .iter()
            .map(|row| row.get("table_name"))
            .collect::<Vec<String>>();
        Ok(tables)
    }

    async fn create_table(
        &self,
        column_data_types: &IndexMap<String, String>,
        primary_keys: &[String],
        schema_name: &str,
        table_name: &str,
    ) -> Result<()> {
        let pg_pool = self.db_client.clone();

        // Prepare the query to create a table
        let query = CreateTable(
            schema_name.to_string(),
            table_name.to_string(),
            column_data_types.clone(),
            primary_keys.join(","),
        );
        sqlx::query(&query.to_string())
            .execute(&pg_pool)
            .await
            .expect("Failed to create table");

        Ok(())
    }

    async fn drop_schema(&self, schema_name: &str) -> Result<()> {
        let pg_pool = self.db_client.clone();

        // Prepare the query to drop a schema
        let query = DropSchema(schema_name.to_string());
        sqlx::query(&query.to_string())
            .execute(&pg_pool)
            .await
            .expect("Failed to drop schema");

        Ok(())
    }

    #[instrument(name = "Insert data into table", skip(self, df))]
    async fn insert_dataframe_in_target_db(
        &self,
        df: &DataFrame,
        payload: &InsertDataframePayload,
    ) -> Result<()> {
        let pg_pool = self.db_client.clone();

        let mut df = df.clone();

        // Drop the columns added by DMS
        _ = df.drop_in_place("Op").expect("Failed to drop 'Op' column");
        _ = df
            .drop_in_place("_dms_ingestion_timestamp")
            .expect("Failed to drop '_dms_ingestion_timestamp' column");

        let column_names = df.get_column_names();
        let fields = column_names.join(", ");

        let df_height = df.height().to_i64().unwrap();

        info!("Total DF height: {df_height}");

        if df_height >= 200_000 {
            let rows_per_df = 10_000;
            let mut offset = 0i64;

            let insert_by_chunk_start = Instant::now();
            while offset <= df_height {
                debug!("Inserting rows at offset: {offset}");
                let df_chunk = df.slice(offset, rows_per_df);
                let df_height = df_chunk.height();
                let df_columns = df_chunk.get_columns();

                let values = (0..df_height)
                    .map(|row_idx| {
                        let values = df_columns
                            .iter()
                            .map(|column| {
                                let v = column.get(row_idx).unwrap();
                                RowStruct::new(&v).displayed()
                            })
                            .collect::<Vec<_>>()
                            .join(", ");

                        format!("({})", values)
                    })
                    .collect::<Vec<String>>()
                    .join(", ");

                let query = format!(
                    "INSERT INTO {schema_name}.{table_name} ({fields}) VALUES {values};",
                    schema_name = payload.schema_name,
                    table_name = payload.table_name,
                );

                sqlx::query(&query)
                    .execute(&pg_pool)
                    .await
                    .inspect_err(|e| {
                        panic!(
                            "Failed to insert data into table -> {}: {e}",
                            payload.table_name
                        );
                    })
                    .expect("Failed to insert data into table");

                offset += rows_per_df.to_i64().unwrap();
            }

            let insert_by_chunk_duration = insert_by_chunk_start.elapsed().as_millis();
            debug!("Inserting DF by chunk took: {insert_by_chunk_duration}ms");
        } else {
            let df_columns = df.get_columns();

            let insert_by_row_start = Instant::now();
            for row in 0..df_height {
                debug!("Inserting rows in chunks");
                debug!("Dataframe height: {df_height}");

                // Construct the query with placeholders
                let values_concatenation_start = Instant::now();
                let values = df_columns
                    .iter()
                    .map(|column| {
                        let v = column.get(row.try_into().unwrap()).unwrap();
                        RowStruct::new(&v).displayed()
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                let values_concatenation_duration =
                    values_concatenation_start.elapsed().as_millis();
                debug!("Concatenating values took: {values_concatenation_duration}ms");

                let query = format!(
                    "INSERT INTO {schema_name}.{table_name} ({fields}) VALUES ({values});",
                    schema_name = payload.schema_name,
                    table_name = payload.table_name,
                );

                let query_insert_by_row_start = Instant::now();
                sqlx::query(&query)
                    .execute(&pg_pool)
                    .await
                    .inspect_err(|e| {
                        panic!(
                            "Failed to insert data into table -> {}: {e}",
                            payload.table_name
                        );
                    })
                    .expect("Failed to insert data into table");
                let query_insert_by_row_duration = query_insert_by_row_start.elapsed().as_millis();
                debug!("Inserting row took: {query_insert_by_row_duration}ms")
            }
            let insert_by_row_duration = insert_by_row_start.elapsed().as_millis();
            debug!("Inserting DF by row took: {insert_by_row_duration}ms");
        }

        Ok(())
    }

    #[instrument(name = "Upsert data into table", skip(self, df))]
    async fn upsert_dataframe_in_target_db(
        &self,
        df: &DataFrame,
        payload: &UpsertDataframePayload,
    ) -> Result<()> {
        let pg_pool = self.db_client.clone();

        let mut row_values = Vec::new();
        let mut deleted_row: bool;

        let column_names = df
            .get_column_names()
            .into_iter()
            .filter(|column| {
                let is_not_op = *column != "Op";
                let is_not_dms_ingestion_timestamp = *column != "_dms_ingestion_timestamp";
                is_not_op && is_not_dms_ingestion_timestamp
            })
            .collect::<Vec<_>>();
        let fields = column_names.join(", ");

        for row in 0..df.height() {
            row_values.clear();
            deleted_row = false;

            let pk_vector = payload
                .primary_key
                .split(',')
                .map(|key| df.column(key).unwrap().get(row).unwrap().to_string())
                .collect::<Vec<String>>();

            for column in df.get_columns() {
                // Operation: Delete
                // Delete the rows where Op="D"
                let column_name = column.name();
                let is_op = column_name == "Op";

                let value = column.get(row).unwrap();
                let is_delete = value.to_string().contains('D');
                let is_op_and_delete = is_op && is_delete;

                if !is_op_and_delete {
                    row_values.push(value);
                    continue;
                }

                let query = DeleteRows(
                    payload.schema_name.clone(),
                    payload.table_name.clone(),
                    payload.primary_key.clone(),
                    pk_vector.as_slice().join(","),
                );

                debug!("Query: {}", query);
                sqlx::query(&query.to_string().replace('"', "'"))
                    .execute(&pg_pool)
                    .await
                    .unwrap_or_else(|_| {
                        panic!(
                            "Failed to delete rows from table: {schema_name}.{table_name}",
                            schema_name = payload.schema_name.clone(),
                            table_name = payload.table_name.clone()
                        )
                    });

                deleted_row = true;
                break;
            }

            if deleted_row {
                debug!("Deleted row");
                continue;
            }

            // Operation: Update
            let is_update_op = row_values.first().unwrap().to_string().contains('U');

            debug!("Row values: {:?}", row_values);

            // Remove the Op and _dms_ingestion_timestamp column from the row values
            let row_values = row_values.iter().skip(2).collect::<Vec<_>>();
            let values_of_row = row_values
                .iter()
                .map(|v| RowStruct::new(v).displayed())
                .collect::<Vec<_>>()
                .join(", ");

            let on_conflict_strategy = if !is_update_op {
                String::from("")
            } else {
                let column_names = column_names
                    .clone()
                    .into_iter()
                    .enumerate()
                    .map(|(index, column)| {
                        format!(
                            "{} = {}",
                            column,
                            RowStruct::new(row_values.get(index).unwrap()).displayed()
                        )
                    })
                    .collect::<Vec<_>>();

                // Construct the query, on Conflict, update the row
                let strategy = format!(" ON CONFLICT ({}) DO UPDATE SET ", payload.primary_key);
                let concatenated_values = column_names.join(", ");

                format!("{strategy} {concatenated_values}")
            };

            let query = format!(
                "INSERT INTO {schema_name}.{table_name} ({fields}) VALUES ({values_of_row})",
                schema_name = payload.schema_name,
                table_name = payload.table_name,
            );
            let query = format!("{query}{on_conflict_strategy};");

            debug!("Query: {}", query);
            sqlx::query(&query)
                .execute(&pg_pool)
                .await
                .unwrap_or_else(|_| {
                    panic!(
                        "Failed to upsert data in table: {schema_name}.{table_name}",
                        schema_name = payload.schema_name.clone(),
                        table_name = payload.table_name.clone()
                    )
                });
        }

        Ok(())
    }

    async fn close_connection_pool(&self) {
        self.db_client.close().await;
    }
}

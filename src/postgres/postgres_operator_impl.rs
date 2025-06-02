use anyhow::Result;
use async_trait::async_trait;
use bon::bon;
use deadpool_postgres::{GenericClient, Pool};
use indexmap::IndexMap;
use log::{debug, error, trace};
use polars::prelude::*;
use rust_decimal::prelude::ToPrimitive;
use std::sync::LazyLock;

use std::{fmt::Display, time::Instant};

use TableQuery::*;
use tracing::info;

use super::postgres_geometry_type::PostgresGeometryType;
pub(crate) use super::postgres_operator::PostgresOperator;
use super::{
    postgres_operator::{InsertDataframePayload, UpsertDataframePayload},
    table_query::TableQuery,
};

use crate::postgres::postgres_row_struct::RowStruct;
use crate::postgres::table_mode::TableMode;

static INSERT_DELAYABLES: LazyLock<Vec<String>> = LazyLock::new(|| {
    let insert_delayables: Vec<String> = std::env::var("DELAYABLE_CONFIG")
        .unwrap_or("".to_string())
        .split(",")
        .map(String::from)
        .collect();
    info!("Delayable config: {}", insert_delayables.join(", "));
    insert_delayables
});

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
    pool: Pool,
}

#[bon]
impl PostgresOperatorImpl {
    #[builder]
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl PostgresOperator for PostgresOperatorImpl {
    async fn get_table_columns(
        &self,
        schema_name: &str,
        table_name: &str,
    ) -> Result<IndexMap<String, String>> {
        // Prepare the query to get all columns for a table
        let query = FindAllColumns(schema_name.to_string(), table_name.to_string());

        // Fetch columns for the table
        let client = self.pool.get().await?;

        let rows = client.query(&query.to_string(), &[]).await?;
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
        // Prepare the query to get the primary key for a table
        let query = FindPrimaryKey(table_name.to_string(), schema_name.to_string());
        // Fetch the primary key for the table
        let client = self.pool.get().await?;

        let row = client
            .query(&query.to_string(), &[])
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
        // Prepare the query to create a schema
        let query = CreateSchema(schema_name.to_string());

        let client = self.pool.get().await?;
        client
            .execute(&query.to_string(), &[])
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

        let client = self.pool.get().await?;
        let rows = client
            .query(&query.to_string(), &[])
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
        // Prepare the query to create a table
        let query = CreateTable(
            schema_name.to_string(),
            table_name.to_string(),
            column_data_types.clone(),
            primary_keys.join(","),
        );

        let client = self.pool.get().await?;
        client
            .execute(&query.to_string(), &[])
            .await
            .expect("Failed to create table");

        Ok(())
    }

    async fn drop_schema(&self, schema_name: &str) -> Result<()> {
        // Prepare the query to drop a schema
        let query = DropSchema(schema_name.to_string());

        let client = self.pool.get().await?;
        client
            .execute(&query.to_string(), &[])
            .await
            .expect("Failed to drop schema");

        Ok(())
    }

    async fn insert_dataframe_in_target_db(
        &self,
        df: &DataFrame,
        payload: &InsertDataframePayload,
    ) -> Result<()> {
        let mut df = df.clone();

        // Drop the columns added by DMS
        _ = df.drop_in_place("Op").expect("Failed to drop 'Op' column");
        _ = df
            .drop_in_place("_dms_ingestion_timestamp")
            .expect("Failed to drop '_dms_ingestion_timestamp' column");

        let column_names = df.get_column_names_str();
        let fields = column_names.join(", ");

        debug!("Columns names: {fields}");

        let df_height = df.height().to_f64().unwrap();

        info!("Total DF height: {df_height}");

        let insert_by_chunk_start = Instant::now();
        let client = self.pool.get().await?;

        let rows_per_df = rows_per_df(payload);
        let should_delay_insert = should_delay_insert(payload);

        if should_delay_insert {
            info!(
                "Using delayable config for payload: {payload}",
                payload = payload.as_key()
            );
        }

        let insert_delay = insert_delay();

        let mut offset = 0f64;

        while offset < df_height {
            debug!(
                "Inserting rows at offset: {offset}, table: {table}",
                table = payload.table_name
            );
            let df_chunk = df.slice(offset as i64, rows_per_df);
            let df_chunk_height = df_chunk.height();
            let df_columns = df_chunk.get_columns();

            let values = (0..df_chunk_height)
                .map(|row_idx| {
                    let values = df_columns
                        .iter()
                        .map(|column| {
                            let v = column.get(row_idx).unwrap();
                            preprocess_value(&v)
                        })
                        .collect::<Vec<_>>()
                        .join(", ");

                    format!("({})", values)
                })
                .collect::<Vec<String>>()
                .join(", ");

            let query = format!(
                "INSERT INTO {schema_name}.{table_name} ({fields}) VALUES {values}",
                schema_name = payload.schema_name,
                table_name = payload.table_name,
            );

            let insert_result = client.execute(query.as_str(), &[]).await;

            match insert_result {
                Ok(_) => (),
                Err(e) => {
                    error!("DF height at point: {df_height}");
                    error!("DF chunk height at point: {df_chunk_height}");
                    error!("Offset at point: {offset}");
                    error!("Failed to insert: {e}");
                    error!(
                        "Failed to insert data into table -> {}: {e}",
                        payload.table_name
                    );
                    panic!(
                        "Failed query: {}",
                        query.chars().take(1000).collect::<String>()
                    );
                }
            }

            offset += rows_per_df.to_f64().unwrap();

            if should_delay_insert {
                tokio::time::sleep(insert_delay).await;
            }
        }

        let insert_by_chunk_duration = insert_by_chunk_start.elapsed().as_millis();
        info!("Inserting DF by chunk took: {insert_by_chunk_duration}ms");

        Ok(())
    }

    async fn upsert_dataframe_in_target_db(
        &self,
        df: &DataFrame,
        payload: &UpsertDataframePayload,
    ) -> Result<()> {
        let mut row_values = Vec::new();
        let mut deleted_row: bool;

        let mut column_names = df.get_column_names_str();
        column_names.retain(|column| *column != "Op" && *column != "_dms_ingestion_timestamp");

        let fields = column_names.join(", ");
        let client = self.pool.get().await?;

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
                trace!("Query: {}", query);

                let query = query.to_string().replace('"', "'");

                client.query(&query, &[]).await.unwrap_or_else(|_| {
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
                "INSERT INTO {schema_name}.{table_name} ({fields}) VALUES ({values_of_row});",
                schema_name = payload.schema_name,
                table_name = payload.table_name,
            );
            let query = format!("{query}{on_conflict_strategy}");

            trace!("Query: {}", query);

            let client = self.pool.get().await?;

            client
                .execute(query.as_str(), &[])
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

    async fn run_sql_command(&self, sql_command: &str) -> Result<()> {
        let client = self.pool.get().await?;

        client
            .execute(sql_command, &[])
            .await
            .unwrap_or_else(|_| panic!("Failed to execute SQL command: {}", sql_command));

        Ok(())
    }

    async fn close_connection_pool(&self) {
        self.pool.close();
    }
}

// Use Env Vars to tune Insert chunk size/speed
fn rows_per_df(payload: &InsertDataframePayload) -> usize {
    if !INSERT_DELAYABLES.contains(&payload.as_key()) {
        return 10_000;
    }
    std::env::var("ROWS_PER_DF")
        .unwrap_or(10_000.to_string())
        .parse::<usize>()
        .unwrap()
}

fn should_delay_insert(payload: &InsertDataframePayload) -> bool {
    if !INSERT_DELAYABLES.contains(&payload.as_key()) {
        return false;
    }
    std::env::var("DELAY_INSERT")
        .unwrap_or("false".to_string())
        .parse::<bool>()
        .unwrap()
}

fn insert_delay() -> std::time::Duration {
    std::time::Duration::from_millis(
        std::env::var("INSERT_DELAY")
            .unwrap_or(1000.to_string())
            .parse::<u64>()
            .unwrap(),
    )
}

fn preprocess_value(value: &AnyValue) -> String {
    match value {
        AnyValue::String(_) | AnyValue::StringOwned(_) => {
            let string_value = &value.str_value();
            let potential_geometry_value = string_value.to_string();
            let potential_geometry_value = potential_geometry_value.trim();
            let postgres_geometry_type = PostgresGeometryType::new(potential_geometry_value);
            if postgres_geometry_type.is_geometry_type() {
                let formatted_geometry_value =
                    postgres_geometry_type.format_value(potential_geometry_value);
                debug!("Formatted Geometry value: {formatted_geometry_value}");
                formatted_geometry_value
            } else {
                RowStruct::FromString(string_value.to_string()).displayed()
            }
        }
        _ => {
            debug!("On other =>: {}", RowStruct::new(value).displayed());
            debug!("Type of value: {}", value.dtype());
            RowStruct::new(value).displayed()
        }
    }
}

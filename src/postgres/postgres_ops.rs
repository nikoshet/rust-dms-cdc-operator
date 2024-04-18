use anyhow::Result;
use async_trait::async_trait;
use indexmap::IndexMap;
use log::debug;
use polars::prelude::*;

use polars_core::export::num::ToPrimitive;
use sqlx::{Pool, Postgres, Row};
use std::fmt::Display;

use tracing::instrument;
use TableQuery::*;

pub(crate) use super::postgres_operator::PostgresOperator;
use super::table_query::TableQuery;

use crate::postgres::postgres_row_struct::RowStruct;

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
    ) -> Result<IndexMap<String, String>, sqlx::Error> {
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

    async fn get_primary_key(
        &self,
        table_name: &str,
        schema_name: &str,
    ) -> Result<Vec<String>, sqlx::Error> {
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

    async fn create_schema(&self, schema_name: &str) -> Result<(), sqlx::Error> {
        let pg_pool = self.db_client.clone();

        // Prepare the query to create a schema
        let query = CreateSchema(schema_name.to_string());
        sqlx::query(&query.to_string())
            .execute(&pg_pool)
            .await
            .expect("Failed to create schema");

        Ok(())
    }

    async fn create_table(
        &self,
        column_data_types: &IndexMap<String, String>,
        primary_keys: Vec<String>,
        schema_name: &str,
        table_name: &str,
    ) -> Result<(), sqlx::Error> {
        let pg_pool = self.db_client.clone();

        // Prepare the query to create a table
        let query = CreateTable(
            schema_name.to_string(),
            table_name.to_string(),
            column_data_types.clone(),
            primary_keys.as_slice().join(","),
        );
        sqlx::query(&query.to_string())
            .execute(&pg_pool)
            .await
            .expect("Failed to create table");

        Ok(())
    }

    #[instrument(name = "Insert data into table", skip(self))]
    async fn insert_dataframe_in_local_db(
        &self,
        df: DataFrame,
        database_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<()> {
        let pg_pool = self.db_client.clone();

        let column_names = df.get_column_names();
        let fields = column_names.join(", ");

        let insert_rows_number = 100_000;
        let df_height = df
            .height()
            .to_i64()
            .expect("Error while looping through the dataframe");

        let mut offset = 0;
        // Insert rows in chunks to avoid the bulk insert issue of EOF
        while offset <= df_height {
            let df_slice = df.slice(offset, insert_rows_number);
            debug!(
                "Current offset: {}, current df height: {}",
                offset,
                df_slice.height()
            );

            // Construct the query with placeholders
            let values = (0..df_slice.height())
                .map(|row| {
                    df_slice
                        .get_columns()
                        .iter()
                        .map(|column| column.get(row).unwrap())
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();

            let values = values
                .iter()
                .map(|row_values| {
                    let concatenated_row_values = row_values
                        .iter()
                        .map(|v| RowStruct::new(v).displayed())
                        .collect::<Vec<_>>()
                        .join(", ");

                    format!("({concatenated_row_values})")
                })
                .collect::<Vec<_>>()
                .join(", ");

            let query =
                format!("INSERT INTO {schema_name}.{table_name} ({fields}) VALUES {values}");

            sqlx::query(&query)
                .execute(&pg_pool)
                .await
                .expect("Failed to insert data into table");

            offset += insert_rows_number
                .to_i64()
                .expect("Error while incrementing the offset");
        }

        Ok(())
    }

    #[instrument(name = "Upsert data into table", skip(self))]
    async fn upsert_dataframe_in_local_db(
        &self,
        df: DataFrame,
        database_name: &str,
        schema_name: &str,
        table_name: &str,
        primary_key: &str,
    ) -> Result<()> {
        let pg_pool = self.db_client.clone();

        let mut row_values = Vec::new();
        let mut deleted_row: bool;

        let column_names = df.get_column_names();
        let fields = column_names.join(", ");

        for row in 0..df.height() {
            row_values.clear();
            deleted_row = false;

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

                let pk = primary_key
                    .split(',')
                    .map(|key| df.column(key).unwrap().get(row).unwrap().to_string())
                    .collect::<Vec<String>>();

                let query = DeleteRows(
                    schema_name.to_string(),
                    table_name.to_string(),
                    primary_key.to_string(),
                    pk.as_slice().join(","),
                );

                sqlx::query(&query.to_string().replace('"', "'"))
                    .execute(&pg_pool)
                    .await
                    .expect("Failed to delete rows from table");

                deleted_row = true;
                break;
            }

            if deleted_row {
                debug!("Deleted row");
                continue;
            }

            debug!("Row values: {:?}", row_values);
            let values = row_values
                .iter()
                .map(|v| RowStruct::new(v).displayed())
                .collect::<Vec<_>>()
                .join(", ");

            // Operation: Update
            let is_update_op = row_values.first().unwrap().to_string().contains('U');

            let on_conflict_strategy = if !is_update_op {
                String::from("")
            } else {
                let set_values = df
                    .schema()
                    .iter_fields()
                    .enumerate()
                    .filter(|(_, column)| !primary_key.contains(&column.name().to_string()))
                    .map(|(index, column)| {
                        format!(
                            "{} = {}",
                            column.name(),
                            RowStruct::new(row_values.get(index).unwrap()).displayed()
                        )
                    })
                    .collect::<Vec<_>>();

                // Construct the query, on Conflict, update the row
                let strategy = format!(" ON CONFLICT ({}) DO UPDATE SET ", primary_key);
                let concatenated_values = set_values.join(", ");

                format!("{strategy} {concatenated_values}")
            };

            let query =
                format!("INSERT INTO {schema_name}.{table_name} ({fields}) VALUES ({values})");
            let query = format!("{query}{on_conflict_strategy}");

            debug!("Query: {}", query);
            sqlx::query(&query)
                .execute(&pg_pool)
                .await
                .expect("Failed to upsert data into table");
        }

        Ok(())
    }

    async fn drop_dms_columns(
        &self,
        schema_name: &str,
        table_name: &str,
    ) -> Result<(), sqlx::Error> {
        let pg_pool = self.db_client.clone();

        // Prepare the query to drop the columns added by DMS
        let query = DropDmsColumns(schema_name.to_string(), table_name.to_string());

        // Drop the columns added by DMS
        sqlx::query(&query.to_string())
            .execute(&pg_pool)
            .await
            .expect("Failed to drop columns added by DMS");

        Ok(())
    }

    async fn close_connection_pool(&self) {
        self.db_client.close().await;
    }
}

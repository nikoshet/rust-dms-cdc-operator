use crate::postgres::table_query::TableQuery;
use async_trait::async_trait;
use indexmap::IndexMap;
use log::debug;
use polars::prelude::*;
use polars_core::export::num::ToPrimitive;
use rust_decimal::Decimal;
use sqlx::{Pool, Postgres, Row};
use std::fmt::Display;
use TableQuery::*;

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

#[cfg(test)]
use mockall::automock;

#[cfg_attr(test, automock)]
#[async_trait]
pub trait PostgresOperator {
    /// Get the columns of a table.
    ///
    /// # Arguments
    ///
    /// * `schema_name` - The name of the schema.
    /// * `table_name` - The name of the table.
    ///
    /// # Returns
    ///
    /// A IndexMap containing the column names and their data types.
    async fn get_table_columns(
        &self,
        schema_name: &str,
        table_name: &str,
    ) -> Result<IndexMap<String, String>, sqlx::Error>;

    //// Get the primary key of a table.
    ///
    /// # Arguments
    ///
    /// * `table_name` - The name of the table.
    /// * `schema_name` - The name of the schema.
    ///
    /// # Returns
    ///
    /// The primary key of the table.
    async fn get_primary_key(
        &self,
        table_name: &str,
        schema_name: &str,
    ) -> Result<Vec<String>, sqlx::Error>;

    /// Create a schema in the local database.
    ///
    /// # Arguments
    ///
    /// * `schema_name` - The name of the schema.
    ///
    /// # Returns
    ///
    /// A Result indicating success or failure.
    async fn create_schema(&self, schema_name: &str) -> Result<(), sqlx::Error>;

    /// Create a table in the local database.
    ///
    /// # Arguments
    ///
    /// * `column_data_types` - The data types of the columns in the table.
    /// * `primary_key` - The primary key of the table.
    /// * `schema_name` - The name of the schema.
    /// * `table_name` - The name of the table.
    ///
    /// # Returns
    ///
    /// A Result indicating success or failure.
    async fn create_table(
        &self,
        column_data_types: &IndexMap<String, String>,
        primary_key: Vec<String>,
        schema_name: &str,
        table_name: &str,
    ) -> Result<(), sqlx::Error>;

    /// Insert a DataFrame into the local database.
    ///
    /// # Arguments
    ///
    /// * `df` - The DataFrame to insert.
    /// * `schema_name` - The name of the schema.
    /// * `table_name` - The name of the table.
    ///
    /// # Returns
    ///
    /// A Result indicating success or failure.
    async fn insert_dataframe_in_local_db(
        &self,
        df: DataFrame,
        schema_name: &str,
        table_name: &str,
    ) -> Result<(), anyhow::Error>;

    /// Upsert a DataFrame into the local database.
    ///
    /// # Arguments
    ///
    /// * `df` - The DataFrame to upsert.
    /// * `schema_name` - The name of the schema.
    /// * `table_name` - The name of the table.
    /// * `primary_key` - The primary key of the table.
    ///
    /// # Returns
    ///
    /// A Result indicating success or failure.
    async fn upsert_dataframe_in_local_db(
        &self,
        df: DataFrame,
        schema_name: &str,
        table_name: &str,
        primary_key: &str,
    ) -> Result<(), anyhow::Error>;

    /// Drop the columns added by DMS.
    ///
    /// # Arguments
    ///
    /// * `schema_name` - The name of the schema.
    /// * `table_name` - The name of the table.
    ///
    /// # Returns
    ///
    /// A Result indicating success or failure.
    async fn drop_dms_columns(
        &self,
        schema_name: &str,
        table_name: &str,
    ) -> Result<(), sqlx::Error>;

    /// Close the connection pool.
    ///
    /// # Returns
    ///
    /// A Result indicating success or failure.
    async fn close_connection_pool(&self);

    /// Process a string value.
    ///
    /// # Arguments
    ///
    /// * `value` - The string value to process.
    ///
    /// # Returns
    ///
    /// The processed value.
    fn process_string_value(&self, value: &str) -> String;

    /// Process a decimal value.
    ///
    /// # Arguments
    ///
    /// * `integer` - The integer part of the decimal value.
    ///
    /// * `precision` - The precision of the decimal value.
    ///
    /// # Returns
    ///
    /// The processed value.
    fn process_decimal_value(&self, integer: i128, precision: usize) -> String;
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

    async fn insert_dataframe_in_local_db(
        &self,
        df: DataFrame,
        schema_name: &str,
        table_name: &str,
    ) -> Result<(), anyhow::Error> {
        let pg_pool = self.db_client.clone();

        let mut query = format!(
            "INSERT INTO {}.{} ({}) VALUES ",
            &schema_name,
            &table_name,
            df.schema()
                .iter_fields()
                .map(|f| f.name().to_string())
                .collect::<Vec<_>>()
                .join(", ")
        );

        // Construct the query with placeholders
        let mut values = Vec::new();

        for row in 0..df.height() {
            let mut placeholders = Vec::new();
            for column in df.get_columns() {
                let value = column.get(row).unwrap();
                // Push the value to the placeholder vector
                placeholders.push(value);
            }
            // Push the placeholders to the values vector
            values.push(placeholders);
        }

        query.push_str(
            &values
                .iter()
                .map(|row_values| {
                    format!(
                        "({})",
                        row_values
                            .iter()
                            .map(|v| match v {
                                AnyValue::String(v) => self.process_string_value(v),
                                AnyValue::Datetime(_, _, _) => format!("'{}'", v),
                                AnyValue::Date(_) => format!("'{}'", v),
                                AnyValue::Decimal(integer, precision) =>
                                    self.process_decimal_value(*integer, *precision),
                                AnyValue::Binary(v) => {
                                    format!("'{:?}'", v)
                                }
                                AnyValue::Float64(v) => {
                                    format!("{}", v)
                                }
                                _ => format!("{}", v),
                            })
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                })
                .collect::<Vec<_>>()
                .join(", "),
        );

        sqlx::query(&query)
            .execute(&pg_pool)
            .await
            .expect("Failed to insert data into table");

        Ok(())
    }

    async fn upsert_dataframe_in_local_db(
        &self,
        df: DataFrame,
        schema_name: &str,
        table_name: &str,
        primary_key: &str,
    ) -> Result<(), anyhow::Error> {
        let pg_pool = self.db_client.clone();

        let mut row_values = Vec::new();
        let mut deleted_row: bool;

        for row in 0..df.height() {
            row_values.clear();
            deleted_row = false;

            for column in df.get_columns() {
                // Operation: Delete
                // Delete the rows where Op="D"
                if column.name() == "Op" && column.get(row).unwrap().to_string().contains('D') {
                    let mut pk = Vec::new();
                    for key in primary_key.split(',') {
                        pk.push(df.column(key).unwrap().get(row).unwrap().to_string());
                    }

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

                let value = column.get(row).unwrap();
                row_values.push(value);
            }

            if deleted_row {
                debug!("Deleted row");
                continue;
            }

            debug!("Row values: {:?}", row_values);
            let mut query = format!(
                "INSERT INTO {}.{} ({}) VALUES ",
                &schema_name,
                &table_name,
                df.schema()
                    .iter_fields()
                    .map(|f| f.name().to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            );

            query.push_str(&format!(
                "({})",
                row_values
                    .iter()
                    .map(|v| match v {
                        AnyValue::String(v) => self.process_string_value(v),
                        AnyValue::Datetime(_, _, _) => format!("'{}'", v),
                        AnyValue::Date(_) => format!("'{}'", v),
                        AnyValue::Decimal(integer, precision) =>
                            self.process_decimal_value(*integer, *precision),
                        AnyValue::Binary(v) => {
                            format!("'{:?}'", v)
                        }
                        AnyValue::Float64(v) => {
                            format!("{}", v)
                        }
                        _ => format!("{}", v),
                    })
                    .collect::<Vec<_>>()
                    .join(", ")
            ));

            // Operation: Update
            if row_values.first().unwrap().to_string().contains('U') {
                // Construct the query, on Conflict, update the row
                query.push_str(&format!(" ON CONFLICT ({}) DO UPDATE SET ", primary_key));
                let mut set_values = Vec::new();
                for (index, column) in df.schema().iter_fields().enumerate() {
                    if primary_key.contains(&column.name().to_string()) {
                        continue;
                    }
                    set_values.push(format!(
                        "{} = {}",
                        column.name(),
                        match row_values.get(index).unwrap() {
                            AnyValue::String(v) => self.process_string_value(v),
                            AnyValue::Datetime(_, _, _) =>
                                format!("'{}'", row_values.get(index).unwrap()),
                            AnyValue::Date(_) => format!("'{}'", row_values.get(index).unwrap()),
                            AnyValue::Decimal(integer, precision) =>
                                self.process_decimal_value(*integer, *precision),
                            AnyValue::Binary(v) => {
                                format!("'{:?}'", v)
                            }
                            AnyValue::Float64(v) => {
                                format!("{}", v)
                            }
                            _ => format!("{}", row_values.get(index).unwrap()),
                        }
                    ));
                }
                query.push_str(&set_values.join(", "));
            }

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

    fn process_string_value(&self, v: &str) -> String {
        // The fields that are of JSON type on polars DataFrame are stored as String,
        // so we can parse them like this to maintain order
        let v = v.replace('\'', "''");
        format!("'{}'", v)
    }

    fn process_decimal_value(&self, integer: i128, precision: usize) -> String {
        let decimal = Decimal::new(
            integer.to_i64().expect("Could not convert value to i64"),
            precision.to_u32().expect("Could not convert value to u32"),
        );
        format!("{:}", decimal)
    }
}

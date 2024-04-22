use anyhow::Result;
use async_trait::async_trait;

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
    ) -> Result<indexmap::IndexMap<String, String>, sqlx::Error>;

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

    /// Create a schema in the target database.
    ///
    /// # Arguments
    ///
    /// * `schema_name` - The name of the schema.
    ///
    /// # Returns
    ///
    /// A Result indicating success or failure.
    async fn create_schema(&self, schema_name: &str) -> Result<(), sqlx::Error>;

    /// Create a table in the target database.
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
        column_data_types: &indexmap::IndexMap<String, String>,
        primary_key: Vec<String>,
        schema_name: &str,
        table_name: &str,
    ) -> Result<(), sqlx::Error>;

    /// Insert a DataFrame into the target database.
    ///
    /// # Arguments
    ///
    /// * `df` - The DataFrame to insert.
    /// * `database_name` - The name of the database.
    /// * `schema_name` - The name of the schema.
    /// * `table_name` - The name of the table.
    ///
    /// # Returns
    ///
    /// A Result indicating success or failure.
    async fn insert_dataframe_in_target_db(
        &self,
        df: polars::frame::DataFrame,
        database_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<()>;

    /// Upsert a DataFrame into the target database.
    ///
    /// # Arguments
    ///
    /// * `df` - The DataFrame to upsert.
    /// * `database_name` - The name of the database.
    /// * `schema_name` - The name of the schema.
    /// * `table_name` - The name of the table.
    /// * `primary_key` - The primary key of the table.
    ///
    /// # Returns
    ///
    /// A Result indicating success or failure.
    async fn upsert_dataframe_in_target_db(
        &self,
        df: polars::frame::DataFrame,
        database_name: &str,
        schema_name: &str,
        table_name: &str,
        primary_key: &str,
    ) -> Result<()>;

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
}

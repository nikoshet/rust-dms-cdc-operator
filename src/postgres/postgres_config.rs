use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;

/// Represents a Postgres config that connects to a Postgres database.
pub struct PostgresConfig {
    postgres_url: String,
    database_schema: String,
    table_names: Vec<String>,
    max_connections: u32,
}
impl PostgresConfig {
    /// Creates a new Postgres config.
    ///
    /// # Arguments
    ///
    /// * `postgres_url` - The Postgres URL
    /// * `database_schema` - The schema of the database
    /// * `table_names` - The list of table names
    /// * `max_connections` - The maximum number of connections
    ///
    /// # Returns
    ///
    /// A new Postgres config instance.
    pub fn new(
        postgres_url: impl Into<String>,
        database_schema: impl Into<String>,
        table_names: Vec<impl Into<String>>,
        max_connections: u32,
    ) -> Self {
        PostgresConfig {
            postgres_url: postgres_url.into(),
            database_schema: database_schema.into(),
            table_names: table_names.into_iter().map(|t| t.into()).collect(),
            max_connections,
        }
    }

    /// Gets the schema name.
    pub fn schema_name(&self) -> &str {
        &self.database_schema
    }

    /// Gets the table name.
    pub fn table_names(&self) -> Vec<String> {
        self.table_names.clone()
    }

    /// Gets the database name.
    pub fn database_name(&self) -> String {
        self.postgres_url.split('/').last().unwrap().to_string()
    }

    /// Connects to the Postgres database.
    ///
    /// # Returns
    ///
    /// A connection pool to the Postgres database.
    pub async fn connect_to_postgres(&self) -> PgPool {
        let connection_string = self.postgres_url.to_string();
        let max_connections = self.max_connections;
        PgPoolOptions::new()
            .max_connections(max_connections)
            .connect(&connection_string)
            .await
            .expect("Failed to connect to DB")
    }

    /// Returns the connection string.
    pub fn connection_string(&self) -> String {
        self.postgres_url.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_postgres_config() {
        let table_list = vec!["table1", "table2"];
        let config = PostgresConfig::new(
            "postgres://postgres:postgres@localhost:5432/mydb",
            "database_schema",
            table_list.clone(),
            100,
        );

        assert_eq!(
            config.postgres_url,
            "postgres://postgres:postgres@localhost:5432/mydb"
        );
        assert_eq!(config.database_schema, "database_schema");
        assert_eq!(config.table_names, table_list);
    }

    #[test]
    fn test_connection_string() {
        let table_list = vec!["table1", "table2"];
        let config = PostgresConfig::new(
            "postgres://postgres:postgres@localhost:5432/mydb",
            "database_schema",
            table_list,
            100,
        );

        assert_eq!(
            config.connection_string(),
            "postgres://postgres:postgres@localhost:5432/mydb"
        );
    }
}

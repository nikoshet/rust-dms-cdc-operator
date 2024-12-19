use bon::bon;
use deadpool_postgres::{tokio_postgres::NoTls, Config, Pool, Runtime};

#[allow(dead_code)]
/// Represents a Postgres config that connects to a Postgres database.
pub struct PostgresConfig {
    postgres_url: String,
    database_schema: String,
    max_connections: u32,
}

#[allow(dead_code)]
#[bon]
impl PostgresConfig {
    /// Creates a new Postgres config.
    ///
    /// # Arguments
    ///
    /// * `postgres_url` - The Postgres URL
    /// * `database_schema` - The schema of the database
    /// * `max_connections` - The maximum number of connections
    ///
    /// # Returns
    ///
    /// A new Postgres config instance.
    #[builder]
    pub fn new(
        postgres_url: impl Into<String>,
        database_schema: impl Into<String>,
        max_connections: u32,
    ) -> Self {
        PostgresConfig {
            postgres_url: postgres_url.into(),
            database_schema: database_schema.into(),
            max_connections,
        }
    }

    /// Gets the schema name.
    pub fn schema_name(&self) -> &str {
        &self.database_schema
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
    pub async fn connect_to_postgres(&self, accept_invalid_certs: bool) -> Pool {
        let connection_string = self.postgres_url.to_string();
        let max_connections: usize = self.max_connections as usize;
        let mut cfg = Config::new();
        cfg.url = Some(connection_string);
        cfg.pool = Some(deadpool_postgres::PoolConfig::new(max_connections));

        let tls_connector = if accept_invalid_certs {
            use native_tls::TlsConnector;
            use postgres_native_tls::MakeTlsConnector;

            let tls_connector = TlsConnector::builder()
                .danger_accept_invalid_certs(true)
                .danger_accept_invalid_hostnames(true)
                .build()
                .unwrap();

            Some(MakeTlsConnector::new(tls_connector))
        } else {
            None
        };

        if accept_invalid_certs {
            cfg.create_pool(Some(Runtime::Tokio1), tls_connector.clone().unwrap())
                .unwrap()
        } else {
            cfg.create_pool(Some(Runtime::Tokio1), NoTls).unwrap()
        }
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
        let config = PostgresConfig::builder()
            .postgres_url("postgres://postgres:postgres@localhost:5432/mydb")
            .database_schema("database_schema")
            .max_connections(100)
            .build();

        assert_eq!(
            config.postgres_url,
            "postgres://postgres:postgres@localhost:5432/mydb"
        );
        assert_eq!(config.database_schema, "database_schema");
    }

    #[test]
    fn test_connection_string() {
        let config = PostgresConfig::builder()
            .postgres_url("postgres://postgres:postgres@localhost:5432/mydb")
            .database_schema("database_schema")
            .max_connections(100)
            .build();

        assert_eq!(
            config.connection_string(),
            "postgres://postgres:postgres@localhost:5432/mydb"
        );
    }
}

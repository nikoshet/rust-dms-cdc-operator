/// Represents a validator payload that validates the data between S3 and a target database.
#[derive(Clone)]
pub struct ValidatorPayload {
    bucket_name: String,
    s3_prefix: String,
    source_postgres_url: String,
    target_postgres_url: String,
    database_schema: String,
    table_names: Vec<String>,
    start_date: String,
    stop_date: Option<String>,
    chunk_size: i64,
    max_connections: u32,
    start_position: i64,
    only_datadiff: bool,
    only_snapshot: bool,
}

impl ValidatorPayload {
    /// Creates a new validator payload.
    ///
    /// # Arguments
    ///
    /// * `bucket_name` - The name of the S3 bucket.
    /// * `s3_prefix` - The prefix of the S3 bucket.
    /// * `source_postgres_url` - The source Postgres URL.
    /// * `target_postgres_url` - The target Postgres URL.
    /// * `database_schema` - The schema of the database.
    /// * `table_names` - The list of tables to include for validation.
    /// * `start_date` - Will be used to constract a key from which Amazon will start listing files after that key.
    /// * `stop_date` - Will be used to stop listing files after that date.
    /// * `chunk_size` - The chunk size for pgdatadiff validation.
    /// * `max_connections` - The maximum number of connections to the Postgres database.
    /// * `start_position` - The start position for pgdatadiff validation.
    /// * `only_datadiff` - Whether to only validate the data difference.
    /// * `only_snapshot` - Whether to only take a snapshot and skip validation.
    ///
    /// # Returns
    ///
    /// A new validator instance.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        bucket_name: impl Into<String>,
        s3_prefix: impl Into<String>,
        source_postgres_url: impl Into<String>,
        target_postgres_url: impl Into<String>,
        database_schema: impl Into<String>,
        table_names: Vec<impl Into<String>>,
        start_date: impl Into<String>,
        stop_date: impl Into<Option<String>>,
        chunk_size: i64,
        max_connections: u32,
        start_position: i64,
        only_datadiff: bool,
        only_snapshot: bool,
    ) -> Self {
        if only_datadiff && only_snapshot {
            panic!("Cannot run both only_datadiff and only_snapshot at the same time");
        }

        Self {
            bucket_name: bucket_name.into(),
            s3_prefix: s3_prefix.into(),
            source_postgres_url: source_postgres_url.into(),
            target_postgres_url: target_postgres_url.into(),
            database_schema: database_schema.into(),
            table_names: table_names.into_iter().map(|t| t.into()).collect(),
            start_date: start_date.into(),
            stop_date: stop_date.into(),
            chunk_size,
            max_connections,
            start_position,
            only_datadiff,
            only_snapshot,
        }
    }

    pub fn bucket_name(&self) -> &str {
        &self.bucket_name
    }

    pub fn s3_prefix(&self) -> &str {
        &self.s3_prefix
    }

    pub fn source_postgres_url(&self) -> &str {
        &self.source_postgres_url
    }

    pub fn target_postgres_url(&self) -> &str {
        &self.target_postgres_url
    }

    pub fn database_name(&self) -> String {
        self.source_postgres_url
            .split('/')
            .last()
            .unwrap()
            .to_string()
    }

    pub fn schema_name(&self) -> &str {
        &self.database_schema
    }

    pub fn table_names(&self) -> &[String] {
        &self.table_names
    }

    pub fn start_date(&self) -> &str {
        &self.start_date
    }

    pub fn stop_date(&self) -> Option<&str> {
        self.stop_date.as_deref()
    }

    pub fn chunk_size(&self) -> i64 {
        self.chunk_size
    }

    pub fn max_connections(&self) -> u32 {
        self.max_connections
    }

    pub fn start_position(&self) -> i64 {
        self.start_position
    }

    pub fn only_datadiff(&self) -> bool {
        self.only_datadiff
    }

    pub fn only_snapshot(&self) -> bool {
        self.only_snapshot
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
        let source_postgres_url = "postgres://postgres:postgres@localhost:5432/mydb";
        let target_postgres_url = "postgres://postgres:postgres@localhost:5432/mydb";
        let database_schema = "public";
        let table_names = vec!["table1", "table2"];
        let start_date = "2021-01-01";
        let stop_date = Some("2021-01-02".to_string());
        let chunk_size = 1000;
        let max_connections = 100;
        let start_position = 0;
        let only_datadiff = true;
        let only_snapshot = true;

        let _validator = ValidatorPayload::new(
            bucket_name,
            s3_prefix,
            source_postgres_url,
            target_postgres_url,
            database_schema,
            table_names,
            start_date,
            stop_date,
            chunk_size,
            max_connections,
            start_position,
            only_datadiff,
            only_snapshot,
        );
    }
}

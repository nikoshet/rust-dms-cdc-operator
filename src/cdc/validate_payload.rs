pub struct CDCOperatorValidatePayload {
    pub source_postgres_url: String,
    pub target_postgres_url: String,
    pub included_tables: Vec<String>,
    pub excluded_tables: Vec<String>,
    pub schema_name: String,
    pub chunk_size: i64,
    pub start_position: i64,
}

impl CDCOperatorValidatePayload {
    pub fn new(
        source_postgres_url: impl Into<String>,
        target_postgres_url: impl Into<String>,
        included_tables: Vec<impl Into<String>>,
        excluded_tables: Vec<impl Into<String>>,
        schema_name: impl Into<String>,
        chunk_size: i64,
        start_position: i64,
    ) -> Self {
        CDCOperatorValidatePayload {
            source_postgres_url: source_postgres_url.into(),
            target_postgres_url: target_postgres_url.into(),
            included_tables: included_tables.into_iter().map(|t| t.into()).collect(),
            excluded_tables: excluded_tables.into_iter().map(|t| t.into()).collect(),
            schema_name: schema_name.into(),
            chunk_size,
            start_position,
        }
    }

    pub fn source_postgres_url(&self) -> String {
        self.source_postgres_url.clone()
    }

    pub fn target_postgres_url(&self) -> String {
        self.target_postgres_url.clone()
    }

    pub fn included_tables(&self) -> Vec<String> {
        self.included_tables.clone()
    }

    pub fn excluded_tables(&self) -> Vec<String> {
        self.excluded_tables.clone()
    }

    pub fn schema_name(&self) -> String {
        self.schema_name.clone()
    }

    pub fn chunk_size(&self) -> i64 {
        self.chunk_size
    }

    pub fn start_position(&self) -> i64 {
        self.start_position
    }
}

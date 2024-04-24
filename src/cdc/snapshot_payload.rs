#[allow(clippy::too_many_arguments)]
pub struct CDCOperatorSnapshotPayload {
    pub bucket_name: String,
    pub key: String,
    pub database_name: String,
    pub schema_name: String,
    pub table_names: Vec<String>,
    pub start_date: String,
    pub stop_date: Option<String>,
    pub only_datadiff: bool,
}

impl CDCOperatorSnapshotPayload {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        bucket_name: impl Into<String>,
        key: impl Into<String>,
        database_name: impl Into<String>,
        schema_name: impl Into<String>,
        table_names: Vec<impl Into<String>>,
        start_date: impl Into<String>,
        stop_date: Option<String>,
        only_datadiff: bool,
    ) -> Self {
        CDCOperatorSnapshotPayload {
            bucket_name: bucket_name.into(),
            key: key.into(),
            database_name: database_name.into(),
            schema_name: schema_name.into(),
            table_names: table_names.into_iter().map(|x| x.into()).collect(),
            start_date: start_date.into(),
            stop_date,
            only_datadiff,
        }
    }

    pub fn bucket_name(&self) -> String {
        self.bucket_name.clone()
    }

    pub fn key(&self) -> String {
        self.key.clone()
    }

    pub fn database_name(&self) -> String {
        self.database_name.clone()
    }

    pub fn schema_name(&self) -> String {
        self.schema_name.clone()
    }

    pub fn table_names(&self) -> Vec<String> {
        self.table_names.clone()
    }

    pub fn start_date(&self) -> String {
        self.start_date.clone()
    }

    pub fn stop_date(&self) -> Option<String> {
        self.stop_date.clone()
    }

    pub fn only_datadiff(&self) -> bool {
        self.only_datadiff
    }
}

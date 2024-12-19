use bon::bon;

use crate::postgres::table_mode::TableMode;

use super::cdc_operator_mode::ModeValueEnum;

#[allow(clippy::too_many_arguments)]
#[derive(Debug)]
pub struct CDCOperatorSnapshotPayload {
    bucket_name: String,
    key: String,
    database_name: String,
    schema_name: String,
    included_tables: Vec<String>,
    excluded_tables: Vec<String>,
    mode: ModeValueEnum,
    start_date: Option<String>,
    stop_date: Option<String>,
    source_postgres_url: String,
    target_postgres_url: String,
}

#[bon]
impl CDCOperatorSnapshotPayload {
    #[builder]
    pub fn new(
        bucket_name: impl Into<String>,
        key: impl Into<String>,
        database_name: impl Into<String>,
        schema_name: impl Into<String>,
        included_tables: Vec<impl Into<String>>,
        excluded_tables: Vec<impl Into<String>>,
        mode: ModeValueEnum,
        start_date: Option<String>,
        stop_date: Option<String>,
        source_postgres_url: String,
        target_postgres_url: String,
    ) -> Self {
        CDCOperatorSnapshotPayload {
            bucket_name: bucket_name.into(),
            key: key.into(),
            database_name: database_name.into(),
            schema_name: schema_name.into(),
            included_tables: included_tables.into_iter().map(|x| x.into()).collect(),
            excluded_tables: excluded_tables.into_iter().map(|x| x.into()).collect(),
            mode,
            start_date,
            stop_date,
            source_postgres_url,
            target_postgres_url,
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

    pub fn included_tables(&self) -> Vec<String> {
        self.included_tables.clone()
    }

    pub fn excluded_tables(&self) -> Vec<String> {
        self.excluded_tables.clone()
    }

    pub fn table_mode(&self) -> TableMode {
        if !self.included_tables.is_empty() {
            TableMode::IncludeTables
        } else if !self.excluded_tables.is_empty() {
            TableMode::ExcludeTables
        } else {
            TableMode::AllTables
        }
    }

    pub fn start_date(&self) -> Option<String> {
        self.start_date.clone()
    }

    pub fn stop_date(&self) -> Option<String> {
        self.stop_date.clone()
    }

    pub fn mode_is_date_aware(&self) -> bool {
        self.mode == ModeValueEnum::DateAware
    }

    pub fn mode_is_absolute_path(&self) -> bool {
        self.mode == ModeValueEnum::AbsolutePath
    }

    pub fn mode_is_full_load_only(&self) -> bool {
        self.mode == ModeValueEnum::FullLoadOnly
    }

    pub fn source_postgres_url(&self) -> String {
        self.source_postgres_url.clone()
    }

    pub fn target_postgres_url(&self) -> String {
        self.target_postgres_url.clone()
    }
}

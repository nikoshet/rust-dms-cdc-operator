use anyhow::Result;
use async_trait::async_trait;
use aws_sdk_s3::primitives::{DateTime, DateTimeFormat};
use aws_sdk_s3::Client as S3Client;
use chrono::{Datelike, NaiveDate};
use log::{debug, info};

#[cfg(test)]
use mockall::automock;

pub enum LoadParquetFilesPayload {
    DateAware {
        bucket_name: String,
        s3_prefix: String,
        database_name: String,
        schema_name: String,
        table_name: String,
        start_date: String,
        stop_date: Option<String>,
    },
    AbsolutePath(String),
}

#[derive(Debug)]
pub struct S3ParquetFile {
    pub file_name: String,
}

impl S3ParquetFile {
    pub fn new(file_name: impl Into<String>) -> Self {
        Self {
            file_name: file_name.into(),
        }
    }

    pub fn is_load_file(&self) -> bool {
        self.file_name.contains("LOAD")
    }

    pub fn is_first_load_file(&self) -> bool {
        self.is_load_file() && self.file_name.contains('1')
    }
}

#[cfg_attr(test, automock)]
#[async_trait]
pub trait S3Operator {
    /// Gets the list of Parquet files from S3.
    ///
    /// # Arguments
    ///
    /// * `bucket_name` - The name of the S3 bucket
    /// * `s3_prefix` - The prefix of the S3 bucket
    /// * `database_name` - The name of the database
    /// * `database_schema` - The schema of the database
    /// * `table_name` - The name of the table
    /// * `start_date` - The start date
    /// * `stop_date` - The stop date
    ///
    /// # Returns
    ///
    /// A list of Parquet files.
    #[allow(clippy::too_many_arguments)]
    async fn get_list_of_parquet_files_from_s3(
        &self,
        load_parquet_files_payload: &LoadParquetFilesPayload,
    ) -> Result<Vec<S3ParquetFile>>;

    /// Gets the list of files from S3 based on the date.
    ///
    /// # Arguments
    ///
    /// * `bucket_name` - The name of the S3 bucket
    /// * `table_name` - The name of the table
    /// * `start_date_path` - The start date path
    /// * `prefix_path` - The prefix path
    /// * `start_date` - The start date to include the files
    /// * `stop_date` - The stop date to include the files
    ///
    /// # Returns
    ///
    /// A list of files.
    async fn get_files_from_s3_based_on_date(
        &self,
        bucket_name: &str,
        table_name: &str,
        start_date_path: &str,
        prefix_path: &str,
        start_date: &DateTime,
        stop_date: Option<DateTime>,
    ) -> Result<Vec<S3ParquetFile>>;
}

pub struct S3OperatorImpl<'a> {
    s3_client: &'a S3Client,
}

impl<'a> S3OperatorImpl<'a> {
    pub fn new(s3_client: &'a S3Client) -> Self {
        Self { s3_client }
    }
}

#[async_trait]
impl S3Operator for S3OperatorImpl<'_> {
    async fn get_list_of_parquet_files_from_s3(
        &self,
        s3_parquet_file_load_key: &LoadParquetFilesPayload,
    ) -> Result<Vec<S3ParquetFile>> {
        let parquet_files = match s3_parquet_file_load_key {
            LoadParquetFilesPayload::DateAware {
                bucket_name,
                s3_prefix,
                database_name,
                schema_name,
                table_name,
                start_date,
                stop_date,
            } => {
                let iter_start_date =
                    NaiveDate::parse_from_str(start_date.as_str(), "%Y-%m-%dT%H:%M:%SZ")?;
                let year = iter_start_date.year();
                let month = format!("{:02}", iter_start_date.month());
                let day = format!("{:02}", iter_start_date.day());
                let prefix_path = format!(
                    "{}/{}/{}/{}",
                    s3_prefix, database_name, schema_name, table_name
                );
                let start_date_path = format!("{}/{}/{}/{}/", prefix_path, year, month, day);

                let start_date =
                    DateTime::from_str(start_date.as_str(), DateTimeFormat::DateTimeWithOffset)?;
                let stop_date = if stop_date.is_none() {
                    None
                } else {
                    Some(DateTime::from_str(
                        stop_date.as_ref().unwrap().as_str(),
                        DateTimeFormat::DateTimeWithOffset,
                    )?)
                };

                let mut files_list: Vec<S3ParquetFile> = self
                    .get_files_from_s3_based_on_date(
                        bucket_name.as_str(),
                        table_name.as_str(),
                        start_date_path.as_str(),
                        format!("{}/", prefix_path).as_str(),
                        &start_date,
                        stop_date,
                    )
                    .await?;

                // We want to process the LOAD files first in INSERT mode, so we rotate the list,
                // Then, we will process the rest CDC files in UPSERT mode.
                let load_files_count = files_list.iter().filter(|s| s.is_load_file()).count();
                files_list.rotate_right(load_files_count);
                files_list
            }
            LoadParquetFilesPayload::AbsolutePath(absolute_path) => {
                vec![S3ParquetFile::new(absolute_path.to_string())]
            }
        };

        Ok(parquet_files)
    }

    async fn get_files_from_s3_based_on_date(
        &self,
        bucket_name: &str,
        table_name: &str,
        start_date_path: &str,
        prefix_path: &str,
        start_date: &DateTime,
        stop_date: Option<DateTime>,
    ) -> Result<Vec<S3ParquetFile>> {
        let mut files: Vec<String> = Vec::new();
        let mut next_token = None;

        loop {
            let builder = self
                .s3_client
                .list_objects_v2()
                .bucket(bucket_name)
                .start_after(start_date_path)
                .prefix(prefix_path);

            let response = if next_token.is_some() {
                builder
                    .continuation_token(next_token.clone().unwrap())
                    .send()
                    .await
                    .map_err(aws_sdk_s3::Error::from)?
            } else {
                builder
                    .to_owned()
                    .send()
                    .await
                    .map_err(aws_sdk_s3::Error::from)?
            };

            next_token.clone_from(&response.next_continuation_token);

            if let Some(contents) = response.contents {
                for object in contents.clone() {
                    let file = object.key.unwrap();
                    // Filter files based on last modified date
                    if let Some(last_modified) = object.last_modified {
                        if let Some(stop_date) = stop_date {
                            if (last_modified > *start_date && last_modified < stop_date)
                                || file.contains("LOAD")
                            {
                                debug!("File: {:?}", file);
                                files.push(file);
                            }
                        } else if last_modified > *start_date || file.contains("LOAD") {
                            debug!("File: {:?}", file);
                            files.push(file);
                        }
                    }
                }
            }
            if next_token.is_none() {
                info!("Files to process for table {table_name}: {:?}", files.len());
                break;
            }
        }

        let files = files
            .iter()
            .map(|f| S3ParquetFile::new(f.to_string()))
            .collect::<Vec<_>>();

        Ok(files)
    }
}

use anyhow::Result;
use async_trait::async_trait;
use aws_sdk_s3::primitives::{DateTime, DateTimeFormat};
use aws_sdk_s3::Client as S3Client;
use chrono::{Datelike, NaiveDate};
use log::{debug, info};

#[cfg(test)]
use mockall::automock;

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
        bucket_name: &str,
        s3_prefix: &str,
        database_name: &str,
        database_schema: &str,
        table_name: &str,
        start_date: &str,
        stop_date: Option<String>,
    ) -> Result<Vec<String>>;

    /// Gets the list of files from S3 based on the date.
    ///
    /// # Arguments
    ///
    /// * `bucket_name` - The name of the S3 bucket
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
        start_date_path: String,
        prefix_path: String,
        start_date: DateTime,
        stop_date: Option<DateTime>,
    ) -> Result<Vec<String>>;
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
        bucket_name: &str,
        s3_prefix: &str,
        database_name: &str,
        database_schema: &str,
        table_name: &str,
        start_date: &str,
        stop_date: Option<String>,
    ) -> Result<Vec<String>> {
        let prefix_path = format!(
            "{}/{}/{}/{}",
            s3_prefix, database_name, database_schema, table_name
        );

        let iter_start_date = NaiveDate::parse_from_str(start_date, "%Y-%m-%dT%H:%M:%SZ")?;
        let year = iter_start_date.year();
        let month = format!("{:02}", iter_start_date.month());
        let day = format!("{:02}", iter_start_date.day());
        let start_date_path = format!("{}/{}/{}/{}/", prefix_path, year, month, day);

        let start_date = DateTime::from_str(start_date, DateTimeFormat::DateTimeWithOffset)?;
        let stop_date = if stop_date.is_none() {
            None
        } else {
            Some(DateTime::from_str(
                &stop_date.unwrap(),
                DateTimeFormat::DateTimeWithOffset,
            )?)
        };

        let mut files_list: Vec<String>;
        files_list = Self::get_files_from_s3_based_on_date(
            self,
            bucket_name,
            start_date_path,
            format!("{}/", prefix_path),
            start_date,
            stop_date,
        )
        .await?;

        // We want to process the LOAD files first in INSERT mode, so we rotate the list,
        // Then, we will process the rest CDC files in UPSERT mode.
        let load_files_count = files_list.iter().filter(|s| s.contains("LOAD")).count();
        files_list.rotate_right(load_files_count);
        Ok(files_list)
    }

    async fn get_files_from_s3_based_on_date(
        &self,
        bucket_name: &str,
        start_date_path: String,
        prefix_path: String,
        start_date: DateTime,
        stop_date: Option<DateTime>,
    ) -> Result<Vec<String>> {
        let mut files: Vec<String> = Vec::new();
        let mut next_token = None;

        loop {
            let builder = self
                .s3_client
                .list_objects_v2()
                .bucket(bucket_name)
                .start_after(&start_date_path)
                .prefix(&prefix_path);

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

            next_token = response.next_continuation_token.clone();

            if let Some(contents) = response.contents {
                for object in contents.clone() {
                    let file = object.key.unwrap();
                    // Filter files based on last modified date
                    if let Some(last_modified) = object.last_modified {
                        if let Some(stop_date) = stop_date {
                            if (last_modified > start_date && last_modified < stop_date)
                                || file.contains("LOAD")
                            {
                                debug!("File: {:?}", file);
                                files.push(file);
                            }
                        } else if last_modified > start_date || file.contains("LOAD") {
                            debug!("File: {:?}", file);
                            files.push(file);
                        }
                    }
                }
            }
            if next_token.is_none() {
                info!("Files to process: {:?}", files.clone().len());
                break;
            }
        }
        Ok(files)
    }
}

pub async fn create_s3_client() -> S3Client {
    let config = aws_config::load_from_env().await;
    S3Client::new(&config)
}

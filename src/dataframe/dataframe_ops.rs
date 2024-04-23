use anyhow::Result;
use async_trait::async_trait;
use aws_sdk_s3::Client as S3Client;
use log::debug;
use polars::prelude::*;

#[cfg(test)]
use mockall::automock;

#[derive(Clone)]
pub struct CreateDataframePayload {
    pub bucket_name: String,
    pub key: String,
    pub database_name: String,
    pub schema_name: String,
    pub table_name: String,
}

#[cfg_attr(test, automock)]
#[async_trait]
pub trait DataframeOperator {
    /// Reads a Parquet file from S3.
    ///
    /// # Arguments
    ///
    /// * `payload` - The payload to create a DataFrame from a Parquet file.
    ///
    /// # Returns
    ///
    /// A DataFrame.
    async fn create_dataframe_from_parquet_file(
        &self,
        payload: CreateDataframePayload,
    ) -> Result<DataFrame>;
}

pub struct DataframeOperatorImpl<'a> {
    s3_client: &'a S3Client,
}

impl<'a> DataframeOperatorImpl<'a> {
    pub fn new(s3_client: &'a S3Client) -> Self {
        Self { s3_client }
    }
}

#[async_trait]
impl DataframeOperator for DataframeOperatorImpl<'_> {
    async fn create_dataframe_from_parquet_file(
        &self,
        payload: CreateDataframePayload,
    ) -> Result<DataFrame> {
        // If we used LazyFrame, we would have an issue with tokio, since we should have to block on the tokio runtime untill the
        // result is ready with .collect(). To avoid this, we use the ParquetReader, which is a synchronous reader.
        // For LazyFrame, we would have to use the following code:
        // let full_path = format!("s3://{}/{}", bucket_name, &key);
        // let df = LazyFrame::scan_parquet(full_path, ScanArgsParquet::default())?
        //     .with_streaming(true)
        //     .select([
        //         // select all columns
        //         all(),
        //     ])
        //     .collect()?;
        // debug!("{:?}", df.schema());
        // Ok(df)

        let object = self
            .s3_client
            .get_object()
            .bucket(payload.bucket_name)
            .key(payload.key)
            .send()
            .await
            .unwrap();

        let bytes = object.body.collect().await.unwrap().into_bytes();
        let cursor = std::io::Cursor::new(bytes);

        let reader = ParquetReader::new(cursor);
        let df = reader
            .read_parallel(ParallelStrategy::RowGroups)
            .finish()
            .unwrap();
        debug!("First row: {:?}", df.get(0).unwrap());
        debug!("{:?}", df.schema());
        Ok(df)
    }
}

#[cfg(test)]
mod tests {
    use polars::frame::DataFrame;

    use crate::dataframe::dataframe_ops::{
        CreateDataframePayload, DataframeOperator, MockDataframeOperator,
    };

    #[tokio::test]
    async fn test_create_dataframe_from_parquet_file() {
        let mut dataframe_operator = MockDataframeOperator::new();

        dataframe_operator
            .expect_create_dataframe_from_parquet_file()
            .returning(|_| Ok(DataFrame::empty()));

        let create_dataframe_payload = CreateDataframePayload {
            bucket_name: "bucket_name".to_string(),
            key: "key".to_string(),
            database_name: "database_name".to_string(),
            schema_name: "schema_name".to_string(),
            table_name: "table_name".to_string(),
        };

        let df = dataframe_operator
            .create_dataframe_from_parquet_file(create_dataframe_payload)
            .await
            .unwrap();

        assert_eq!(df.height(), 0);
    }
}

#[cfg(test)]
mod tests {
    use crate::s3::s3_ops::MockS3Operator;
    use crate::s3::s3_ops::S3Operator;
    use aws_sdk_s3::primitives::{DateTime, DateTimeFormat};

    #[tokio::test]
    async fn test_get_list_of_parquet_files_from_s3() {
        let mut s3_operator = MockS3Operator::new();

        let bucket_name = "bucket_name";
        let s3_prefix = "s3_prefix";
        let database_name = "database_name";
        let database_schema = "database_schema";
        let table_name = "table_name";
        let start_date = "2021-01-01T00:00:00Z";
        let stop_date = Some("2021-01-01T00:00:00Z".to_string());

        s3_operator
            .expect_get_list_of_parquet_files_from_s3()
            .returning(|_, _, _, _, _, _, _| {
                Ok(vec![
                    format!("bucket_name/s3_prefix/file.parquet").to_string()
                ])
            });

        let files = s3_operator
            .get_list_of_parquet_files_from_s3(
                bucket_name,
                s3_prefix,
                database_name,
                database_schema,
                table_name,
                start_date,
                stop_date,
            )
            .await
            .unwrap();

        assert_eq!(files.len(), 1);
    }

    #[tokio::test]
    async fn test_get_files_from_s3_based_on_date() {
        let mut s3_operator = MockS3Operator::new();

        s3_operator
            .expect_get_files_from_s3_based_on_date()
            .returning(|_, _, _, _, _| Ok(vec!["file1".to_string()]));

        let bucket_name = "bucket_name".to_string();
        let start_date_path = "start_date_path".to_string();
        let prefix_path = "prefix_path".to_string();
        let start_date =
            DateTime::from_str("2021-01-01T00:00:00Z", DateTimeFormat::DateTimeWithOffset).unwrap();
        let stop_date = Some(
            DateTime::from_str("2021-01-01T00:00:00Z", DateTimeFormat::DateTimeWithOffset).unwrap(),
        );

        let files = s3_operator
            .get_files_from_s3_based_on_date(
                &bucket_name,
                start_date_path,
                prefix_path,
                start_date,
                stop_date,
            )
            .await
            .unwrap();

        assert_eq!(files.len(), 1);
    }
}

#[cfg(test)]
mod tests {
    use indexmap::IndexMap;
    use mockall::predicate::*;
    use polars::prelude::*;

    use crate::postgres::{
        postgres_operator::MockPostgresOperator, postgres_ops::PostgresOperator,
    };

    #[tokio::test]
    async fn test_get_table_columns() {
        let mut postgres_operator = MockPostgresOperator::new();
        postgres_operator
            .expect_get_table_columns()
            .times(1)
            .with(eq("schema"), eq("table"))
            .returning(|_, _| {
                let mut columns = IndexMap::new();
                columns.insert("column1".to_string(), "text".to_string());
                columns.insert("column2".to_string(), "text".to_string());
                Ok(columns)
            });

        let result = postgres_operator
            .get_table_columns("schema", "table")
            .await
            .unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result.get("column1").unwrap(), "text");
        assert_eq!(result.get("column2").unwrap(), "text");
    }

    #[tokio::test]
    async fn test_get_primary_key() {
        let mut postgres_operator = MockPostgresOperator::new();
        postgres_operator
            .expect_get_primary_key()
            .times(1)
            .with(eq("table"), eq("schema"))
            .returning(|_, _| Ok(vec!["primary_key".to_string()]));

        let result = postgres_operator
            .get_primary_key("table", "schema")
            .await
            .unwrap();
        assert_eq!(result, vec!["primary_key"]);
    }

    #[tokio::test]
    async fn test_create_table() {
        let mut postgres_operator = MockPostgresOperator::new();
        postgres_operator
            .expect_create_table()
            .times(1)
            .returning(|_, _, _, _| Ok(()));

        let mut column_data_types = IndexMap::new();
        column_data_types.insert("column1".to_string(), "text".to_string());
        column_data_types.insert("column2".to_string(), "text".to_string());

        postgres_operator
            .create_table(
                &column_data_types,
                vec!["primary_key".to_string()],
                "schema",
                "table",
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_insert_dataframe_in_target_db() {
        let mut postgres_operator = MockPostgresOperator::new();
        postgres_operator
            .expect_insert_dataframe_in_target_db()
            .times(1)
            .returning(|_, _, _, _| Ok(()));

        let df = DataFrame::new(vec![Series::new("column1", &[1, 2, 3])]).unwrap();
        postgres_operator
            .insert_dataframe_in_target_db(df, "database", "schema", "table")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_upsert_dataframe_in_target_db() {
        let mut postgres_operator = MockPostgresOperator::new();
        postgres_operator
            .expect_upsert_dataframe_in_target_db()
            .times(1)
            .returning(|_, _, _, _, _| Ok(()));

        let df = DataFrame::new(vec![Series::new("column1", &[1, 2, 3])]).unwrap();
        postgres_operator
            .upsert_dataframe_in_target_db(
                df,
                "database",
                "schema",
                "table",
                &vec!["primary_key".to_string()].as_slice().join(","),
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_drop_dms_columns() {
        let mut postgres_operator = MockPostgresOperator::new();
        postgres_operator
            .expect_drop_dms_columns()
            .times(1)
            .with(eq("schema"), eq("table"))
            .returning(|_, _| Ok(()));

        postgres_operator
            .drop_dms_columns("schema", "table")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_close_connection_pool() {
        let mut postgres_operator = MockPostgresOperator::new();
        postgres_operator
            .expect_close_connection_pool()
            .times(1)
            .return_const(());

        postgres_operator.close_connection_pool().await;
    }
}

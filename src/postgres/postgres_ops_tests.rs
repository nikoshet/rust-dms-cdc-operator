#[cfg(test)]
mod tests {
    use crate::postgres::postgres_ops::MockPostgresOperator;
    use crate::postgres::postgres_ops::PostgresOperator;
    use indexmap::IndexMap;
    use mockall::predicate::*;
    use polars::prelude::*;

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
    async fn test_insert_dataframe_in_local_db() {
        let mut postgres_operator = MockPostgresOperator::new();
        postgres_operator
            .expect_insert_dataframe_in_local_db()
            .times(1)
            .returning(|_, _, _| Ok(()));

        let df = DataFrame::new(vec![Series::new("column1", &[1, 2, 3])]).unwrap();
        postgres_operator
            .insert_dataframe_in_local_db(df, "schema", "table")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_upsert_dataframe_in_local_db() {
        let mut postgres_operator = MockPostgresOperator::new();
        postgres_operator
            .expect_upsert_dataframe_in_local_db()
            .times(1)
            .returning(|_, _, _, _| Ok(()));

        let df = DataFrame::new(vec![Series::new("column1", &[1, 2, 3])]).unwrap();
        postgres_operator
            .upsert_dataframe_in_local_db(
                df,
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

    #[test]
    fn test_process_string_value_with_json() {
        let input =
            "{'bankAccount':{'Key1':'Val1', 'Key2':'Val2'}, 'city':'city1', 'postCode':'111111'}";
        let expected_output =
            "{'bankAccount':{'Key1':'Val1','Key2':'Val2'},'city':'city1','postCode':'111111'}";

        let mut postgres_operator = MockPostgresOperator::new();
        postgres_operator
            .expect_process_string_value()
            .times(1)
            .with(eq(input))
            .returning(|_| expected_output.to_string());

        let result = postgres_operator.process_string_value(input);
        assert_eq!(result, expected_output);
    }

    #[test]
    fn test_process_string_value_without_json() {
        let input = "Some string without JSON";
        let expected_output = "'Some string without JSON'";

        let mut postgres_operator = MockPostgresOperator::new();
        postgres_operator
            .expect_process_string_value()
            .times(1)
            .with(eq(input))
            .returning(|_| expected_output.to_string());

        let result = postgres_operator.process_string_value(input);
        assert_eq!(result, expected_output);
    }

    #[test]
    fn test_process_decimal_value() {
        let integer: i128 = 24000;
        let precision: usize = 2;

        let expected_output = "240.00";

        let mut postgres_operator = MockPostgresOperator::new();
        postgres_operator
            .expect_process_decimal_value()
            .times(1)
            .with(eq(integer), eq(precision))
            .returning(|_, _| expected_output.to_string());

        let result = postgres_operator.process_decimal_value(integer, precision);
        assert_eq!(result, expected_output);
    }
}

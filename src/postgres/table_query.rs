use indexmap::IndexMap;
use std::fmt::Display;

pub enum TableQuery {
    FindAllColumns(String, String),
    DeleteRows(String, String, String),
    FindPrimaryKey(String),
    CreateTable(String, String, IndexMap<String, String>, String),
    DropDmsColumns(String, String),
}

impl Display for TableQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TableQuery::FindAllColumns(schema, table) => {
                write!(
                    f,
                    "SELECT column_name , data_type
                    FROM information_schema.columns 
                    WHERE table_schema = '{}' 
                    AND table_name = '{}'",
                    schema, table
                )
            }
            TableQuery::DeleteRows(table, primary_key, primary_key_value) => {
                write!(
                    f,
                    // language=postgresql
                    r#"
                    DELETE FROM {}
                    WHERE {}={}
                    "#,
                    table, primary_key, primary_key_value
                )
            }
            TableQuery::FindPrimaryKey(table) => {
                write!(
                    f,
                    // language=postgresql
                    r#"
                    SELECT a.attname
                    FROM   pg_index i
                    JOIN   pg_attribute a ON a.attrelid = i.indrelid
                    AND a.attnum = ANY(i.indkey)
                    WHERE  i.indrelid = '{}'::regclass
                    AND    i.indisprimary"#,
                    table
                )
            }
            TableQuery::CreateTable(schema, table, column_data_types, primary_key) => {
                let mut query = format!("CREATE TABLE IF NOT EXISTS {}.{} (", schema, table);

                // Add columns added on S3
                query.push_str(&format!("{} {},", "Op", "varchar"));
                query.push_str(&format!("{} {},", "_dms_ingestion_timestamp", "varchar"));

                for (column, data_type) in column_data_types {
                    query.push_str(&format!("{} {},", column, data_type));
                }
                query.push_str(&format!("PRIMARY KEY ({})", primary_key));
                query.push(')');

                write!(f, "{}", query)
            }
            TableQuery::DropDmsColumns(schema, table) => {
                write!(
                    f,
                    // language=postgresql
                    r#"
                    ALTER TABLE {}.{}
                    DROP COLUMN IF EXISTS Op,
                    DROP COLUMN IF EXISTS _dms_ingestion_timestamp
                    "#,
                    schema, table
                )
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_display_find_all_columns() {
        let query = TableQuery::FindAllColumns("schema".to_string(), "table".to_string());
        assert_eq!(
            query.to_string(),
            "SELECT column_name , data_type
                    FROM information_schema.columns 
                    WHERE table_schema = 'schema' 
                    AND table_name = 'table'"
        );
    }

    #[test]
    fn test_display_delete_rows() {
        let query = TableQuery::DeleteRows(
            "table".to_string(),
            "primary_key".to_string(),
            "1".to_string(),
        );
        assert_eq!(
            query.to_string(),
            r#"
                    DELETE FROM table
                    WHERE primary_key=1
                    "#
        );
    }

    #[test]
    fn test_display_find_primary_key() {
        let query = TableQuery::FindPrimaryKey("table".to_string());
        assert_eq!(
            query.to_string(),
            r#"
                    SELECT a.attname
                    FROM   pg_index i
                    JOIN   pg_attribute a ON a.attrelid = i.indrelid
                    AND a.attnum = ANY(i.indkey)
                    WHERE  i.indrelid = 'table'::regclass
                    AND    i.indisprimary"#
        );
    }

    #[test]
    fn test_display_create_table() {
        let mut column_data_types = IndexMap::new();
        column_data_types.insert("column1".to_string(), "varchar".to_string());
        column_data_types.insert("column2".to_string(), "int".to_string());
        let query = TableQuery::CreateTable(
            "schema".to_string(),
            "table".to_string(),
            column_data_types,
            "primary_key".to_string(),
        );
        assert_eq!(
            query.to_string(),
            "CREATE TABLE IF NOT EXISTS schema.table (Op varchar,_dms_ingestion_timestamp varchar,column1 varchar,column2 int,PRIMARY KEY (primary_key))"
        );
    }

    #[test]
    fn test_display_drop_dms_columns() {
        let query = TableQuery::DropDmsColumns("schema".to_string(), "table".to_string());
        assert_eq!(
            query.to_string(),
            r#"
                    ALTER TABLE schema.table
                    DROP COLUMN IF EXISTS Op,
                    DROP COLUMN IF EXISTS _dms_ingestion_timestamp
                    "#
        );
    }
}

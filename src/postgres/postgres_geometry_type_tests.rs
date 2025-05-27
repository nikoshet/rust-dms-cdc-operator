#[cfg(test)]
mod tests {
    use crate::postgres::postgres_geometry_type::PostgresGeometryType;

    #[test]
    fn test_is_geometry_type() {
        let geometry_type = PostgresGeometryType::new("MULTIPOLYGON(");
        assert!(geometry_type.is_geometry_type());
    }

    #[test]
    fn test_is_not_geometry_type() {
        let geometry_type = PostgresGeometryType::new("POINT(");
        assert!(!geometry_type.is_geometry_type());
    }
}

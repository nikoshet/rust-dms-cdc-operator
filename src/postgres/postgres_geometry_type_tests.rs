#[cfg(test)]
mod tests {
    use crate::postgres::postgres_geometry_type::PostgresGeometryType;

    #[test]
    fn test_is_geometry_type() {
        let geometry_type = PostgresGeometryType::new("MULTIPOLYGON(", 0);
        assert!(geometry_type.is_geometry_type());
    }

    #[test]
    fn test_is_not_geometry_type() {
        let geometry_type = PostgresGeometryType::new("POINT(", 0);
        assert!(!geometry_type.is_geometry_type());
    }
}

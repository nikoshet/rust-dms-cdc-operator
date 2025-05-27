pub struct PostgresGeometryType {
    value_type: String,
}

impl PostgresGeometryType {
    const MULTIPOLYGON: &str = "MULTIPOLYGON";
    const GEOMETRY_TYPE_FROM_TEXT_PREFIX: &str = "ST_GeomFromText";
    const ACCEPTED_GEOMETRY_KEYWORDS: &[&str] = &[Self::MULTIPOLYGON];
    const NUM_OF_CHARS_FOR_GEOMETRY_TYPE_CHECK: usize = 30;

    pub fn new(input: &str) -> Self {
        let value_type = input
            .chars()
            .take(Self::NUM_OF_CHARS_FOR_GEOMETRY_TYPE_CHECK)
            .collect::<String>();

        let sanitized_value_type = if value_type.starts_with('"') && value_type.ends_with('"') {
            value_type
                .strip_prefix('"')
                .and_then(|s| s.strip_suffix('"'))
                .unwrap_or(&value_type)
                .to_string()
        } else {
            value_type.to_string()
        };

        PostgresGeometryType {
            value_type: sanitized_value_type,
        }
    }

    pub fn is_geometry_type(&self) -> bool {
        self.value_type
            .find('(')
            .map(|pos| &self.value_type[..pos])
            .is_some_and(|prefix| Self::ACCEPTED_GEOMETRY_KEYWORDS.contains(&prefix))
    }

    pub fn format_value(&self, value: &str) -> String {
        let value_type =
            &self.value_type[..self.value_type.find('(').unwrap_or(self.value_type.len())];

        match value_type {
            Self::MULTIPOLYGON => format!(
                "{}('{}', 4326)",
                Self::GEOMETRY_TYPE_FROM_TEXT_PREFIX,
                value
            ),
            _ => self.value_type.clone(),
        }
    }
}

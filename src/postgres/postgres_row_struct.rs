use polars::datatypes::AnyValue;
use polars_core::export::num::ToPrimitive;
use rust_decimal::Decimal;

#[allow(clippy::enum_variant_names)]
pub enum RowStruct<'a> {
    FromString(String),
    FromDecimal(i128, usize),
    FromFloat(f64),
    FromDate(&'a AnyValue<'a>),
    FromOther(&'a AnyValue<'a>),
}

impl<'a> RowStruct<'a> {
    pub fn new(value: &'a AnyValue<'a>) -> Self {
        match value {
            AnyValue::String(v) => RowStruct::FromString(v.to_string()),
            AnyValue::Decimal(integer, precision) => RowStruct::FromDecimal(*integer, *precision),
            AnyValue::Datetime(_, _, _) | AnyValue::Date(_) => RowStruct::FromDate(value),
            AnyValue::Float64(v) => RowStruct::FromFloat(*v),
            _ => RowStruct::FromOther(value),
        }
    }

    pub fn displayed(&self) -> String {
        match self {
            RowStruct::FromString(v) => Self::process_string_value(v),
            RowStruct::FromDecimal(integer, precision) => {
                Self::process_decimal_value(*integer, *precision)
            }
            RowStruct::FromDate(v) => format!("'{}'", v),
            RowStruct::FromOther(v) => format!("{}", v),
            RowStruct::FromFloat(v) => format!("{}", v),
        }
    }

    fn process_string_value(value: &str) -> String {
        format!("'{}'", value.replace('\'', "''"))
    }

    fn process_decimal_value(integer: i128, precision: usize) -> String {
        let decimal = Decimal::new(
            integer.to_i64().expect("Could not convert value to i64"),
            precision.to_u32().expect("Could not convert value to u32"),
        );
        format!("'{:}'", decimal)
    }
}

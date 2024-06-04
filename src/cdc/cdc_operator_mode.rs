use clap::ValueEnum;
use std::fmt::{self, Display, Formatter};

/// Represents the mode of the CDC Operator.
///
/// The mode can be one of the following:
///
/// * DateAware - The CDC Operator will be date aware with a specific date range of CDC files.
/// * AbsolutePath - The CDC Operator will use an absolute path.
/// * FullLoadOnly - The CDC Operator will only do a full load.
#[derive(ValueEnum, Clone, Debug, Copy, PartialEq, Eq)]
pub enum ModeValueEnum {
    DateAware,
    AbsolutePath,
    FullLoadOnly,
}

impl Display for ModeValueEnum {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            ModeValueEnum::DateAware => write!(f, "DateAware"),
            ModeValueEnum::AbsolutePath => write!(f, "AbsolutePath"),
            ModeValueEnum::FullLoadOnly => write!(f, "FullLoadOnly"),
        }
    }
}

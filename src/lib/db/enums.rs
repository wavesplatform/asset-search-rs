use std::fmt::Display;

use diesel_derive_enum::DbEnum;

#[derive(DbEnum, Clone, Debug)]
pub enum DataEntryValueType {
    Bin,
    Bool,
    Int,
    Str,
}

#[derive(DbEnum, Clone, Debug, PartialEq)]
pub enum VerificationStatusValueType {
    Declined,
    Unknown,
    Verified,
}

impl Display for VerificationStatusValueType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Declined => f.write_str("'declined'::verification_status_value_type"),
            Self::Unknown => f.write_str("'unknown'::verification_status_value_type"),
            Self::Verified => f.write_str("'verified'::verification_status_value_type"),
        }
    }
}

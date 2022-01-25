use std::fmt::Display;

use diesel_derive_enum::DbEnum;

#[derive(DbEnum, Clone, Debug)]
pub enum AssetWxLabelValueType {
    #[db_rename = "defi"]
    DeFi,
    Gateway,
    Stablecoin,
    Qualified,
    WaVerified,
    CommunityVerified,
}

impl Display for AssetWxLabelValueType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DeFi => f.write_str("'defi'::asset_wx_label_value_type"),
            Self::Gateway => f.write_str("'gateway'::asset_wx_label_value_type"),
            Self::Stablecoin => f.write_str("'stablecoin'::asset_wx_label_value_type"),
            Self::Qualified => f.write_str("'qualified'::asset_wx_label_value_type"),
            Self::WaVerified => f.write_str("'wa_verified'::asset_wx_label_value_type"),
            Self::CommunityVerified => {
                f.write_str("'community_verified'::asset_wx_label_value_type")
            }
        }
    }
}

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

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use std::collections::HashMap;
use std::convert::TryFrom;
use std::fmt::Display;

use crate::db::enums::{DataEntryValueType, VerificationStatusValueType};
use crate::error::Error as AppError;
use crate::waves::{WAVES_ID, WAVES_NAME, WAVES_PRECISION};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AssetInfo {
    pub asset: Asset,
    pub metadata: AssetMetadata,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Asset {
    pub id: String,
    pub name: String,
    pub precision: i32,
    pub description: String,
    pub height: i32,
    pub timestamp: DateTime<Utc>,
    pub issuer: String,
    pub quantity: i64,
    pub reissuable: bool,
    pub min_sponsored_fee: Option<i64>,
    pub smart: bool,
    pub nft: bool,
    pub ticker: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AssetMetadata {
    pub verification_status: VerificationStatus,
    pub labels: Vec<String>,
    pub sponsor_balance: Option<AssetSponsorBalance>,
    pub oracles_data: HashMap<String, Vec<AssetOracleDataEntry>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AssetSponsorBalance {
    pub regular_balance: i64,
    pub out_leasing: Option<i64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AssetOracleDataEntry {
    pub asset_id: String,
    pub oracle_address: String,
    pub key: String,
    pub data_type: DataEntryType,
    pub bin_val: Option<Vec<u8>>,
    pub bool_val: Option<bool>,
    pub int_val: Option<i64>,
    pub str_val: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum DataEntryType {
    Bin,
    Bool,
    Int,
    Str,
}

impl From<&DataEntryValueType> for DataEntryType {
    fn from(v: &DataEntryValueType) -> Self {
        match v {
            DataEntryValueType::Bin => Self::Bin,
            DataEntryValueType::Bool => Self::Bool,
            DataEntryValueType::Int => Self::Int,
            DataEntryValueType::Str => Self::Str,
        }
    }
}

#[derive(Clone, Debug, Serialize_repr, Deserialize_repr)]
#[repr(i8)]
pub enum VerificationStatus {
    Verified = 1,
    Unknown = 0,
    Declined = -1,
}

impl From<&VerificationStatusValueType> for VerificationStatus {
    fn from(v: &VerificationStatusValueType) -> Self {
        match v {
            VerificationStatusValueType::Declined => Self::Declined,
            VerificationStatusValueType::Unknown => Self::Unknown,
            VerificationStatusValueType::Verified => Self::Verified,
        }
    }
}

impl From<&VerificationStatus> for VerificationStatusValueType {
    fn from(v: &VerificationStatus) -> Self {
        match v {
            VerificationStatus::Declined => VerificationStatusValueType::Declined,
            VerificationStatus::Unknown => VerificationStatusValueType::Unknown,
            VerificationStatus::Verified => VerificationStatusValueType::Verified,
        }
    }
}

impl TryFrom<i32> for VerificationStatus {
    type Error = AppError;

    fn try_from(v: i32) -> Result<Self, Self::Error> {
        match v {
            1 => Ok(Self::Verified),
            0 => Ok(Self::Unknown),
            -1 => Ok(Self::Declined),
            _ => Err(AppError::InvalidVariant(format!(
                "Unexpected Verification Status: {}",
                v
            ))),
        }
    }
}

impl TryFrom<String> for VerificationStatus {
    type Error = AppError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value
            .parse::<i32>()
            .map_err(|e| {
                AppError::ValidationError(
                    format!(
                        "Error occurred while parsing VerificationStatus {}: {}",
                        value, e
                    ),
                    Some(
                        vec![
                            ("reason".to_owned(), e.to_string()),
                            ("got".to_owned(), value),
                        ]
                        .into_iter()
                        .collect::<HashMap<String, String>>(),
                    ),
                )
            })
            .and_then(|i| VerificationStatus::try_from(i))
    }
}

impl Display for VerificationStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            VerificationStatus::Verified => f.write_str("1"),
            VerificationStatus::Unknown => f.write_str("0"),
            VerificationStatus::Declined => f.write_str("-1"),
        }
    }
}

impl Default for VerificationStatus {
    fn default() -> Self {
        Self::Unknown
    }
}

#[derive(Clone, Debug)]
pub enum AssetInfoUpdate {
    Base(BaseAssetInfoUpdate),
    SponsorRegularBalance(i64),
    SponsorOutLeasing(i64),
    OraclesData(HashMap<String, Vec<AssetOracleDataEntry>>),
    Labels(Vec<String>),
}

#[derive(Clone, Debug)]
pub struct BaseAssetInfoUpdate {
    pub id: String,
    pub issuer: String,
    pub precision: i32,
    pub nft: bool,
    pub updated_at: DateTime<Utc>,
    pub update_height: i32,
    pub name: String,
    pub description: String,
    pub smart: bool,
    pub quantity: i64,
    pub reissuable: bool,
    pub min_sponsored_fee: Option<i64>,
}

impl BaseAssetInfoUpdate {
    pub fn waves_update(height: i32, time_stamp: DateTime<Utc>, quantity: i64) -> Self {
        Self {
            id: WAVES_ID.to_owned(),
            issuer: "".to_owned(),
            precision: WAVES_PRECISION.to_owned(),
            nft: false,
            updated_at: time_stamp,
            update_height: height,
            name: WAVES_NAME.to_owned(),
            description: "".to_owned(),
            smart: false,
            quantity,
            reissuable: false,
            min_sponsored_fee: None,
        }
    }
}

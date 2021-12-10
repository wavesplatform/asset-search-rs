use chrono::{DateTime, Utc};
use diesel::expression::NonAggregate;
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use std::collections::HashMap;
use std::convert::TryFrom;
use std::fmt::Display;

use crate::db::enums::{AssetWxLabelValueType, DataEntryValueType, VerificationStatusValueType};
use crate::error::Error as AppError;

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
    pub ticker: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AssetMetadata {
    pub verification_status: VerificationStatus,
    pub labels: Vec<AssetLabel>,
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

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "UPPERCASE")]
pub enum AssetLabel {
    Gateway,
    DeFi,
    Stablecoin,
    Qualified,
    WaVerified,
    CommunityVerified,
    #[serde(rename = "null")]
    WithoutLabels,
}

impl From<&AssetWxLabelValueType> for AssetLabel {
    fn from(v: &AssetWxLabelValueType) -> Self {
        match v {
            AssetWxLabelValueType::DeFi => Self::DeFi,
            AssetWxLabelValueType::Gateway => Self::Gateway,
            AssetWxLabelValueType::Stablecoin => Self::Stablecoin,
            AssetWxLabelValueType::Qualified => Self::Qualified,
            AssetWxLabelValueType::WaVerified => Self::WaVerified,
            AssetWxLabelValueType::CommunityVerified => Self::CommunityVerified,
        }
    }
}

impl TryFrom<&AssetLabel> for AssetWxLabelValueType {
    type Error = AppError;

    fn try_from(v: &AssetLabel) -> Result<Self, Self::Error> {
        match v {
            AssetLabel::DeFi => Ok(Self::DeFi),
            AssetLabel::Gateway => Ok(Self::Gateway),
            AssetLabel::Stablecoin => Ok(Self::Stablecoin),
            AssetLabel::Qualified => Ok(Self::Qualified),
            AssetLabel::WaVerified => Ok(Self::WaVerified),
            AssetLabel::CommunityVerified => Ok(Self::CommunityVerified),
            _ => Err(AppError::ValidationError(
                format!(
                    "Error occurred while parsing AssetWxLabelValueType from AssetLabel {}",
                    v
                ),
                None,
            )),
        }
    }
}

impl TryFrom<&str> for AssetLabel {
    type Error = AppError;

    fn try_from(v: &str) -> Result<Self, Self::Error> {
        match v {
            "GATEWAY" => Ok(Self::Gateway),
            "DEFI" => Ok(Self::DeFi),
            "STABLECOIN" => Ok(Self::Stablecoin),
            "QUALIFIED" => Ok(Self::Qualified),
            "WA_VERIFIED" => Ok(Self::WaVerified),
            "COMMUNITY_VERIFIED" => Ok(Self::CommunityVerified),
            "null" => Ok(Self::WithoutLabels),
            _ => Err(AppError::InvalidVariant(format!(
                "Error occurred while parsing AssetLabel from {}",
                v
            ))),
        }
    }
}

impl Display for AssetLabel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AssetLabel::Gateway => f.write_str("GATEWAY"),
            AssetLabel::DeFi => f.write_str("DEFI"),
            AssetLabel::Stablecoin => f.write_str("STABLECOIN"),
            AssetLabel::Qualified => f.write_str("QUALIFIED"),
            AssetLabel::WaVerified => f.write_str("WA_VERIFIED"),
            AssetLabel::CommunityVerified => f.write_str("COMMUNITY_VERIFIED"),
            &AssetLabel::WithoutLabels => Ok(()),
        }
    }
}

impl NonAggregate for AssetLabel {}

#[derive(Clone, Debug)]
pub struct AssetInfoUpdate {
    // Asset
    pub id: String,
    // updatable fields
    pub updated_at: DateTime<Utc>,
    pub update_height: i32,
    // mutable fields
    pub name: Option<String>,
    pub description: Option<String>,
    pub smart: Option<bool>,
    pub quantity: Option<i64>,
    pub reissuable: Option<bool>,
    pub min_sponsored_fee: Option<i64>,
    // AssetMetadata mutable fields
    pub sponsor_regular_balance: Option<i64>,
    pub sponsor_out_leasing: Option<i64>,
    // Address -> Vec<DataEntry>
    pub oracles_data: Option<HashMap<String, Vec<AssetOracleDataEntry>>>,
}

impl Default for AssetInfoUpdate {
    fn default() -> Self {
        AssetInfoUpdate {
            id: String::default(),
            updated_at: Utc::now(),
            update_height: 0,
            // mutable fields
            name: None,
            description: None,
            smart: None,
            quantity: None,
            reissuable: None,
            min_sponsored_fee: None,
            // AssetMetadata mutable fields
            sponsor_regular_balance: None,
            sponsor_out_leasing: None,
            oracles_data: None,
        }
    }
}

/// Produces AssetInfo applying specified AssetInfoUpdate to specified AssetInfo
impl From<(&AssetInfo, &AssetInfoUpdate)> for AssetInfo {
    fn from((current_asset_info, update): (&AssetInfo, &AssetInfoUpdate)) -> Self {
        let sponsor_balance = if current_asset_info.asset.min_sponsored_fee.is_some()
            || update.min_sponsored_fee.is_some()
        {
            current_asset_info
                .metadata
                .sponsor_balance
                .as_ref()
                .map(|sponsor_balance| AssetSponsorBalance {
                    regular_balance: update
                        .sponsor_regular_balance
                        .unwrap_or(sponsor_balance.regular_balance),
                    out_leasing: update.sponsor_out_leasing.or(sponsor_balance.out_leasing),
                })
                .or(Some(AssetSponsorBalance {
                    regular_balance: update.sponsor_regular_balance.expect(&format!(
                        "Expected asset {} sponsor {} regular balance",
                        update.id, current_asset_info.asset.issuer
                    )),
                    out_leasing: update.sponsor_out_leasing,
                }))
        } else {
            None
        };

        Self {
            asset: Asset {
                id: current_asset_info.asset.id.clone(),
                name: update
                    .name
                    .clone()
                    .unwrap_or(current_asset_info.asset.name.clone()),
                precision: current_asset_info.asset.precision,
                description: update
                    .description
                    .clone()
                    .unwrap_or(current_asset_info.asset.description.clone()),
                height: update.update_height,
                timestamp: update.updated_at,
                issuer: current_asset_info.asset.issuer.clone(),
                quantity: update.quantity.unwrap_or(current_asset_info.asset.quantity),
                reissuable: update
                    .reissuable
                    .unwrap_or(current_asset_info.asset.reissuable),
                min_sponsored_fee: update.min_sponsored_fee,
                smart: update.smart.unwrap_or(current_asset_info.asset.smart),
                ticker: current_asset_info.asset.ticker.clone(),
            },
            metadata: AssetMetadata {
                verification_status: current_asset_info.metadata.verification_status.clone(),
                labels: current_asset_info.metadata.labels.clone(),
                oracles_data: update
                    .oracles_data
                    .clone()
                    .unwrap_or(current_asset_info.metadata.oracles_data.clone()),
                sponsor_balance,
            },
        }
    }
}

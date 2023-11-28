use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::cache::AssetBlockchainData;
use crate::db::enums::DataEntryValueType;
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
    pub ext_ticker: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AssetMetadata {
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

#[derive(Clone, Debug)]
pub enum AssetInfoUpdate {
    Base(BaseAssetInfoUpdate),
    SponsorRegularBalance(i64),
    SponsorOutLeasing(i64),
    OraclesData(HashMap<String, Vec<AssetOracleDataEntry>>),
    Labels(Vec<String>),
    Ticker(String),
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

impl From<AssetBlockchainData> for BaseAssetInfoUpdate {
    fn from(value: AssetBlockchainData) -> Self {
        Self {
            updated_at: Utc::now(),
            update_height: value.height,
            id: value.id.clone(),
            issuer: value.issuer.clone(),
            precision: value.precision,
            nft: value.nft,
            name: value.name.clone(),
            description: value.description.clone(),
            smart: value.smart,
            quantity: value.quantity,
            reissuable: value.reissuable,
            min_sponsored_fee: value.min_sponsored_fee,
        }
    }
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

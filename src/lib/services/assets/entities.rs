use chrono::{DateTime, Utc};
use diesel::{
    sql_types::{Array, BigInt, Bool, Integer, Nullable, Text, Timestamptz},
    Queryable,
};
use std::collections::HashMap;

use crate::{
    cache::{AssetBlockchainData, AssetUserDefinedData},
    db::enums::DataEntryValueType,
    error::Error as AppError,
    models::{AssetOracleDataEntry, AssetSponsorBalance, DataEntryType},
};

#[derive(Clone, Debug, QueryableByName)]
pub struct Asset {
    #[sql_type = "Text"]
    pub id: String,
    #[sql_type = "Text"]
    pub name: String,
    #[sql_type = "Integer"]
    pub precision: i32,
    #[sql_type = "Text"]
    pub description: String,
    #[sql_type = "Integer"]
    pub height: i32,
    #[sql_type = "Timestamptz"]
    pub timestamp: DateTime<Utc>,
    #[sql_type = "Text"]
    pub issuer: String,
    #[sql_type = "BigInt"]
    pub quantity: i64,
    #[sql_type = "Bool"]
    pub reissuable: bool,
    #[sql_type = "Nullable<BigInt>"]
    pub min_sponsored_fee: Option<i64>,
    #[sql_type = "Bool"]
    pub smart: bool,
    #[sql_type = "Bool"]
    pub nft: bool,
    #[sql_type = "Nullable<BigInt>"]
    pub sponsor_regular_balance: Option<i64>,
    #[sql_type = "Nullable<BigInt>"]
    pub sponsor_out_leasing: Option<i64>,
    #[sql_type = "Nullable<Text>"]
    pub ticker: Option<String>,
}

#[derive(Clone, Debug, Queryable)]
pub struct OracleDataEntry {
    pub asset_id: String,
    pub oracle_address: String,
    pub key: String,
    pub data_type: DataEntryValueType,
    pub bin_val: Option<Vec<u8>>,
    pub bool_val: Option<bool>,
    pub int_val: Option<i64>,
    pub str_val: Option<String>,
}

impl From<&OracleDataEntry> for AssetOracleDataEntry {
    fn from(de: &OracleDataEntry) -> Self {
        Self {
            asset_id: de.asset_id.clone(),
            oracle_address: de.oracle_address.clone(),
            key: de.key.clone(),
            data_type: DataEntryType::from(&de.data_type),
            bin_val: de.bin_val.clone(),
            bool_val: de.bool_val,
            int_val: de.int_val,
            str_val: de.str_val.clone(),
        }
    }
}

impl AssetBlockchainData {
    pub fn try_from_asset_and_oracles_data(
        asset: &Asset,
        oracles_data: &HashMap<String, Vec<OracleDataEntry>>,
    ) -> Result<Self, AppError> {
        let sponsor_balance = if asset.min_sponsored_fee.is_some() {
            Some(AssetSponsorBalance {
                regular_balance: asset.sponsor_regular_balance.ok_or(
                    AppError::ConsistencyError(format!(
                        "Expected asset {} sponsor ({}) regular balance",
                        asset.id, asset.issuer
                    )),
                )?,
                out_leasing: asset.sponsor_out_leasing,
            })
        } else {
            None
        };

        let asset_blockchain_data = Self {
            id: asset.id.clone(),
            name: asset.name.clone(),
            ticker: asset.ticker.clone(),
            precision: asset.precision,
            description: asset.description.clone(),
            height: asset.height,
            timestamp: asset.timestamp,
            issuer: asset.issuer.clone(),
            quantity: asset.quantity,
            reissuable: asset.reissuable,
            min_sponsored_fee: asset.min_sponsored_fee,
            smart: asset.smart,
            nft: asset.nft,
            sponsor_balance,
            oracles_data: oracles_data
                .into_iter()
                .map(|(oracle_address, des)| {
                    let des = des
                        .into_iter()
                        .map(|de| AssetOracleDataEntry::from(de))
                        .collect();
                    (oracle_address.to_owned(), des)
                })
                .collect(),
        };

        Ok(asset_blockchain_data)
    }
}

#[derive(Clone, Debug, QueryableByName)]
pub struct UserDefinedData {
    #[sql_type = "Text"]
    pub asset_id: String,
    #[sql_type = "Nullable<Text>"]
    pub ticker: Option<String>,
    #[sql_type = "Array<Text>"]
    pub labels: Vec<String>,
}

impl From<&UserDefinedData> for AssetUserDefinedData {
    fn from(d: &UserDefinedData) -> Self {
        let labels = d.labels.clone().into_iter().collect::<Vec<_>>();
        Self {
            asset_id: d.asset_id.clone(),
            ticker: d.ticker.clone(),
            labels,
        }
    }
}

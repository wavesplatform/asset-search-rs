use chrono::{DateTime, Utc};
use diesel::sql_types::{BigInt, Bool, Integer, Nullable, Text, Timestamptz};
use std::{
    collections::HashMap,
    hash::{Hash, Hasher},
};

use crate::{
    cache::AssetBlockchainData,
    db::enums::DataEntryValueType,
    models::{
        Asset as AssetModel, AssetInfo, AssetInfoUpdate, AssetMetadata, AssetOracleDataEntry,
        AssetSponsorBalance, DataEntryType, VerificationStatus,
    },
    schema::assets,
    waves::{WAVES_ID, WAVES_NAME, WAVES_PRECISION},
};

#[derive(Clone, Debug, Insertable)]
#[table_name = "assets"]
pub struct InsertableAsset {
    pub uid: i64,
    pub superseded_by: i64,
    pub block_uid: i64,
    pub id: String,
    pub name: String,
    pub description: String,
    pub time_stamp: DateTime<Utc>,
    pub issuer: String,
    pub precision: i32,
    pub smart: bool,
    pub nft: bool,
    pub quantity: i64,
    pub reissuable: bool,
    pub min_sponsored_fee: Option<i64>,
}

impl PartialEq for InsertableAsset {
    fn eq(&self, other: &InsertableAsset) -> bool {
        (&self.id) == (&other.id)
    }
}

impl Eq for InsertableAsset {}

impl Hash for InsertableAsset {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

#[derive(Clone, Debug)]
pub struct AssetOverride {
    pub superseded_by: i64,
    pub id: String,
}

#[derive(Clone, Debug)]
pub struct DeletedAsset {
    pub uid: i64,
    pub id: String,
}

impl PartialEq for DeletedAsset {
    fn eq(&self, other: &Self) -> bool {
        (&self.id) == (&other.id)
    }
}

impl Eq for DeletedAsset {}

impl Hash for DeletedAsset {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

#[derive(Clone, Debug)]
pub struct Asset {
    pub height: i32,
    pub time_stamp: DateTime<Utc>,
    pub id: String,
    pub name: String,
    pub description: String,
    pub issuer: String,
    pub precision: i32,
    pub smart: bool,
    pub nft: bool,
    pub quantity: i64,
    pub reissuable: bool,
    pub min_sponsored_fee: Option<i64>,
}

impl Asset {
    pub fn waves_update(height: i32, time_stamp: DateTime<Utc>, quantity: i64) -> Self {
        Self {
            height,
            time_stamp,
            quantity,
            id: WAVES_ID.to_owned(),
            name: WAVES_NAME.to_owned(),
            description: "".to_owned(),
            precision: WAVES_PRECISION.to_owned(),
            issuer: "".to_owned(),
            smart: false,
            nft: false,
            reissuable: false,
            min_sponsored_fee: None,
        }
    }
}

impl From<(&Asset, &AssetInfoUpdate)> for AssetInfo {
    fn from((asset, update): (&Asset, &AssetInfoUpdate)) -> Self {
        let sponsor_balance =
            if asset.min_sponsored_fee.is_some() || update.min_sponsored_fee.is_some() {
                Some(AssetSponsorBalance {
                    regular_balance: update.sponsor_regular_balance.expect(&format!(
                        "Expected sponsor {} regular balance while producing new asset info for {}",
                        asset.issuer, asset.id
                    )),
                    out_leasing: update.sponsor_out_leasing,
                })
            } else {
                None
            };

        Self {
            asset: AssetModel {
                id: asset.id.clone(),
                name: update.name.clone().unwrap_or(asset.name.clone()),
                precision: asset.precision,
                description: update
                    .description
                    .clone()
                    .unwrap_or(asset.description.clone()),
                height: update.update_height,
                timestamp: update.updated_at,
                issuer: asset.issuer.clone(),
                quantity: update.quantity.unwrap_or(asset.quantity),
                reissuable: update.reissuable.unwrap_or(asset.reissuable),
                min_sponsored_fee: update.min_sponsored_fee,
                smart: update.smart.unwrap_or(asset.smart),
                ticker: None,
            },
            metadata: AssetMetadata {
                verification_status: VerificationStatus::default(),
                labels: vec![],
                oracles_data: update.oracles_data.clone().unwrap_or_default(),
                sponsor_balance,
            },
        }
    }
}

impl From<(&Asset, &AssetInfoUpdate)> for AssetBlockchainData {
    fn from((current, update): (&Asset, &AssetInfoUpdate)) -> Self {
        Self {
            id: current.id.clone(),
            name: update.name.clone().unwrap_or(current.name.clone()),
            precision: current.precision,
            description: update
                .description
                .clone()
                .unwrap_or(current.description.clone()),
            height: update.update_height,
            timestamp: update.updated_at,
            issuer: current.issuer.clone(),
            quantity: update.quantity.unwrap_or(current.quantity),
            reissuable: update.reissuable.unwrap_or(current.reissuable),
            min_sponsored_fee: update.min_sponsored_fee,
            smart: update.smart.unwrap_or(current.smart),
            oracles_data: update.oracles_data.clone().unwrap_or_default(),
            sponsor_balance: if update.min_sponsored_fee.is_some()
                || current.min_sponsored_fee.is_some()
            {
                update
                    .sponsor_regular_balance
                    .map(|srb| AssetSponsorBalance {
                        regular_balance: srb,
                        out_leasing: update.sponsor_out_leasing,
                    })
            } else {
                None
            },
        }
    }
}

#[derive(Clone, Debug, QueryableByName)]
pub struct QueryableAsset {
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
    #[sql_type = "Nullable<BigInt>"]
    pub sponsor_regular_balance: Option<i64>,
    #[sql_type = "Nullable<BigInt>"]
    pub sponsor_out_leasing: Option<i64>,
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

impl From<(&QueryableAsset, &HashMap<String, Vec<AssetOracleDataEntry>>)> for AssetBlockchainData {
    fn from(
        (update, oracles_data): (&QueryableAsset, &HashMap<String, Vec<AssetOracleDataEntry>>),
    ) -> Self {
        Self {
            id: update.id.clone(),
            name: update.name.clone(),
            precision: update.precision,
            description: update.description.clone(),
            height: update.height,
            timestamp: update.timestamp,
            issuer: update.issuer.clone(),
            quantity: update.quantity,
            reissuable: update.reissuable,
            min_sponsored_fee: update.min_sponsored_fee,
            smart: update.smart,
            oracles_data: oracles_data.to_owned(),
            sponsor_balance: if update.min_sponsored_fee.is_some() {
                update
                    .sponsor_regular_balance
                    .map(|srb| AssetSponsorBalance {
                        regular_balance: srb,
                        out_leasing: update.sponsor_out_leasing,
                    })
            } else {
                None
            },
        }
    }
}

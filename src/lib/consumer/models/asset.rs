use chrono::{DateTime, Utc};
use diesel::sql_types::{BigInt, Bool, Integer, Nullable, Text, Timestamptz};
use std::{
    collections::HashMap,
    hash::{Hash, Hasher},
};

use crate::{
    cache::AssetBlockchainData,
    db::enums::DataEntryValueType,
    models::{AssetOracleDataEntry, AssetSponsorBalance, BaseAssetInfoUpdate, DataEntryType},
    schema::assets,
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
    #[sql_type = "Bool"]
    pub nft: bool,
    #[sql_type = "Nullable<BigInt>"]
    pub sponsor_regular_balance: Option<i64>,
    #[sql_type = "Nullable<BigInt>"]
    pub sponsor_out_leasing: Option<i64>,
}

impl From<&QueryableAsset> for BaseAssetInfoUpdate {
    fn from(a: &QueryableAsset) -> Self {
        Self {
            id: a.id.clone(),
            issuer: a.issuer.clone(),
            precision: a.precision,
            update_height: a.height,
            updated_at: a.timestamp.clone(),
            name: a.name.clone(),
            description: a.description.clone(),
            smart: a.smart,
            nft: a.nft,
            quantity: a.quantity,
            reissuable: a.reissuable,
            min_sponsored_fee: a.min_sponsored_fee,
        }
    }
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
    pub fn from_asset_and_oracles_data(
        asset: &QueryableAsset,
        oracles_data: &HashMap<String, Vec<AssetOracleDataEntry>>,
    ) -> Self {
        Self {
            id: asset.id.clone(),
            name: asset.name.clone(),
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
            oracles_data: oracles_data.to_owned(),
            sponsor_balance: if asset.min_sponsored_fee.is_some() {
                asset
                    .sponsor_regular_balance
                    .map(|srb| AssetSponsorBalance {
                        regular_balance: srb,
                        out_leasing: asset.sponsor_out_leasing,
                    })
            } else {
                None
            },
        }
    }
}

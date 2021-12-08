use chrono::{DateTime, Utc};
use std::hash::{Hash, Hasher};

use crate::{
    cache::AssetBlockchainData,
    models::{
        Asset as AssetModel, AssetInfo, AssetInfoUpdate, AssetMetadata, AssetSponsorBalance,
        VerificationStatus,
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

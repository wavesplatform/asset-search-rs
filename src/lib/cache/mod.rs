pub mod redis;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::error::Error as AppError;
use crate::models::{
    Asset, AssetInfo, AssetInfoUpdate, AssetLabel, AssetMetadata, AssetOracleDataEntry,
    AssetSponsorBalance, VerificationStatus,
};

pub const ASSET_KEY_PREFIX: &str = "asset";
pub const ASSET_USER_DEFINED_DATA_KEY_PREFIX: &str = "asset_user_defined_data";

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct AssetBlockchainData {
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
    pub oracles_data: HashMap<String, Vec<AssetOracleDataEntry>>,
    pub sponsor_balance: Option<AssetSponsorBalance>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct AssetUserDefinedData {
    pub asset_id: String,
    pub ticker: Option<String>,
    pub verification_status: VerificationStatus,
    pub labels: Vec<AssetLabel>,
}

impl From<(&AssetBlockchainData, &AssetUserDefinedData)> for AssetInfo {
    fn from(
        (blockchain_data, user_defined_data): (&AssetBlockchainData, &AssetUserDefinedData),
    ) -> Self {
        let sponsor_balance = if blockchain_data.min_sponsored_fee.is_some() {
            blockchain_data.sponsor_balance.clone()
        } else {
            None
        };

        Self {
            asset: Asset {
                ticker: user_defined_data.ticker.clone(),
                id: blockchain_data.id.clone(),
                name: blockchain_data.name.clone(),
                precision: blockchain_data.precision.clone(),
                description: blockchain_data.description.clone(),
                height: blockchain_data.height.clone(),
                timestamp: blockchain_data.timestamp.clone(),
                issuer: blockchain_data.issuer.clone(),
                quantity: blockchain_data.quantity.clone(),
                reissuable: blockchain_data.reissuable.clone(),
                min_sponsored_fee: blockchain_data.min_sponsored_fee.clone(),
                smart: blockchain_data.smart.clone(),
            },
            metadata: AssetMetadata {
                verification_status: user_defined_data.verification_status.clone(),
                labels: user_defined_data.labels.clone(),
                sponsor_balance,
                oracles_data: blockchain_data.oracles_data.clone(),
            },
        }
    }
}

impl From<(&AssetBlockchainData, &AssetInfoUpdate)> for AssetBlockchainData {
    fn from((current, update): (&AssetBlockchainData, &AssetInfoUpdate)) -> Self {
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

pub trait CacheKeyFn {
    fn key_fn(&self, source_key: &str) -> String;
}

pub trait SyncReadCache<T>: CacheKeyFn {
    fn get(&self, key: &str) -> Result<Option<T>, AppError>;

    fn mget(&self, keys: &[&str]) -> Result<Vec<Option<T>>, AppError>;
}

pub trait SyncWriteCache<T>: SyncReadCache<T> {
    fn set(&self, key: &str, value: T) -> Result<(), AppError>;
}

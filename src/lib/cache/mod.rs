pub mod invalidator;
pub mod redis;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::convert::TryFrom;

use crate::error::Error as AppError;
use crate::models::{
    Asset, AssetInfo, AssetInfoUpdate, AssetLabel, AssetMetadata, AssetOracleDataEntry,
    AssetSponsorBalance, VerificationStatus,
};

pub const KEY_SEPARATOR: &str = ":";
pub const ASSET_KEY_PREFIX: &str = "asset";
pub const ASSET_USER_DEFINED_DATA_KEY_PREFIX: &str = "asset_user_defined_data";

#[derive(Clone, Debug, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum InvalidateCacheMode {
    BlockchainData,
    UserDefinedData,
    AllData,
}

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
    pub nft: bool,
    pub oracles_data: HashMap<String, Vec<AssetOracleDataEntry>>,
    pub sponsor_balance: Option<AssetSponsorBalance>,
}

impl From<&crate::models::AssetInfo> for AssetBlockchainData {
    fn from(a: &crate::models::AssetInfo) -> Self {
        Self {
            id: a.asset.id.clone(),
            name: a.asset.name.clone(),
            precision: a.asset.precision,
            description: a.asset.description.clone(),
            height: a.asset.height,
            timestamp: a.asset.timestamp,
            issuer: a.asset.issuer.clone(),
            quantity: a.asset.quantity,
            reissuable: a.asset.reissuable,
            min_sponsored_fee: a.asset.min_sponsored_fee,
            smart: a.asset.smart,
            nft: a.asset.nft,
            oracles_data: a.metadata.oracles_data.clone(),
            sponsor_balance: a.metadata.sponsor_balance.clone(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct AssetUserDefinedData {
    pub asset_id: String,
    pub ticker: Option<String>,
    pub verification_status: VerificationStatus,
    pub labels: Vec<AssetLabel>,
}

impl AssetUserDefinedData {
    pub fn add_label(&self, label: &AssetLabel) -> Self {
        let mut labels = self.labels.iter().fold(HashSet::new(), |mut acc, cur| {
            acc.insert(cur.to_owned());
            acc
        });
        labels.insert(label.to_owned());

        Self {
            asset_id: self.asset_id.clone(),
            ticker: self.ticker.clone(),
            verification_status: self.verification_status.clone(),
            labels: labels.into_iter().collect::<Vec<_>>(),
        }
    }

    pub fn delete_label(&self, label: &AssetLabel) -> Self {
        let labels = self
            .labels
            .iter()
            .filter_map(|l| if l == label { None } else { Some(l.to_owned()) })
            .collect::<Vec<_>>();

        Self {
            asset_id: self.asset_id.clone(),
            ticker: self.ticker.clone(),
            verification_status: self.verification_status.clone(),
            labels,
        }
    }
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
                nft: blockchain_data.nft
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

/// Used by consumer for updating cached data
/// 
/// Generates new AssetBlockchainData via applying sequence of updates on current AssetBlockchainData value
impl From<(&AssetBlockchainData, &Vec<AssetInfoUpdate>)> for AssetBlockchainData {
    fn from((current, updates): (&AssetBlockchainData, &Vec<AssetInfoUpdate>)) -> Self {
        updates
            .iter()
            .fold(current.to_owned(), |mut cur, update| match update {
                AssetInfoUpdate::Base(base_asset_info_update) => {
                    cur.name = base_asset_info_update.name.clone();
                    cur.description = base_asset_info_update.description.clone();
                    cur.quantity = base_asset_info_update.quantity;
                    cur.reissuable = base_asset_info_update.reissuable;
                    cur.min_sponsored_fee = base_asset_info_update
                        .min_sponsored_fee;
                    cur.smart = base_asset_info_update.smart;
                    cur.nft = base_asset_info_update.nft;
                    cur
                }
                AssetInfoUpdate::OraclesData(oracle_data) => {
                    cur.oracles_data = oracle_data.to_owned();
                    cur
                }
                AssetInfoUpdate::SponsorRegularBalance(regular_balance) => {
                    if cur.min_sponsored_fee.is_some() {
                        match cur.sponsor_balance.as_mut() {
                            Some(sponsor_balance) => {
                                sponsor_balance.regular_balance = regular_balance.to_owned();
                            }
                            _ => {
                                cur.sponsor_balance = Some(AssetSponsorBalance {
                                    regular_balance: regular_balance.to_owned(),
                                    out_leasing: None,
                                })
                            }
                        }
                    }
                    cur
                }
                AssetInfoUpdate::SponsorOutLeasing(out_leasing) => {
                    if cur.min_sponsored_fee.is_some() {
                        match cur.sponsor_balance.as_mut() {
                            Some(sponsor_balance) => {
                                sponsor_balance.out_leasing = Some(out_leasing.to_owned());
                            }
                            _ => {
                                unreachable!(
                                    "Expected asset {} issuer ({}) sponsor balance for updating out leasing",
                                    current.id, current.issuer
                                );
                            }
                        }
                    }
                    cur
                }
            })
    }
}

/// Used by consumer for caching initial asset blockchain data
/// 
/// Generates new AssetBlockchainData from sequence of asset info updates
/// Requires BaseAssetInfoUpdate to be the first one
impl TryFrom<&Vec<AssetInfoUpdate>> for AssetBlockchainData {
    type Error = AppError;

    fn try_from(updates: &Vec<AssetInfoUpdate>) -> Result<Self, Self::Error> {
        let mut updates_it = updates.iter();
        let base = match updates_it.next() {
            Some(AssetInfoUpdate::Base(base)) => Ok(base),
            _ => Err(AppError::IncosistDataError("Expected BaseAssetInfoUpdate as 1st update for transforming Vec<AssetInfoUpdate> into AssetBlockchainData".to_owned())),
        }?;

        let initial = Self {
            id: base.id.to_owned(),
            issuer: base.issuer.to_owned(),
            precision: base.precision,
            height: base.update_height,
            timestamp: base.updated_at,
            name: base.name.to_owned(),
            description: base.description.to_owned(),
            quantity: base.quantity,
            reissuable: base.reissuable,
            min_sponsored_fee: base.min_sponsored_fee,
            smart: base.smart,
            nft: base.nft,
            oracles_data: HashMap::new(),
            sponsor_balance: None,
        };

        let remaining_updates = updates_it.cloned().collect::<Vec<_>>();

        // apply remaining updates
        let asset_blockchain_data = Self::from((&initial, &remaining_updates));

        Ok(asset_blockchain_data)
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

    fn clear(&self) -> Result<(), AppError>;
}

#[cfg(test)]
mod tests {
    use super::AssetUserDefinedData;
    use crate::models::{AssetLabel, VerificationStatus};

    #[test]
    fn should_add_label() {
        let udd = AssetUserDefinedData {
            asset_id: "asset_id".to_owned(),
            ticker: None,
            verification_status: VerificationStatus::Unknown,
            labels: vec![],
        };

        let udd_with_new_label = udd.add_label(&AssetLabel::WaVerified);
        assert_eq!(udd_with_new_label.labels, vec![AssetLabel::WaVerified]);
    }

    #[test]
    fn should_delete_label() {
        let udd = AssetUserDefinedData {
            asset_id: "asset_id".to_owned(),
            ticker: None,
            verification_status: VerificationStatus::Unknown,
            labels: vec![AssetLabel::WaVerified],
        };

        let udd_with_new_label = udd.delete_label(&AssetLabel::WaVerified);
        assert_eq!(udd_with_new_label.labels, vec![]);
    }
}

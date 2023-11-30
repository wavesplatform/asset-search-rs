use crate::models::{Asset, AssetInfo, AssetMetadata, AssetOracleDataEntry, AssetSponsorBalance};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

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
    pub ticker: Option<String>,
    pub ext_ticker: Option<String>,
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

impl From<&AssetInfo> for AssetBlockchainData {
    fn from(a: &AssetInfo) -> Self {
        Self {
            id: a.asset.id.clone(),
            name: a.asset.name.clone(),
            ticker: a.asset.ticker.clone(),
            ext_ticker: a.asset.ext_ticker.clone(),
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
    pub labels: Vec<String>,
}

impl AssetUserDefinedData {
    pub fn new(asset_id: impl AsRef<str>) -> Self {
        Self {
            asset_id: asset_id.as_ref().to_owned(),
            labels: Vec::<String>::new(),
        }
    }

    pub fn add_label(&self, label: &str) -> Self {
        let mut labels = self.labels.iter().fold(HashSet::new(), |mut acc, cur| {
            acc.insert(cur.to_owned());
            acc
        });
        if !label.is_empty() {
            labels.insert(label.to_owned());
        };
        Self {
            asset_id: self.asset_id.clone(),
            labels: labels.into_iter().collect::<Vec<_>>(),
        }
    }

    pub fn delete_label(&self, label: &str) -> Self {
        let labels = self
            .labels
            .iter()
            .filter_map(|l| if l == label { None } else { Some(l.to_owned()) })
            .collect::<Vec<_>>();

        Self {
            asset_id: self.asset_id.clone(),
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
                ticker: blockchain_data.ticker.clone(),
                ext_ticker: blockchain_data.ext_ticker.clone(),
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
                nft: blockchain_data.nft,
            },
            metadata: AssetMetadata {
                labels: user_defined_data.labels.clone(),
                sponsor_balance,
                oracles_data: blockchain_data.oracles_data.clone(),
            },
        }
    }
}

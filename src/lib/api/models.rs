use chrono::{DateTime, Utc};
use itertools::Itertools;
use serde::Serialize;
use std::collections::HashMap;

use crate::consumer::models::data_entry::DataEntryValue;
use crate::models::DataEntryType;
use crate::waves::{parse_waves_association_key, KNOWN_WAVES_ASSOCIATION_ASSET_ATTRIBUTES};

use super::dtos::ResponseFormat;

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename = "list")]
pub struct List<T> {
    pub data: Vec<T>,
    pub cursor: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "type", rename = "asset")]
pub struct Asset {
    pub data: Option<AssetInfo>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<AssetMetadata>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(untagged)]
pub enum AssetInfo {
    Full(FullAssetInfo),
    Brief(BriefAssetInfo),
}

#[derive(Clone, Debug, Serialize)]
pub struct FullAssetInfo {
    pub ticker: Option<String>,
    pub ext_ticker: Option<String>,
    pub id: String,
    pub name: String,
    pub precision: i32,
    pub description: String,
    pub height: i32,
    pub timestamp: DateTime<Utc>,
    pub sender: String,
    pub quantity: i64,
    pub reissuable: bool,
    pub has_script: bool,
    pub min_sponsored_fee: Option<i64>,
    pub smart: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct BriefAssetInfo {
    pub ticker: Option<String>,
    pub id: String,
    pub name: String,
    pub smart: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct AssetMetadata {
    pub oracle_data: Vec<OracleData>,
    pub labels: Vec<String>,
    pub sponsor_balance: Option<i64>,
    pub has_image: bool,
}

#[derive(Clone, Debug)]
pub struct AssetLabel {
    pub asset_id: String,
    pub label: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct OracleData(HashMap<String, DataEntryValue>);

impl Asset {
    pub fn new(
        asset_info: Option<crate::models::AssetInfo>,
        has_image: bool,
        include_metadata: bool,
        format: &ResponseFormat,
    ) -> Self {
        match asset_info {
            Some(asset_info) => {
                let ai = match format {
                    ResponseFormat::Full => AssetInfo::Full(FullAssetInfo {
                        id: asset_info.asset.id,
                        name: asset_info.asset.name,
                        description: asset_info.asset.description,
                        precision: asset_info.asset.precision,
                        height: asset_info.asset.height,
                        timestamp: asset_info.asset.timestamp,
                        sender: asset_info.asset.issuer,
                        quantity: asset_info.asset.quantity,
                        reissuable: asset_info.asset.reissuable,
                        has_script: asset_info.asset.smart,
                        smart: asset_info.asset.smart,
                        min_sponsored_fee: asset_info.asset.min_sponsored_fee,
                        ticker: asset_info.asset.ticker,
                        ext_ticker: asset_info.asset.ext_ticker,
                    }),
                    ResponseFormat::Brief => AssetInfo::Brief(BriefAssetInfo {
                        id: asset_info.asset.id,
                        name: asset_info.asset.name,
                        smart: asset_info.asset.smart,
                        ticker: asset_info.asset.ticker,
                    }),
                };
                let metadata = AssetMetadata {
                    has_image,
                    labels: asset_info.metadata.labels,
                    oracle_data: asset_info
                        .metadata
                        .oracles_data
                        .into_iter()
                        .map(|(_oracle_address, oracle_data)| {
                            let oracle_data =
                                oracle_data
                                    .into_iter()
                                    .fold(HashMap::new(), |mut acc, cur| {
                                        // todo: improve performance (based on profiling)
                                        let waves_association_key = parse_waves_association_key(
                                            &KNOWN_WAVES_ASSOCIATION_ASSET_ATTRIBUTES,
                                            &cur.key,
                                        );
                                        let key = waves_association_key
                                            .map(|wak| wak.key_without_asset_id)
                                            .or(Some(cur.key))
                                            .unwrap();
                                        match cur.data_type {
                                            DataEntryType::Bin => {
                                                acc.insert(
                                                    key,
                                                    DataEntryValue::BinVal(cur.bin_val.unwrap()),
                                                );
                                            }
                                            DataEntryType::Bool => {
                                                acc.insert(
                                                    key,
                                                    DataEntryValue::BoolVal(cur.bool_val.unwrap()),
                                                );
                                            }
                                            DataEntryType::Int => {
                                                acc.insert(
                                                    key,
                                                    DataEntryValue::IntVal(cur.int_val.unwrap()),
                                                );
                                            }
                                            DataEntryType::Str => {
                                                acc.insert(
                                                    key,
                                                    DataEntryValue::StrVal(cur.str_val.unwrap()),
                                                );
                                            }
                                        }
                                        acc
                                    });

                            OracleData(oracle_data)
                        })
                        .collect_vec(),
                    sponsor_balance: asset_info.metadata.sponsor_balance.map(|sb| {
                        match sb.out_leasing {
                            Some(out_leasing) => sb.regular_balance - out_leasing,
                            _ => sb.regular_balance,
                        }
                    }),
                };
                Self {
                    data: Some(ai),
                    metadata: if include_metadata {
                        Some(metadata)
                    } else {
                        None
                    },
                }
            }
            _ => Self {
                data: None,
                metadata: None,
            },
        }
    }
}

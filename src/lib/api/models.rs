use chrono::{DateTime, Utc};
use diesel::query_source::joins::{JoinOn, LeftOuter};
use itertools::Itertools;
use serde::Serialize;
use std::collections::HashMap;

use crate::consumer::models::data_entry::DataEntryValue;
use crate::models::{AssetLabel, DataEntryType, VerificationStatus};
use crate::schema::{asset_wx_labels, assets, predefined_verifications};

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
pub struct AssetInfo {
    pub ticker: Option<String>,
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
pub struct AssetInfoBrief {
    pub ticker: Option<String>,
    pub id: String,
    pub name: String,
    pub smart: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct AssetMetadata {
    pub oracle_data: Vec<OracleData>,
    pub labels: Vec<AssetLabel>,
    pub sponsor_balance: Option<i64>,
    pub has_image: bool,
    pub verified_status: VerificationStatus,
}

impl diesel::expression::Expression for AssetLabel {
    type SqlType = diesel::sql_types::Text;
}

impl
    diesel::expression::AppearsOnTable<
        JoinOn<
            diesel::query_source::joins::Join<
                JoinOn<
                    diesel::query_source::joins::Join<
                        assets::table,
                        predefined_verifications::table,
                        LeftOuter,
                    >,
                    diesel::expression::operators::Eq<
                        predefined_verifications::columns::asset_id,
                        assets::columns::id,
                    >,
                >,
                asset_wx_labels::table,
                LeftOuter,
            >,
            diesel::expression::operators::Eq<
                asset_wx_labels::columns::asset_id,
                assets::columns::id,
            >,
        >,
    > for AssetLabel
{
}

impl diesel::query_builder::QueryFragment<diesel::pg::Pg> for AssetLabel {
    fn walk_ast(
        &self,
        mut out: diesel::query_builder::AstPass<diesel::pg::Pg>,
    ) -> diesel::QueryResult<()> {
        let v = match self.clone() {
            AssetLabel::CommunityVerified => Some("COMMUNITY_VERIFIED"),
            AssetLabel::DeFi => Some("DEFI"),
            AssetLabel::Gateway => Some("GATEWAY"),
            AssetLabel::Stablecoin => Some("STABLECOIN"),
            AssetLabel::Qualified => Some("QUALIFIED"),
            AssetLabel::WaVerified => Some("WA_VERIFIED"),
            _ => None,
        };

        if let Some(v) = v {
            out.push_bind_param::<diesel::sql_types::Text, _>(&v)?;
        }

        Ok(())
    }
}

impl diesel::expression::Expression for VerificationStatus {
    type SqlType = diesel::sql_types::Integer;
}

impl
    diesel::expression::AppearsOnTable<
        JoinOn<
            diesel::query_source::joins::Join<
                JoinOn<
                    diesel::query_source::joins::Join<
                        assets::table,
                        predefined_verifications::table,
                        LeftOuter,
                    >,
                    diesel::expression::operators::Eq<
                        predefined_verifications::columns::asset_id,
                        assets::columns::id,
                    >,
                >,
                asset_wx_labels::table,
                LeftOuter,
            >,
            diesel::expression::operators::Eq<
                asset_wx_labels::columns::asset_id,
                assets::columns::id,
            >,
        >,
    > for VerificationStatus
{
}

impl diesel::query_builder::QueryFragment<diesel::pg::Pg> for VerificationStatus {
    fn walk_ast(
        &self,
        mut out: diesel::query_builder::AstPass<diesel::pg::Pg>,
    ) -> diesel::QueryResult<()> {
        let s = match self.clone() {
            VerificationStatus::Declined => -1,
            VerificationStatus::Unknown => 0,
            VerificationStatus::Verified => 1,
        };

        out.push_bind_param::<diesel::sql_types::Integer, _>(&s)?;

        Ok(())
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct OracleData(HashMap<String, DataEntryValue>);

impl From<(Option<crate::models::AssetInfo>, bool, bool)> for Asset {
    fn from(
        (asset_info, has_image, include_metadata): (Option<crate::models::AssetInfo>, bool, bool),
    ) -> Self {
        match asset_info {
            Some(asset_info) => {
                let ai = AssetInfo {
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
                };
                let metadata = AssetMetadata {
                    has_image: has_image,
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
                                        match cur.data_type {
                                            DataEntryType::Bin => {
                                                acc.insert(
                                                    cur.key,
                                                    DataEntryValue::BinVal(cur.bin_val.unwrap()),
                                                );
                                            }
                                            DataEntryType::Bool => {
                                                acc.insert(
                                                    cur.key,
                                                    DataEntryValue::BoolVal(cur.bool_val.unwrap()),
                                                );
                                            }
                                            DataEntryType::Int => {
                                                acc.insert(
                                                    cur.key,
                                                    DataEntryValue::IntVal(cur.int_val.unwrap()),
                                                );
                                            }
                                            DataEntryType::Str => {
                                                acc.insert(
                                                    cur.key,
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
                    verified_status: asset_info.metadata.verification_status,
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

use serde::Deserialize;

use crate::models::{AssetLabel, VerificationStatus};

use super::DEFAULT_LIMIT;

#[derive(Clone, Debug, Deserialize)]
pub struct SearchRequest {
    pub ids: Option<Vec<String>>,
    pub ticker: Option<String>,
    pub search: Option<String>,
    pub smart: Option<bool>,
    pub verified_status: Option<Vec<VerificationStatus>>,
    #[serde(rename = "label__in")]
    pub asset_label_in: Option<Vec<AssetLabel>>,
    pub limit: Option<u32>,
    pub after: Option<String>,
}

impl From<SearchRequest> for crate::services::assets::SearchRequest {
    fn from(sr: SearchRequest) -> Self {
        Self {
            ids: sr.ids,
            ticker: sr.ticker,
            search: sr.search,
            smart: sr.smart,
            verification_status_in: sr.verified_status,
            asset_label_in: sr.asset_label_in,
            limit: sr.limit.unwrap_or(DEFAULT_LIMIT),
            after: sr.after,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct MgetRequest {
    pub ids: Vec<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct RequestOptions {
    pub format: Option<ResponseFormat>,
    pub include_metadata: Option<bool>,
    #[serde(rename = "height__gte")]
    pub height_gte: Option<i32>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResponseFormat {
    Full,
    Brief,
}

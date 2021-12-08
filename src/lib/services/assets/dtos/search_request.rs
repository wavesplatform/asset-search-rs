use crate::models::AssetLabel;
use crate::models::VerificationStatus;
use serde::Deserialize;

#[derive(Clone, Debug, Deserialize)]
pub struct SearchRequest {
    pub ids: Option<Vec<String>>,
    pub ticker: Option<String>,
    pub search: Option<String>,
    pub smart: Option<bool>,
    pub verification_status_in: Option<Vec<VerificationStatus>>,
    pub asset_label_in: Option<Vec<AssetLabel>>,
    pub limit: Option<i64>,
    pub after: Option<String>,
}

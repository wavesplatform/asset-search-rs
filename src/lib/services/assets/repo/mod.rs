pub mod pg;

use diesel::sql_types::Text;

use crate::{
    error::Error as AppError,
    models::{AssetLabel, VerificationStatus},
};

pub use super::entities::{Asset, OracleDataEntry, UserDefinedData};

#[derive(Clone, Debug, QueryableByName)]
pub struct AssetId {
    #[sql_type = "Text"]
    pub id: String,
}

#[derive(Clone, Debug)]
pub struct FindParams {
    pub search: Option<String>,
    pub ticker: Option<TickerFilter>,
    pub smart: Option<bool>,
    pub verification_status_in: Option<Vec<VerificationStatus>>,
    pub asset_label_in: Option<Vec<AssetLabel>>,
    pub limit: u32,
    pub after: Option<String>,
}

#[derive(Clone, Debug)]
pub enum TickerFilter {
    Any,
    One(String),
}

pub trait Repo {
    fn find(&self, params: FindParams) -> Result<Vec<AssetId>, AppError>;

    fn get(&self, id: &str, oracle_address: &str) -> Result<Option<Asset>, AppError>;

    fn mget(&self, ids: &[&str], oracle_address: &str) -> Result<Vec<Option<Asset>>, AppError>;

    fn mget_for_height(&self, ids: &[&str], height: i32, oracle_address: &str) -> Result<Vec<Option<Asset>>, AppError>;

    fn data_entries(&self, asset_ids: &[&str], oracle_address: &str) -> Result<Vec<OracleDataEntry>, AppError>;

    fn get_asset_user_defined_data(&self, id: &str, oracle_address: &str) -> Result<UserDefinedData, AppError>;

    fn mget_asset_user_defined_data(&self, ids: &[&str], oracle_address: &str) -> Result<Vec<UserDefinedData>, AppError>;

    fn all_assets_user_defined_data(&self, oracle_address: &str) -> Result<Vec<UserDefinedData>, AppError>;
}

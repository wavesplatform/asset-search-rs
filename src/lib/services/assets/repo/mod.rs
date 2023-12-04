pub mod pg;

use diesel::sql_types::Text;

use crate::error::Error as AppError;

pub use super::entities::{Asset, OracleDataEntry, UserDefinedData};

#[derive(Clone, Debug, QueryableByName)]
pub struct AssetId {
    #[diesel(sql_type = Text)]
    pub id: String,
}

#[derive(Clone, Debug)]
pub struct FindParams {
    pub search: Option<String>,
    pub ticker: Option<StringFilter>,
    pub ext_ticker: Option<StringFilter>,
    pub label: Option<StringFilter>,
    pub smart: Option<bool>,
    pub asset_label_in: Option<Vec<String>>,
    pub issuer_in: Option<Vec<String>>,
    pub limit: u32,
    pub after: Option<String>,
}

#[derive(Clone, Debug)]
pub enum StringFilter {
    Any,
    One(String),
}

pub trait Repo {
    fn find(&self, params: FindParams) -> Result<Vec<AssetId>, AppError>;

    fn get(&self, id: &str) -> Result<Option<Asset>, AppError>;

    fn mget(&self, ids: &[&str]) -> Result<Vec<Option<Asset>>, AppError>;

    fn mget_for_height(&self, ids: &[&str], height: i32) -> Result<Vec<Option<Asset>>, AppError>;

    fn data_entries(
        &self,
        asset_ids: &[&str],
        oracle_address: &str,
    ) -> Result<Vec<OracleDataEntry>, AppError>;

    fn get_asset_user_defined_data(&self, id: &str) -> Result<UserDefinedData, AppError>;

    fn mget_asset_user_defined_data(&self, ids: &[&str]) -> Result<Vec<UserDefinedData>, AppError>;

    fn all_assets_user_defined_data(&self) -> Result<Vec<UserDefinedData>, AppError>;
}

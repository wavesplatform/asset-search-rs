use std::hash::{Hash, Hasher};

use crate::schema::asset_ext_tickers;

#[derive(Clone, Debug, Queryable)]
pub struct AssetExtTicker {
    pub asset_id: String,
    pub ext_ticker: String,
}

#[derive(Clone, Debug, Insertable)]
#[diesel(table_name = asset_ext_tickers)]
pub struct InsertableAssetExtTicker {
    pub uid: i64,
    pub superseded_by: i64,
    pub block_uid: i64,
    pub asset_id: String,
    pub ext_ticker: String,
}

impl PartialEq for InsertableAssetExtTicker {
    fn eq(&self, other: &InsertableAssetExtTicker) -> bool {
        (&self.asset_id) == (&other.asset_id)
    }
}

impl Eq for InsertableAssetExtTicker {}

impl Hash for InsertableAssetExtTicker {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.asset_id.hash(state);
    }
}

#[derive(Clone, Debug)]
pub struct AssetExtTickerOverride {
    pub superseded_by: i64,
    pub asset_id: String,
}

#[derive(Clone, Debug)]
pub struct DeletedAssetExtTicker {
    pub uid: i64,
    pub asset_id: String,
}

impl PartialEq for DeletedAssetExtTicker {
    fn eq(&self, other: &Self) -> bool {
        (&self.asset_id) == (&other.asset_id)
    }
}

impl Eq for DeletedAssetExtTicker {}

impl Hash for DeletedAssetExtTicker {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.asset_id.hash(state);
    }
}

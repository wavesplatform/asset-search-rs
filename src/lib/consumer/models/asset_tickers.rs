use std::hash::{Hash, Hasher};

use crate::schema::asset_tickers;

#[derive(Clone, Debug, Queryable)]
pub struct AssetTicker {
    pub asset_id: String,
    pub ticker: String,
}

#[derive(Clone, Debug, Insertable)]
#[table_name = "asset_tickers"]
pub struct InsertableAssetTicker {
    pub uid: i64,
    pub superseded_by: i64,
    pub block_uid: i64,
    pub asset_id: String,
    pub ticker: String,
}

impl PartialEq for InsertableAssetTicker {
    fn eq(&self, other: &InsertableAssetTicker) -> bool {
        (&self.asset_id) == (&other.asset_id)
    }
}

impl Eq for InsertableAssetTicker {}

impl Hash for InsertableAssetTicker {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.asset_id.hash(state);
    }
}

#[derive(Clone, Debug)]
pub struct AssetTickerOverride {
    pub superseded_by: i64,
    pub asset_id: String,
}

#[derive(Clone, Debug)]
pub struct DeletedAssetTicker {
    pub uid: i64,
    pub asset_id: String,
}

impl PartialEq for DeletedAssetTicker {
    fn eq(&self, other: &Self) -> bool {
        (&self.asset_id) == (&other.asset_id)
    }
}

impl Eq for DeletedAssetTicker {}

impl Hash for DeletedAssetTicker {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.asset_id.hash(state);
    }
}

use std::hash::{Hash, Hasher};

use crate::schema::asset_names;

#[derive(Clone, Debug, Queryable)]
pub struct AssetTicker {
    pub asset_id: String,
    pub asset_name: String,
}

#[derive(Clone, Debug, Insertable)]
#[diesel(table_name = asset_names)]
pub struct InsertableAssetName {
    pub uid: i64,
    pub superseded_by: i64,
    pub block_uid: i64,
    pub asset_id: String,
    pub asset_name: Option<String>,
}

impl PartialEq for InsertableAssetName {
    fn eq(&self, other: &InsertableAssetName) -> bool {
        (&self.asset_id) == (&other.asset_id)
    }
}

impl Eq for InsertableAssetName {}

impl Hash for InsertableAssetName {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.asset_id.hash(state);
    }
}

#[derive(Clone, Debug)]
pub struct AssetNameOverride {
    pub superseded_by: i64,
    pub asset_id: String,
}

#[derive(Clone, Debug)]
pub struct DeletedAssetName {
    pub uid: i64,
    pub asset_id: String,
}

impl PartialEq for DeletedAssetName {
    fn eq(&self, other: &Self) -> bool {
        (&self.asset_id) == (&other.asset_id)
    }
}

impl Eq for DeletedAssetName {}

impl Hash for DeletedAssetName {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.asset_id.hash(state);
    }
}

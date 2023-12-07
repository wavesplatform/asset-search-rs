use std::hash::{Hash, Hasher};

use crate::schema::asset_descriptions;

#[derive(Clone, Debug, Queryable)]
pub struct AssetDescription {
    pub asset_id: String,
    pub asset_description: String,
}

#[derive(Clone, Debug, Insertable)]
#[diesel(table_name = asset_descriptions)]
pub struct InsertableAssetDescription {
    pub uid: i64,
    pub superseded_by: i64,
    pub block_uid: i64,
    pub asset_id: String,
    pub asset_description: Option<String>,
}

impl PartialEq for InsertableAssetDescription {
    fn eq(&self, other: &InsertableAssetDescription) -> bool {
        (&self.asset_id) == (&other.asset_id)
    }
}

impl Eq for InsertableAssetDescription {}

impl Hash for InsertableAssetDescription {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.asset_id.hash(state);
    }
}

#[derive(Clone, Debug)]
pub struct AssetDescriptionOverride {
    pub superseded_by: i64,
    pub asset_id: String,
}

#[derive(Clone, Debug)]
pub struct DeletedAssetDescription {
    pub uid: i64,
    pub asset_id: String,
}

impl PartialEq for DeletedAssetDescription {
    fn eq(&self, other: &Self) -> bool {
        (&self.asset_id) == (&other.asset_id)
    }
}

impl Eq for DeletedAssetDescription {}

impl Hash for DeletedAssetDescription {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.asset_id.hash(state);
    }
}

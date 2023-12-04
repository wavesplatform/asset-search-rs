use std::hash::{Hash, Hasher};

use crate::schema::asset_labels;

#[derive(Clone, Debug, Queryable)]
pub struct AssetLabels {
    pub asset_id: String,
    pub labels: Vec<String>,
}

#[derive(Clone, Debug, Insertable)]
#[diesel(table_name = asset_labels)]
pub struct InsertableAssetLabels {
    pub uid: i64,
    pub superseded_by: i64,
    pub block_uid: i64,
    pub asset_id: String,
    pub labels: Vec<String>,
}

impl PartialEq for InsertableAssetLabels {
    fn eq(&self, other: &InsertableAssetLabels) -> bool {
        (&self.asset_id) == (&other.asset_id)
    }
}

impl Eq for InsertableAssetLabels {}

impl Hash for InsertableAssetLabels {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.asset_id.hash(state);
    }
}

#[derive(Clone, Debug)]
pub struct AssetLabelsOverride {
    pub superseded_by: i64,
    pub asset_id: String,
}

#[derive(Clone, Debug)]
pub struct DeletedAssetLabels {
    pub uid: i64,
    pub asset_id: String,
}

impl PartialEq for DeletedAssetLabels {
    fn eq(&self, other: &Self) -> bool {
        (&self.asset_id) == (&other.asset_id)
    }
}

impl Eq for DeletedAssetLabels {}

impl Hash for DeletedAssetLabels {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.asset_id.hash(state);
    }
}

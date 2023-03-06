use chrono::{DateTime, Utc};
use std::hash::{Hash, Hasher};

use crate::schema::assets;

#[derive(Clone, Debug, Insertable)]
#[table_name = "assets"]
pub struct InsertableAsset {
    pub uid: i64,
    pub superseded_by: i64,
    pub block_uid: i64,
    pub id: String,
    pub name: String,
    pub description: String,
    pub time_stamp: DateTime<Utc>,
    pub issuer: String,
    pub precision: i32,
    pub smart: bool,
    pub nft: bool,
    pub quantity: i64,
    pub reissuable: bool,
    pub min_sponsored_fee: Option<i64>,
}

impl PartialEq for InsertableAsset {
    fn eq(&self, other: &InsertableAsset) -> bool {
        (&self.id) == (&other.id)
    }
}

impl Eq for InsertableAsset {}

impl Hash for InsertableAsset {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

#[derive(Clone, Debug)]
pub struct AssetOverride {
    pub superseded_by: i64,
    pub id: String,
}

#[derive(Clone, Debug)]
pub struct DeletedAsset {
    pub uid: i64,
    pub id: String,
}

impl PartialEq for DeletedAsset {
    fn eq(&self, other: &Self) -> bool {
        (&self.id) == (&other.id)
    }
}

impl Eq for DeletedAsset {}

impl Hash for DeletedAsset {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

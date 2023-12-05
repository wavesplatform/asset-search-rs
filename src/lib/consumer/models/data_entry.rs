use std::hash::{Hash, Hasher};

use chrono::{DateTime, Utc};
use serde::Serialize;

use crate::db::enums::DataEntryValueType;
use crate::schema::data_entries;

#[derive(Clone, Debug, Insertable)]
#[diesel(table_name = data_entries)]
pub struct InsertableDataEntry {
    pub uid: i64,
    pub superseded_by: i64,
    pub block_uid: i64,
    pub address: String,
    pub key: String,
    pub data_type: Option<DataEntryValueType>,
    pub bin_val: Option<Vec<u8>>,
    pub bool_val: Option<bool>,
    pub int_val: Option<i64>,
    pub str_val: Option<String>,
    pub related_asset_id: Option<String>,
}

impl PartialEq for InsertableDataEntry {
    fn eq(&self, other: &InsertableDataEntry) -> bool {
        (&self.address, &self.key) == (&other.address, &other.key)
    }
}

impl Eq for InsertableDataEntry {}

impl Hash for InsertableDataEntry {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.address.hash(state);
        self.key.hash(state);
    }
}

#[derive(Clone, Debug)]
pub struct DataEntryOverride {
    pub superseded_by: i64,
    pub address: String,
    pub key: String,
}

#[derive(Clone, Debug)]
pub struct DeletedDataEntry {
    pub uid: i64,
    pub address: String,
    pub key: String,
}

impl PartialEq for DeletedDataEntry {
    fn eq(&self, other: &Self) -> bool {
        (&self.address, &self.key) == (&other.address, &other.key)
    }
}

impl Eq for DeletedDataEntry {}

impl Hash for DeletedDataEntry {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.address.hash(state);
        self.key.hash(state);
    }
}

#[derive(Clone, Debug)]
pub struct DataEntryUpdate {
    pub update_height: i32,
    pub updated_at: DateTime<Utc>,
    pub address: String,
    pub key: String,
    pub value: Option<DataEntryValue>,
    pub related_asset_id: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(untagged)]
pub enum DataEntryValue {
    BinVal(Vec<u8>),
    BoolVal(bool),
    IntVal(i64),
    StrVal(String),
}

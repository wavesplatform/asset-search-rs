use chrono::{DateTime, Utc};
use std::hash::{Hash, Hasher};

use crate::schema::out_leasings;

#[derive(Clone, Debug, Insertable)]
#[diesel(table_name = out_leasings)]
pub struct InsertableOutLeasing {
    pub uid: i64,
    pub superseded_by: i64,
    pub block_uid: i64,
    pub address: String,
    pub amount: i64,
}

impl PartialEq for InsertableOutLeasing {
    fn eq(&self, other: &InsertableOutLeasing) -> bool {
        (&self.superseded_by, &self.address) == (&other.superseded_by, &other.address)
    }
}

impl Eq for InsertableOutLeasing {}

impl Hash for InsertableOutLeasing {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.superseded_by.hash(state);
        self.address.hash(state);
    }
}

#[derive(Clone, Debug)]
pub struct OutLeasingOverride {
    pub superseded_by: i64,
    pub address: String,
}

#[derive(Clone, Debug)]
pub struct DeletedOutLeasing {
    pub uid: i64,
    pub address: String,
}

impl PartialEq for DeletedOutLeasing {
    fn eq(&self, other: &Self) -> bool {
        (&self.address,) == (&other.address,)
    }
}

impl Eq for DeletedOutLeasing {}

impl Hash for DeletedOutLeasing {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.address.hash(state);
    }
}

#[derive(Clone, Debug)]
pub struct OutLeasingUpdate {
    pub updated_at: DateTime<Utc>,
    pub update_height: i32,
    pub address: String,
    pub new_amount: i64,
}

impl PartialEq for OutLeasingUpdate {
    fn eq(&self, other: &Self) -> bool {
        (&self.address) == (&other.address)
    }
}

impl Eq for OutLeasingUpdate {}

impl Hash for OutLeasingUpdate {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.address.hash(state)
    }
}

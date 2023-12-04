use chrono::{DateTime, Utc};
use std::hash::{Hash, Hasher};

use crate::schema::issuer_balances;

#[derive(Clone, Debug, Insertable)]
#[diesel(table_name = issuer_balances)]
pub struct InsertableIssuerBalance {
    pub uid: i64,
    pub superseded_by: i64,
    pub block_uid: i64,
    pub address: String,
    pub regular_balance: i64,
}

impl PartialEq for InsertableIssuerBalance {
    fn eq(&self, other: &InsertableIssuerBalance) -> bool {
        (&self.superseded_by, &self.address) == (&other.superseded_by, &other.address)
    }
}

impl Eq for InsertableIssuerBalance {}

impl Hash for InsertableIssuerBalance {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.superseded_by.hash(state);
        self.address.hash(state);
    }
}

#[derive(Clone, Debug)]
pub struct IssuerBalanceOverride {
    pub superseded_by: i64,
    pub address: String,
}

#[derive(Clone, Debug)]
pub struct DeletedIssuerBalance {
    pub uid: i64,
    pub address: String,
}

impl PartialEq for DeletedIssuerBalance {
    fn eq(&self, other: &Self) -> bool {
        (&self.address,) == (&other.address,)
    }
}

impl Eq for DeletedIssuerBalance {}

impl Hash for DeletedIssuerBalance {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.address.hash(state);
    }
}

#[derive(Clone, Debug, Queryable)]
pub struct CurrentIssuerBalance {
    pub address: String,
    pub regular_balance: i64,
}

impl PartialEq for CurrentIssuerBalance {
    fn eq(&self, other: &Self) -> bool {
        (&self.address) == (&other.address)
    }
}

impl Eq for CurrentIssuerBalance {}

impl Hash for CurrentIssuerBalance {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.address.hash(state);
    }
}

#[derive(Clone, Debug)]
pub struct IssuerBalanceUpdate {
    pub updated_at: DateTime<Utc>,
    pub update_height: i32,
    pub address: String,
    pub new_regular_balance: i64,
}

impl PartialEq for IssuerBalanceUpdate {
    fn eq(&self, other: &Self) -> bool {
        (&self.address) == (&other.address)
    }
}

impl Eq for IssuerBalanceUpdate {}

impl Hash for IssuerBalanceUpdate {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.address.hash(state)
    }
}

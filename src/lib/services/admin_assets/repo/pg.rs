use diesel::prelude::*;

use super::Repo;
use crate::db::PgPool;
use crate::error::Error as AppError;
use crate::schema::asset_wx_labels;

pub struct PgRepo {
    pg_pool: PgPool,
}

impl PgRepo {
    pub fn new(pg_pool: PgPool) -> Self {
        Self { pg_pool }
    }
}

impl Repo for PgRepo {
    fn add_label(&self, id: &str, label: &str) -> anyhow::Result<bool> {
        diesel::insert_into(asset_wx_labels::table)
            .values((
                asset_wx_labels::asset_id.eq(id),
                asset_wx_labels::label.eq(label),
            ))
            .on_conflict_do_nothing()
            .execute(&self.pg_pool.get()?)
            .map(|_affected_rows| true)
            .map_err(|err| {
                let context = format!("Cannot add asset label: {}", err);
                anyhow::Error::new(AppError::DbDieselError(err)).context(context)
            })
    }

    fn delete_label(&self, id: &str, label: &str) -> anyhow::Result<bool> {
        diesel::delete(
            asset_wx_labels::table
                .filter(asset_wx_labels::asset_id.eq(id))
                .filter(asset_wx_labels::label.eq(label)),
        )
        .execute(&self.pg_pool.get()?)
        .map(|_affected_rows| true)
        .map_err(|err| {
            let context = format!("Cannot delete asset label: {}", err);
            anyhow::Error::new(AppError::DbDieselError(err)).context(context)
        })
    }
}

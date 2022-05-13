use diesel::prelude::*;

use super::Repo;
use crate::db::enums::VerificationStatusValueType;
use crate::db::PgPool;
use crate::error::Error as AppError;
use crate::schema::{asset_wx_labels, predefined_verifications};

pub struct PgRepo {
    pg_pool: PgPool,
}

impl PgRepo {
    pub fn new(pg_pool: PgPool) -> Self {
        Self { pg_pool }
    }
}

impl Repo for PgRepo {
    fn set_verification_status(
        &self,
        id: &str,
        verification_status: &VerificationStatusValueType,
    ) -> anyhow::Result<bool> {
        let q = diesel::insert_into(predefined_verifications::table)
            .values((
                predefined_verifications::asset_id.eq(id),
                predefined_verifications::verification_status.eq(verification_status),
            ))
            .on_conflict(predefined_verifications::asset_id)
            .do_update()
            .set(predefined_verifications::verification_status.eq(verification_status));

        q.execute(&self.pg_pool.get()?)
            .map(|affected_rows| affected_rows == 1)
            .map_err(|err| {
                let context = format!("Cannot update asset verification: {}", err);
                anyhow::Error::new(AppError::DbDieselError(err)).context(context)
            })
    }

    fn update_ticker(&self, id: &str, ticker: Option<&str>) -> anyhow::Result<bool> {
        diesel::update(
            predefined_verifications::table.filter(predefined_verifications::asset_id.eq(id)),
        )
        .set(predefined_verifications::ticker.eq(ticker))
        .execute(&self.pg_pool.get()?)
        .map(|affected_rows| affected_rows == 1)
        .map_err(|err| {
            let context = format!("Cannot update asset ticker: {}", err);
            anyhow::Error::new(AppError::DbDieselError(err)).context(context)
        })
    }

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

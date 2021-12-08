use anyhow::{Error, Result};
use diesel::pg::PgConnection;
use diesel::prelude::*;
use diesel::sql_types::{Array, BigInt, VarChar};

use super::super::models::{
    asset::{AssetOverride, DeletedAsset, InsertableAsset},
    block_microblock::BlockMicroblock,
    data_entry::{DataEntryOverride, DeletedDataEntry, InsertableDataEntry},
    issuer_balance::{
        CurrentIssuerBalance, DeletedIssuerBalance, InsertableIssuerBalance, IssuerBalanceOverride,
    },
    out_leasing::{DeletedOutLeasing, InsertableOutLeasing, OutLeasingOverride},
};
use super::super::PrevHandledHeight;
use super::Repo;
use crate::error::Error as AppError;
use crate::schema::{
    assets, assets_uid_seq, blocks_microblocks, data_entries, data_entries_uid_seq,
    issuer_balances, issuer_balances_uid_seq, out_leasings, out_leasings_uid_seq,
};
use crate::tuple_len::TupleLen;
use crate::waves::WAVES_ID;

const MAX_UID: i64 = std::i64::MAX - 1;
const PG_MAX_INSERT_FIELDS_COUNT: usize = 65535;

pub struct PgRepoImpl {
    conn: PgConnection,
}

pub fn new(conn: PgConnection) -> PgRepoImpl {
    PgRepoImpl { conn }
}

#[async_trait::async_trait]
impl Repo for PgRepoImpl {
    //
    // COMMON
    //

    fn transaction(&self, f: impl FnOnce() -> Result<()>) -> Result<()> {
        self.conn.transaction(|| f())
    }

    fn get_prev_handled_height(&self) -> Result<Option<PrevHandledHeight>> {
        blocks_microblocks::table
            .select((blocks_microblocks::uid, blocks_microblocks::height))
            .filter(
                blocks_microblocks::height.eq(diesel::expression::sql_literal::sql(
                    "(select max(height) - 1 from blocks_microblocks)",
                )),
            )
            .order(blocks_microblocks::uid.asc())
            .first(&self.conn)
            .optional()
            .map_err(|err| Error::new(AppError::DbDieselError(err)))
    }

    fn get_block_uid(&self, block_id: &str) -> Result<i64> {
        blocks_microblocks::table
            .select(blocks_microblocks::uid)
            .filter(blocks_microblocks::id.eq(block_id))
            .get_result(&self.conn)
            .map_err(|err| {
                let context = format!("Cannot get block_uid by block id {}: {}", block_id, err);
                Error::new(AppError::DbDieselError(err)).context(context)
            })
    }

    fn get_key_block_uid(&self) -> Result<i64> {
        blocks_microblocks::table
            .select(diesel::expression::sql_literal::sql("max(uid)"))
            .filter(blocks_microblocks::time_stamp.is_not_null())
            .get_result(&self.conn)
            .map_err(|err| {
                let context = format!("Cannot get key block uid: {}", err);
                Error::new(AppError::DbDieselError(err)).context(context)
            })
    }

    fn get_total_block_id(&self) -> Result<Option<String>> {
        blocks_microblocks::table
            .select(blocks_microblocks::id)
            .filter(blocks_microblocks::time_stamp.is_null())
            .order(blocks_microblocks::uid.desc())
            .first(&self.conn)
            .optional()
            .map_err(|err| {
                let context = format!("Cannot get total block id: {}", err);
                Error::new(AppError::DbDieselError(err)).context(context)
            })
    }

    fn insert_blocks_or_microblocks(&self, blocks: &Vec<BlockMicroblock>) -> Result<Vec<i64>> {
        diesel::insert_into(blocks_microblocks::table)
            .values(blocks)
            .returning(blocks_microblocks::uid)
            .get_results(&self.conn)
            .map_err(|err| {
                let context = format!("Cannot insert blocks/microblocks: {}", err);
                Error::new(AppError::DbDieselError(err)).context(context)
            })
    }

    fn change_block_id(&self, block_uid: &i64, new_block_id: &str) -> Result<()> {
        diesel::update(blocks_microblocks::table)
            .set(blocks_microblocks::id.eq(new_block_id))
            .filter(blocks_microblocks::uid.eq(block_uid))
            .execute(&self.conn)
            .map(|_| ())
            .map_err(|err| {
                let context = format!("Cannot change block id: {}", err);
                Error::new(AppError::DbDieselError(err)).context(context)
            })
    }

    fn delete_microblocks(&self) -> Result<()> {
        diesel::delete(blocks_microblocks::table)
            .filter(blocks_microblocks::time_stamp.is_null())
            .execute(&self.conn)
            .map(|_| ())
            .map_err(|err| {
                let context = format!("Cannot delete microblocks: {}", err);
                Error::new(AppError::DbDieselError(err)).context(context)
            })
    }

    fn rollback_blocks_microblocks(&self, block_uid: &i64) -> Result<()> {
        diesel::delete(blocks_microblocks::table)
            .filter(blocks_microblocks::uid.gt(block_uid))
            .execute(&self.conn)
            .map(|_| ())
            .map_err(|err| {
                let context = format!("Cannot rollback blocks/microblocks: {}", err);
                Error::new(AppError::DbDieselError(err)).context(context)
            })
    }

    //
    // ASSETS
    //

    fn get_current_waves_quantity(&self) -> Result<i64> {
        assets::table
            .select(assets::quantity)
            .filter(assets::superseded_by.eq(MAX_UID))
            .filter(assets::id.eq(WAVES_ID))
            .first(&self.conn)
            .map_err(|err| {
                let context = format!("Cannot get current waves quantity: {}", err);
                Error::new(AppError::DbDieselError(err)).context(context)
            })
    }

    fn get_next_assets_uid(&self) -> Result<i64> {
        assets_uid_seq::table
            .select(assets_uid_seq::last_value)
            .first(&self.conn)
            .map_err(|err| {
                let context = format!("Cannot get next assets update uid: {}", err);
                Error::new(AppError::DbDieselError(err)).context(context)
            })
    }

    fn insert_assets(&self, new_assets: &Vec<InsertableAsset>) -> Result<()> {
        let columns_count = assets::table::all_columns().len();
        let chunk_size = (PG_MAX_INSERT_FIELDS_COUNT / columns_count) / 10 * 10;
        new_assets
            .to_owned()
            .chunks(chunk_size)
            .into_iter()
            .try_fold((), |_, chunk| {
                diesel::insert_into(assets::table)
                    .values(chunk)
                    .execute(&self.conn)
                    .map(|_| ())
            })
            .map_err(|err| {
                let context = format!("Cannot insert new assets: {}", err);
                Error::new(AppError::DbDieselError(err)).context(context)
            })
    }

    fn update_assets_block_references(&self, block_uid: &i64) -> Result<()> {
        diesel::update(assets::table)
            .set((assets::block_uid.eq(block_uid),))
            .filter(assets::block_uid.gt(block_uid))
            .execute(&self.conn)
            .map(|_| ())
            .map_err(|err| {
                let context = format!("Cannot update assets block references: {}", err);
                Error::new(AppError::DbDieselError(err)).context(context)
            })
    }

    fn close_assets_superseded_by(&self, updates: &Vec<AssetOverride>) -> Result<()> {
        let mut ids = vec![];
        let mut superseded_by_uids = vec![];

        updates.iter().for_each(|u| {
            ids.push(&u.id);
            superseded_by_uids.push(&u.superseded_by);
        });

        let q = diesel::sql_query("UPDATE assets SET superseded_by = updates.superseded_by FROM (SELECT UNNEST($1::text[]) as id, UNNEST($2::int8[]) as superseded_by) AS updates WHERE assets.id = updates.id AND assets.superseded_by = $3;")
            .bind::<Array<VarChar>, _>(ids)
            .bind::<Array<BigInt>, _>(superseded_by_uids)
            .bind::<BigInt, _>(MAX_UID);

        q.execute(&self.conn).map(|_| ()).map_err(|err| {
            let context = format!("Cannot close assets superseded_by: {}", err);
            Error::new(AppError::DbDieselError(err)).context(context)
        })
    }

    fn reopen_assets_superseded_by(&self, current_superseded_by: &Vec<i64>) -> Result<()> {
        diesel::sql_query("UPDATE assets SET superseded_by = $1 FROM (SELECT UNNEST($2) AS superseded_by) AS current WHERE assets.superseded_by = current.superseded_by;")
            .bind::<BigInt, _>(MAX_UID)
            .bind::<Array<BigInt>, _>(current_superseded_by)
            .execute(&self.conn)
            .map(|_| ())
            .map_err(|err| {
                let context = format!("Cannot reopen assets superseded_by: {}", err);
                Error::new(AppError::DbDieselError(err)).context(context)
            })
    }

    fn set_assets_next_update_uid(&self, new_uid: i64) -> Result<()> {
        diesel::sql_query(format!(
            "select setval('assets_uid_seq', {}, false);", // 3rd param - is called; in case of true, value'll be incremented before returning
            new_uid
        ))
        .execute(&self.conn)
        .map(|_| ())
        .map_err(|err| {
            let context = format!("Cannot set assets next update uid: {}", err);
            Error::new(AppError::DbDieselError(err)).context(context)
        })
    }

    fn rollback_assets(&self, block_uid: &i64) -> Result<Vec<DeletedAsset>> {
        diesel::delete(assets::table)
            .filter(assets::block_uid.gt(block_uid))
            .returning((assets::uid, assets::id))
            .get_results(&self.conn)
            .map(|bs| {
                bs.into_iter()
                    .map(|(uid, id)| DeletedAsset { uid, id })
                    .collect()
            })
            .map_err(|err| {
                let context = format!("Cannot rollback assets: {}", err);
                Error::new(AppError::DbDieselError(err)).context(context)
            })
    }

    //
    // DATA ENTRIES
    //

    fn get_next_data_entries_uid(&self) -> Result<i64> {
        data_entries_uid_seq::table
            .select(data_entries_uid_seq::last_value)
            .first(&self.conn)
            .map_err(|err| {
                let context = format!("Cannot get next data entries update uid: {}", err);
                Error::new(AppError::DbDieselError(err)).context(context)
            })
    }

    fn insert_data_entries(&self, data_entries: &Vec<InsertableDataEntry>) -> Result<()> {
        let columns_count = data_entries::table::all_columns().len();
        let chunk_size = (PG_MAX_INSERT_FIELDS_COUNT / columns_count) / 10 * 10;
        data_entries
            .to_owned()
            .chunks(chunk_size)
            .into_iter()
            .try_fold((), |_, chunk| {
                diesel::insert_into(data_entries::table)
                    .values(chunk)
                    .execute(&self.conn)
                    .map(|_| ())
            })
            .map_err(|err| {
                let context = format!("Cannot insert new data entries: {}", err);
                Error::new(AppError::DbDieselError(err)).context(context)
            })
    }

    fn update_data_entries_block_references(&self, block_uid: &i64) -> Result<()> {
        diesel::update(data_entries::table)
            .set((data_entries::block_uid.eq(block_uid),))
            .filter(data_entries::block_uid.gt(block_uid))
            .execute(&self.conn)
            .map(|_| ())
            .map_err(|err| {
                let context = format!("Cannot update data entries block references: {}", err);
                Error::new(AppError::DbDieselError(err)).context(context)
            })
    }

    fn close_data_entries_superseded_by(&self, updates: &Vec<DataEntryOverride>) -> Result<()> {
        let mut addresses = vec![];
        let mut keys = vec![];
        let mut superseded_by_uids = vec![];

        updates.iter().for_each(|u| {
            addresses.push(&u.address);
            keys.push(&u.key);
            superseded_by_uids.push(&u.superseded_by);
        });

        let q = diesel::sql_query("UPDATE data_entries SET superseded_by = updates.superseded_by FROM (SELECT UNNEST($1::text[]) as address, UNNEST($2::text[]) as key, UNNEST($3::int8[]) as superseded_by) AS updates WHERE data_entries.address = updates.address AND data_entries.key = updates.key AND data_entries.superseded_by = $4;")
            .bind::<Array<VarChar>, _>(addresses)
            .bind::<Array<VarChar>, _>(keys)
            .bind::<Array<BigInt>, _>(superseded_by_uids)
            .bind::<BigInt, _>(MAX_UID);

        q.execute(&self.conn).map(|_| ()).map_err(|err| {
            let context = format!("Cannot close data entries superseded_by: {}", err);
            Error::new(AppError::DbDieselError(err)).context(context)
        })
    }

    fn reopen_data_entries_superseded_by(&self, current_superseded_by: &Vec<i64>) -> Result<()> {
        diesel::sql_query("UPDATE data_entries SET superseded_by = $1 FROM (SELECT UNNEST($2) AS superseded_by) AS current WHERE data_entries.superseded_by = current.superseded_by;")
            .bind::<BigInt, _>(MAX_UID)
            .bind::<Array<BigInt>, _>(current_superseded_by)
            .execute(&self.conn)
            .map(|_| ())
            .map_err(|err| {
                let context = format!("Cannot reopen data entries superseded_by: {}", err);
                Error::new(AppError::DbDieselError(err)).context(context)
            })
    }

    fn set_data_entries_next_update_uid(&self, new_uid: i64) -> Result<()> {
        diesel::sql_query(format!(
            "select setval('data_entries_uid_seq', {}, false);", // 3rd param - is called; in case of true, value'll be incremented before returning
            new_uid
        ))
        .execute(&self.conn)
        .map(|_| ())
        .map_err(|err| {
            let context = format!("Cannot set data entries next update uid: {}", err);
            Error::new(AppError::DbDieselError(err)).context(context)
        })
    }

    fn rollback_data_entries(&self, block_uid: &i64) -> Result<Vec<DeletedDataEntry>> {
        diesel::delete(data_entries::table)
            .filter(data_entries::block_uid.gt(block_uid))
            .returning((data_entries::uid, data_entries::address, data_entries::key))
            .get_results(&self.conn)
            .map(|bs| {
                bs.into_iter()
                    .map(|(uid, address, key)| DeletedDataEntry { uid, address, key })
                    .collect()
            })
            .map_err(|err| {
                let context = format!("Cannot rollback pool users balances: {}", err);
                Error::new(AppError::DbDieselError(err)).context(context)
            })
    }

    //
    // ISSUER BALANCES
    //

    fn get_current_issuer_balances(&self) -> Result<Vec<CurrentIssuerBalance>> {
        issuer_balances::table
            .select((issuer_balances::address, issuer_balances::regular_balance))
            .filter(issuer_balances::superseded_by.eq(MAX_UID))
            .load(&self.conn)
            .map_err(|err| {
                let context = format!("Cannot get current issuer balances: {}", err);
                Error::new(AppError::DbDieselError(err)).context(context)
            })
    }

    fn get_next_issuer_balances_uid(&self) -> Result<i64> {
        issuer_balances_uid_seq::table
            .select(issuer_balances_uid_seq::last_value)
            .first(&self.conn)
            .map_err(|err| {
                let context = format!("Cannot get next issuer balances uid: {}", err);
                Error::new(AppError::DbDieselError(err)).context(context)
            })
    }

    fn insert_issuer_balances(&self, issuer_balances: &Vec<InsertableIssuerBalance>) -> Result<()> {
        let columns_count = issuer_balances::table::all_columns().len();
        let chunk_size = (PG_MAX_INSERT_FIELDS_COUNT / columns_count) / 10 * 10;
        issuer_balances
            .to_owned()
            .chunks(chunk_size)
            .into_iter()
            .try_fold((), |_, chunk| {
                diesel::insert_into(issuer_balances::table)
                    .values(chunk)
                    .execute(&self.conn)
                    .map(|_| ())
            })
            .map_err(|err| {
                let context = format!("Cannot insert new issuer balances: {}", err);
                Error::new(AppError::DbDieselError(err)).context(context)
            })
    }

    fn update_issuer_balances_block_references(&self, block_uid: &i64) -> Result<()> {
        diesel::update(issuer_balances::table)
            .set((issuer_balances::block_uid.eq(block_uid),))
            .filter(issuer_balances::block_uid.gt(block_uid))
            .execute(&self.conn)
            .map(|_| ())
            .map_err(|err| {
                let context = format!("Cannot update issuer balances block references: {}", err);
                Error::new(AppError::DbDieselError(err)).context(context)
            })
    }

    fn close_issuer_balances_superseded_by(
        &self,
        updates: &Vec<IssuerBalanceOverride>,
    ) -> Result<()> {
        let mut addresses = vec![];
        let mut superseded_by_uids = vec![];

        updates.iter().for_each(|u| {
            addresses.push(&u.address);
            superseded_by_uids.push(&u.superseded_by);
        });

        let q = diesel::sql_query("UPDATE issuer_balances SET superseded_by = updates.superseded_by FROM (SELECT UNNEST($1::text[]) as address, UNNEST($2::int8[]) as superseded_by) AS updates WHERE issuer_balances.address = updates.address AND issuer_balances.superseded_by = $3;")
            .bind::<Array<VarChar>, _>(addresses)
            .bind::<Array<BigInt>, _>(superseded_by_uids)
            .bind::<BigInt, _>(MAX_UID);

        q.execute(&self.conn).map(|_| ()).map_err(|err| {
            let context = format!("Cannot close issuer balances superseded_by: {}", err);
            Error::new(AppError::DbDieselError(err)).context(context)
        })
    }

    fn reopen_issuer_balances_superseded_by(&self, current_superseded_by: &Vec<i64>) -> Result<()> {
        diesel::sql_query("UPDATE issuer_balances SET superseded_by = $1 FROM (SELECT UNNEST($2) AS superseded_by) AS current WHERE issuer_balances.superseded_by = current.superseded_by;")
            .bind::<BigInt, _>(MAX_UID)
            .bind::<Array<BigInt>, _>(current_superseded_by)
            .execute(&self.conn)
            .map(|_| ())
            .map_err(|err| {
                let context = format!("Cannot reopen issuer balances superseded_by: {}", err);
                Error::new(AppError::DbDieselError(err)).context(context)
            })
    }

    fn set_issuer_balances_next_update_uid(&self, new_uid: i64) -> Result<()> {
        diesel::sql_query(format!(
            "select setval('issuer_balances_uid_seq', {}, false);", // 3rd param - is called; in case of true, value'll be incremented before returning
            new_uid
        ))
        .execute(&self.conn)
        .map(|_| ())
        .map_err(|err| {
            let context = format!("Cannot set issuer balances next uid: {}", err);
            Error::new(AppError::DbDieselError(err)).context(context)
        })
    }

    fn rollback_issuer_balances(&self, block_uid: &i64) -> Result<Vec<DeletedIssuerBalance>> {
        diesel::delete(issuer_balances::table)
            .filter(issuer_balances::block_uid.gt(block_uid))
            .returning((issuer_balances::uid, issuer_balances::address))
            .get_results(&self.conn)
            .map(|bs| {
                bs.into_iter()
                    .map(|(uid, address)| DeletedIssuerBalance { uid, address })
                    .collect()
            })
            .map_err(|err| {
                let context = format!("Cannot rollback issuer balances: {}", err);
                Error::new(AppError::DbDieselError(err)).context(context)
            })
    }

    //
    // OUT LEASINGS
    //

    fn get_next_out_leasings_uid(&self) -> Result<i64> {
        out_leasings_uid_seq::table
            .select(out_leasings_uid_seq::last_value)
            .first(&self.conn)
            .map_err(|err| {
                let context = format!("Cannot get next out leasings uid: {}", err);
                Error::new(AppError::DbDieselError(err)).context(context)
            })
    }

    fn insert_out_leasings(&self, out_leasings: &Vec<InsertableOutLeasing>) -> Result<()> {
        let columns_count = out_leasings::table::all_columns().len();
        let chunk_size = (PG_MAX_INSERT_FIELDS_COUNT / columns_count) / 10 * 10;
        out_leasings
            .to_owned()
            .chunks(chunk_size)
            .into_iter()
            .try_fold((), |_, chunk| {
                diesel::insert_into(out_leasings::table)
                    .values(chunk)
                    .execute(&self.conn)
                    .map(|_| ())
            })
            .map_err(|err| {
                let context = format!("Cannot insert new out leasings: {}", err);
                Error::new(AppError::DbDieselError(err)).context(context)
            })
    }

    fn update_out_leasings_block_references(&self, block_uid: &i64) -> Result<()> {
        diesel::update(out_leasings::table)
            .set((out_leasings::block_uid.eq(block_uid),))
            .filter(out_leasings::block_uid.gt(block_uid))
            .execute(&self.conn)
            .map(|_| ())
            .map_err(|err| {
                let context = format!("Cannot update out leasings block references: {}", err);
                Error::new(AppError::DbDieselError(err)).context(context)
            })
    }

    fn close_out_leasings_superseded_by(&self, updates: &Vec<OutLeasingOverride>) -> Result<()> {
        let mut addresses = vec![];
        let mut superseded_by_uids = vec![];

        updates.iter().for_each(|u| {
            addresses.push(&u.address);
            superseded_by_uids.push(&u.superseded_by);
        });

        let q = diesel::sql_query("UPDATE out_leasings SET superseded_by = updates.superseded_by FROM (SELECT UNNEST($1::text[]) as address, UNNEST($2::int8[]) as superseded_by) AS updates WHERE out_leasings.address = updates.address AND out_leasings.superseded_by = $3;")
            .bind::<Array<VarChar>, _>(addresses)
            .bind::<Array<BigInt>, _>(superseded_by_uids)
            .bind::<BigInt, _>(MAX_UID);

        q.execute(&self.conn).map(|_| ()).map_err(|err| {
            let context = format!("Cannot close out leasings superseded_by: {}", err);
            Error::new(AppError::DbDieselError(err)).context(context)
        })
    }

    fn reopen_out_leasings_superseded_by(&self, current_superseded_by: &Vec<i64>) -> Result<()> {
        diesel::sql_query("UPDATE out_leasings SET superseded_by = $1 FROM (SELECT UNNEST($2) AS superseded_by) AS current WHERE out_leasings.superseded_by = current.superseded_by;")
            .bind::<BigInt, _>(MAX_UID)
            .bind::<Array<BigInt>, _>(current_superseded_by)
            .execute(&self.conn)
            .map(|_| ())
            .map_err(|err| {
                let context = format!("Cannot reopen out leasings superseded_by: {}", err);
                Error::new(AppError::DbDieselError(err)).context(context)
            })
    }

    fn set_out_leasings_next_update_uid(&self, new_uid: i64) -> Result<()> {
        diesel::sql_query(format!(
            "select setval('out_leasings_uid_seq', {}, false);", // 3rd param - is called; in case of true, value'll be incremented before returning
            new_uid
        ))
        .execute(&self.conn)
        .map(|_| ())
        .map_err(|err| {
            let context = format!("Cannot set out leasings next uid: {}", err);
            Error::new(AppError::DbDieselError(err)).context(context)
        })
    }

    fn rollback_out_leasings(&self, block_uid: &i64) -> Result<Vec<DeletedOutLeasing>> {
        diesel::delete(out_leasings::table)
            .filter(out_leasings::block_uid.gt(block_uid))
            .returning((out_leasings::uid, out_leasings::address))
            .get_results(&self.conn)
            .map(|bs| {
                bs.into_iter()
                    .map(|(uid, address)| DeletedOutLeasing { uid, address })
                    .collect()
            })
            .map_err(|err| {
                let context = format!("Cannot rollback out leasings: {}", err);
                Error::new(AppError::DbDieselError(err)).context(context)
            })
    }
}

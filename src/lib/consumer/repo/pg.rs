use anyhow::{Error, Result};
use diesel::dsl::sql;
use diesel::pg::PgConnection;
use diesel::sql_types::{Array, BigInt, Bool, Text, VarChar};
use diesel::{prelude::*, sql_query};

use super::super::models::asset_labels::{
    AssetLabels, AssetLabelsOverride, DeletedAssetLabels, InsertableAssetLabels,
};
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
use super::{Repo, RepoOperations};
use crate::consumer::models::asset_descriptions::{
    AssetDescriptionOverride, DeletedAssetDescription, InsertableAssetDescription,
};
use crate::consumer::models::asset_names::{
    AssetNameOverride, DeletedAssetName, InsertableAssetName,
};
use crate::consumer::models::asset_tickers::{
    AssetTicker, AssetTickerOverride, DeletedAssetTicker, InsertableAssetTicker,
};
use crate::db::enums::DataEntryValueTypeMapping;
use crate::error::Error as AppError;
use crate::schema::{
    asset_descriptions as asset_descriptions_diesel, asset_descriptions_uid_seq, asset_labels,
    asset_labels_uid_seq, asset_names as asset_names_diesel, asset_names_uid_seq, asset_tickers,
    asset_tickers_uid_seq, assets, assets_uid_seq, blocks_microblocks, data_entries,
    data_entries_uid_seq, issuer_balances, issuer_balances_uid_seq, out_leasings,
    out_leasings_uid_seq,
};
use crate::services::assets::repo::pg::generate_assets_user_defined_data_base_sql_query;
use crate::services::assets::repo::{Asset, OracleDataEntry, UserDefinedData};
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

unsafe impl Send for PgRepoImpl {}
unsafe impl Sync for PgRepoImpl {}

#[async_trait::async_trait]
impl Repo for PgRepoImpl {
    type Operations = PgConnection;

    async fn execute<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(&Self::Operations) -> Result<R> + Send + 'static,
        R: Send + 'static,
    {
        tokio::task::block_in_place(move || f(&self.conn))
    }

    async fn transaction<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(&Self::Operations) -> Result<R> + Send + 'static,
        R: Clone + Send + 'static,
    {
        tokio::task::block_in_place(move || self.conn.transaction(|| f(&self.conn)))
    }
}

impl RepoOperations for PgConnection {
    //
    // COMMON
    //

    fn get_prev_handled_height(&self, depth: u32) -> Result<Option<PrevHandledHeight>> {
        let sql_height = format!("(select max(height) - {} from blocks_microblocks)", depth);

        blocks_microblocks::table
            .select((blocks_microblocks::uid, blocks_microblocks::height))
            .filter(blocks_microblocks::height.eq(sql(&sql_height)))
            .order(blocks_microblocks::uid.asc())
            .first(self)
            .optional()
            .map_err(|err| Error::new(AppError::DbDieselError(err)))
    }

    fn get_block_uid(&self, block_id: &str) -> Result<i64> {
        blocks_microblocks::table
            .select(blocks_microblocks::uid)
            .filter(blocks_microblocks::id.eq(block_id))
            .get_result(self)
            .map_err(|err| {
                let context = format!("Cannot get block_uid by block id {}: {}", block_id, err);
                Error::new(AppError::DbDieselError(err)).context(context)
            })
    }

    fn get_key_block_uid(&self) -> Result<i64> {
        blocks_microblocks::table
            .select(diesel::expression::sql_literal::sql("max(uid)"))
            .filter(blocks_microblocks::time_stamp.is_not_null())
            .get_result(self)
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
            .first(self)
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
            .get_results(self)
            .map_err(|err| {
                let context = format!("Cannot insert blocks/microblocks: {}", err);
                Error::new(AppError::DbDieselError(err)).context(context)
            })
    }

    fn change_block_id(&self, block_uid: &i64, new_block_id: &str) -> Result<()> {
        diesel::update(blocks_microblocks::table)
            .set(blocks_microblocks::id.eq(new_block_id))
            .filter(blocks_microblocks::uid.eq(block_uid))
            .execute(self)
            .map(|_| ())
            .map_err(|err| {
                let context = format!("Cannot change block id: {}", err);
                Error::new(AppError::DbDieselError(err)).context(context)
            })
    }

    fn delete_microblocks(&self) -> Result<()> {
        diesel::delete(blocks_microblocks::table)
            .filter(blocks_microblocks::time_stamp.is_null())
            .execute(self)
            .map(|_| ())
            .map_err(|err| {
                let context = format!("Cannot delete microblocks: {}", err);
                Error::new(AppError::DbDieselError(err)).context(context)
            })
    }

    fn rollback_blocks_microblocks(&self, block_uid: &i64) -> Result<()> {
        diesel::delete(blocks_microblocks::table)
            .filter(blocks_microblocks::uid.gt(block_uid))
            .execute(self)
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
            .first(self)
            .map_err(|err| {
                let context = format!("Cannot get current waves quantity: {}", err);
                Error::new(AppError::DbDieselError(err)).context(context)
            })
    }

    fn get_next_assets_uid(&self) -> Result<i64> {
        assets_uid_seq::table
            .select(assets_uid_seq::last_value)
            .first(self)
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
                    .execute(self)
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
            .execute(self)
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

        q.execute(self).map(|_| ()).map_err(|err| {
            let context = format!("Cannot close assets superseded_by: {}", err);
            Error::new(AppError::DbDieselError(err)).context(context)
        })
    }

    fn reopen_assets_superseded_by(&self, current_superseded_by: &Vec<i64>) -> Result<()> {
        diesel::sql_query("UPDATE assets SET superseded_by = $1 FROM (SELECT UNNEST($2) AS superseded_by) AS current WHERE assets.superseded_by = current.superseded_by;")
            .bind::<BigInt, _>(MAX_UID)
            .bind::<Array<BigInt>, _>(current_superseded_by)
            .execute(self)
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
        .execute(self)
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
            .get_results(self)
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

    fn assets_gt_block_uid(&self, block_uid: &i64) -> Result<Vec<i64>> {
        assets::table
            .select(assets::uid)
            .filter(assets::block_uid.gt(block_uid))
            .get_results(self)
            .map_err(|err| {
                let context = format!(
                    "Cannot get assets greater then block_uid {}: {}",
                    block_uid, err
                );
                Error::new(AppError::DbDieselError(err)).context(context)
            })
    }

    fn mget_assets(&self, uids: &[i64]) -> Result<Vec<Option<Asset>>> {
        let q = sql_query("SELECT
            a.id,
            a.name,
            a.precision,
            a.description,
            bm.height,
            a.time_stamp as timestamp,
            a.issuer,
            a.quantity,
            a.reissuable,
            a.min_sponsored_fee,
            a.smart,
            a.nft,
            CASE WHEN a.min_sponsored_fee IS NULL THEN NULL ELSE ib.regular_balance END AS sponsor_regular_balance,
            CASE WHEN a.min_sponsored_fee IS NULL THEN NULL ELSE ol.amount END          AS sponsor_out_leasing
            FROM assets AS a
            LEFT JOIN blocks_microblocks bm ON a.block_uid = bm.uid
            LEFT JOIN issuer_balances ib ON ib.address = a.issuer AND ib.superseded_by = $1
            LEFT JOIN out_leasings ol ON ol.address = a.issuer AND ol.superseded_by = $1
            WHERE a.superseded_by = $1 AND a.uid = ANY($2)"
        )
        .bind::<BigInt, _>(MAX_UID)
        .bind::<Array<BigInt>, _>(uids);

        q.load(self).map_err(|err| {
            let context = format!("Cannot mget assets: {}", err);
            Error::new(AppError::DbDieselError(err)).context(context)
        })
    }

    fn assets_oracle_data_entries(
        &self,
        asset_ids: &[&str],
        oracle_address: &str,
    ) -> Result<Vec<OracleDataEntry>> {
        let q = data_entries::table
            .select((
                sql::<Text>("related_asset_id"),
                data_entries::address,
                data_entries::key,
                sql::<DataEntryValueTypeMapping>("data_type"),
                data_entries::bin_val,
                data_entries::bool_val,
                data_entries::int_val,
                data_entries::str_val,
            ))
            .filter(data_entries::superseded_by.eq(MAX_UID))
            .filter(data_entries::address.eq(oracle_address))
            .filter(data_entries::related_asset_id.eq_any(asset_ids))
            .filter(data_entries::data_type.is_not_null());

        q.load(self).map_err(|err| {
            let context = format!("Cannot assets oracle data entries: {}", err);
            Error::new(AppError::DbDieselError(err)).context(context)
        })
    }

    fn issuer_assets(&self, issuer: impl AsRef<str>) -> Result<Vec<Asset>> {
        let q = sql_query("SELECT
            a.id,
            a.name,
            a.precision,
            a.description,
            bm.height,
            a.time_stamp as timestamp,
            a.issuer,
            a.quantity,
            a.reissuable,
            a.min_sponsored_fee,
            a.smart,
            a.nft,
            ast.ticker,
            CASE WHEN a.min_sponsored_fee IS NULL THEN NULL ELSE ib.regular_balance END AS sponsor_regular_balance,
            CASE WHEN a.min_sponsored_fee IS NULL THEN NULL ELSE ol.amount END          AS sponsor_out_leasing
            FROM assets AS a
            LEFT JOIN blocks_microblocks bm ON a.block_uid = bm.uid
            LEFT JOIN issuer_balances ib ON ib.address = a.issuer AND ib.superseded_by = $1
            LEFT JOIN out_leasings ol ON ol.address = a.issuer AND ol.superseded_by = $1
            LEFT JOIN asset_tickers ast ON a.id = ast.asset_id AND ast.superseded_by = $1
            WHERE a.superseded_by = $1 AND a.nft = $2 AND a.issuer = $3"
        )
        .bind::<BigInt, _>(MAX_UID)
        .bind::<Bool, _>(false)
        .bind::<Text, _>(issuer.as_ref());

        q.load(self).map_err(|err| {
            let context = format!("Cannot issuer {} assets: {}", issuer.as_ref(), err);
            Error::new(AppError::DbDieselError(err)).context(context)
        })
    }

    //
    // ASSET LABELS
    //

    fn mget_asset_labels(&self, asset_ids: &[&str]) -> Result<Vec<AssetLabels>> {
        let q = asset_labels::table
            .select((asset_labels::asset_id, asset_labels::labels))
            .filter(asset_labels::superseded_by.eq(MAX_UID))
            .filter(asset_labels::asset_id.eq_any(asset_ids));

        q.load(self).map_err(|err| {
            let context = format!("Cannot assets labels: {}", err);
            Error::new(AppError::DbDieselError(err)).context(context)
        })
    }

    fn get_next_asset_labels_uid(&self) -> Result<i64> {
        asset_labels_uid_seq::table
            .select(asset_labels_uid_seq::last_value)
            .first(self)
            .map_err(|err| {
                let context = format!("Cannot get next asset labels update uid: {}", err);
                Error::new(AppError::DbDieselError(err)).context(context)
            })
    }

    fn insert_asset_labels(&self, labels: &Vec<InsertableAssetLabels>) -> Result<()> {
        let columns_count = asset_labels::table::all_columns().len();
        let chunk_size = (PG_MAX_INSERT_FIELDS_COUNT / columns_count) / 10 * 10;
        labels
            .to_owned()
            .chunks(chunk_size)
            .into_iter()
            .try_fold((), |_, chunk| {
                diesel::insert_into(asset_labels::table)
                    .values(chunk)
                    .execute(self)
                    .map(|_| ())
            })
            .map_err(|err| {
                let context = format!("Cannot insert new asset labels: {}", err);
                Error::new(AppError::DbDieselError(err)).context(context)
            })
    }

    fn update_asset_labels_block_references(&self, block_uid: &i64) -> Result<()> {
        diesel::update(asset_labels::table)
            .set((asset_labels::block_uid.eq(block_uid),))
            .filter(asset_labels::block_uid.gt(block_uid))
            .execute(self)
            .map(|_| ())
            .map_err(|err| {
                let context = format!("Cannot update asset_labels block references: {}", err);
                Error::new(AppError::DbDieselError(err)).context(context)
            })
    }

    fn close_asset_labels_superseded_by(&self, updates: &Vec<AssetLabelsOverride>) -> Result<()> {
        let mut asset_ids = vec![];
        let mut superseded_by_uids = vec![];

        updates.iter().for_each(|u| {
            asset_ids.push(&u.asset_id);
            superseded_by_uids.push(&u.superseded_by);
        });

        let q = diesel::sql_query("UPDATE asset_labels SET superseded_by = updates.superseded_by FROM (SELECT UNNEST($1::text[]) as asset_id, UNNEST($2::int8[]) as superseded_by) AS updates WHERE asset_labels.asset_id = updates.asset_id AND asset_labels.superseded_by = $3;")
            .bind::<Array<VarChar>, _>(asset_ids)
            .bind::<Array<BigInt>, _>(superseded_by_uids)
            .bind::<BigInt, _>(MAX_UID);

        q.execute(self).map(|_| ()).map_err(|err| {
            let context = format!("Cannot close asset_labels superseded_by: {}", err);
            Error::new(AppError::DbDieselError(err)).context(context)
        })
    }

    fn reopen_asset_labels_superseded_by(&self, current_superseded_by: &Vec<i64>) -> Result<()> {
        diesel::sql_query("UPDATE asset_labels SET superseded_by = $1 FROM (SELECT UNNEST($2) AS superseded_by) AS current WHERE asset_labels.superseded_by = current.superseded_by;")
            .bind::<BigInt, _>(MAX_UID)
            .bind::<Array<BigInt>, _>(current_superseded_by)
            .execute(self)
            .map(|_| ())
            .map_err(|err| {
                let context = format!("Cannot reopen asset_labels superseded_by: {}", err);
                Error::new(AppError::DbDieselError(err)).context(context)
            })
    }

    fn set_asset_labels_next_update_uid(&self, new_uid: i64) -> Result<()> {
        diesel::sql_query(format!(
            "select setval('asset_labels_uid_seq', {}, false);", // 3rd param - is called; in case of true, value'll be incremented before returning
            new_uid
        ))
        .execute(self)
        .map(|_| ())
        .map_err(|err| {
            let context = format!("Cannot set asset_labels next update uid: {}", err);
            Error::new(AppError::DbDieselError(err)).context(context)
        })
    }

    fn rollback_asset_labels(&self, block_uid: &i64) -> Result<Vec<DeletedAssetLabels>> {
        diesel::delete(asset_labels::table)
            .filter(asset_labels::block_uid.gt(block_uid))
            .returning((asset_labels::uid, asset_labels::asset_id))
            .get_results(self)
            .map(|bs| {
                bs.into_iter()
                    .map(|(uid, asset_id)| DeletedAssetLabels { uid, asset_id })
                    .collect()
            })
            .map_err(|err| {
                let context = format!("Cannot rollback asset_labels: {}", err);
                Error::new(AppError::DbDieselError(err)).context(context)
            })
    }

    //
    // ASSET TICKERS
    //

    fn mget_asset_tickers(&self, asset_ids: &[&str]) -> Result<Vec<AssetTicker>> {
        let q = asset_tickers::table
            .select((asset_tickers::asset_id, asset_tickers::ticker))
            .filter(asset_tickers::superseded_by.eq(MAX_UID))
            .filter(asset_tickers::asset_id.eq_any(asset_ids));

        q.load(self).map_err(|err| {
            let context = format!("Cannot assets tickers: {}", err);
            Error::new(AppError::DbDieselError(err)).context(context)
        })
    }

    fn get_next_asset_tickers_uid(&self) -> Result<i64> {
        asset_tickers_uid_seq::table
            .select(asset_tickers_uid_seq::last_value)
            .first(self)
            .map_err(|err| {
                let context = format!("Cannot get next asset tickers update uid: {}", err);
                Error::new(AppError::DbDieselError(err)).context(context)
            })
    }

    fn close_asset_tickers_superseded_by(&self, updates: &Vec<AssetTickerOverride>) -> Result<()> {
        let mut asset_ids = vec![];
        let mut superseded_by_uids = vec![];

        updates.iter().for_each(|u| {
            asset_ids.push(&u.asset_id);
            superseded_by_uids.push(&u.superseded_by);
        });

        let q = diesel::sql_query("UPDATE asset_tickers SET superseded_by = updates.superseded_by FROM (SELECT UNNEST($1::text[]) as asset_id, UNNEST($2::int8[]) as superseded_by) AS updates WHERE asset_tickers.asset_id = updates.asset_id AND asset_tickers.superseded_by = $3;")
            .bind::<Array<VarChar>, _>(asset_ids)
            .bind::<Array<BigInt>, _>(superseded_by_uids)
            .bind::<BigInt, _>(MAX_UID);

        q.execute(self).map(|_| ()).map_err(|err| {
            let context = format!("Cannot close asset_tickers superseded_by: {}", err);
            Error::new(AppError::DbDieselError(err)).context(context)
        })
    }

    fn insert_asset_tickers(&self, updates: &Vec<InsertableAssetTicker>) -> Result<()> {
        let columns_count = asset_tickers::table::all_columns().len();
        let chunk_size = (PG_MAX_INSERT_FIELDS_COUNT / columns_count) / 10 * 10;
        updates
            .to_owned()
            .chunks(chunk_size)
            .into_iter()
            .try_fold((), |_, chunk| {
                diesel::insert_into(asset_tickers::table)
                    .values(chunk)
                    .execute(self)
                    .map(|_| ())
            })
            .map_err(|err| {
                let context = format!("Cannot insert new asset tickers: {}", err);
                Error::new(AppError::DbDieselError(err)).context(context)
            })
    }

    fn set_asset_tickers_next_update_uid(&self, new_uid: i64) -> Result<()> {
        diesel::sql_query(format!(
            "select setval('asset_tickers_uid_seq', {}, false);", // 3rd param - is called; in case of true, value'll be incremented before returning
            new_uid
        ))
        .execute(self)
        .map(|_| ())
        .map_err(|err| {
            let context = format!("Cannot set asset_tickers next update uid: {}", err);
            Error::new(AppError::DbDieselError(err)).context(context)
        })
    }

    fn rollback_asset_tickers(&self, block_uid: &i64) -> Result<Vec<DeletedAssetTicker>> {
        diesel::delete(asset_tickers::table)
            .filter(asset_tickers::block_uid.gt(block_uid))
            .returning((asset_tickers::uid, asset_tickers::asset_id))
            .get_results(self)
            .map(|bs| {
                bs.into_iter()
                    .map(|(uid, asset_id)| DeletedAssetTicker { uid, asset_id })
                    .collect()
            })
            .map_err(|err| {
                let context = format!("Cannot rollback asset_tickers: {}", err);
                Error::new(AppError::DbDieselError(err)).context(context)
            })
    }

    fn reopen_asset_tickers_superseded_by(&self, current_superseded_by: &Vec<i64>) -> Result<()> {
        diesel::sql_query("UPDATE asset_tickers SET superseded_by = $1 FROM (SELECT UNNEST($2) AS superseded_by) AS current WHERE asset_tickers.superseded_by = current.superseded_by;")
            .bind::<BigInt, _>(MAX_UID)
            .bind::<Array<BigInt>, _>(current_superseded_by)
            .execute(self)
            .map(|_| ())
            .map_err(|err| {
                let context = format!("Cannot reopen asset_tickers superseded_by: {}", err);
                Error::new(AppError::DbDieselError(err)).context(context)
            })
    }

    fn update_asset_tickers_block_references(&self, block_uid: &i64) -> Result<()> {
        diesel::update(asset_tickers::table)
            .set((asset_tickers::block_uid.eq(block_uid),))
            .filter(asset_tickers::block_uid.gt(block_uid))
            .execute(self)
            .map(|_| ())
            .map_err(|err| {
                let context = format!("Cannot update asset_tickers block references: {}", err);
                Error::new(AppError::DbDieselError(err)).context(context)
            })
    }

    //
    // DATA ENTRIES
    //

    fn get_next_data_entries_uid(&self) -> Result<i64> {
        data_entries_uid_seq::table
            .select(data_entries_uid_seq::last_value)
            .first(self)
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
                    .execute(self)
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
            .execute(self)
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

        q.execute(self).map(|_| ()).map_err(|err| {
            let context = format!("Cannot close data entries superseded_by: {}", err);
            Error::new(AppError::DbDieselError(err)).context(context)
        })
    }

    fn reopen_data_entries_superseded_by(&self, current_superseded_by: &Vec<i64>) -> Result<()> {
        diesel::sql_query("UPDATE data_entries SET superseded_by = $1 FROM (SELECT UNNEST($2) AS superseded_by) AS current WHERE data_entries.superseded_by = current.superseded_by;")
            .bind::<BigInt, _>(MAX_UID)
            .bind::<Array<BigInt>, _>(current_superseded_by)
            .execute(self)
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
        .execute(self)
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
            .get_results(self)
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
            .load(self)
            .map_err(|err| {
                let context = format!("Cannot get current issuer balances: {}", err);
                Error::new(AppError::DbDieselError(err)).context(context)
            })
    }

    fn get_next_issuer_balances_uid(&self) -> Result<i64> {
        issuer_balances_uid_seq::table
            .select(issuer_balances_uid_seq::last_value)
            .first(self)
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
                    .execute(self)
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
            .execute(self)
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

        q.execute(self).map(|_| ()).map_err(|err| {
            let context = format!("Cannot close issuer balances superseded_by: {}", err);
            Error::new(AppError::DbDieselError(err)).context(context)
        })
    }

    fn reopen_issuer_balances_superseded_by(&self, current_superseded_by: &Vec<i64>) -> Result<()> {
        diesel::sql_query("UPDATE issuer_balances SET superseded_by = $1 FROM (SELECT UNNEST($2) AS superseded_by) AS current WHERE issuer_balances.superseded_by = current.superseded_by;")
            .bind::<BigInt, _>(MAX_UID)
            .bind::<Array<BigInt>, _>(current_superseded_by)
            .execute(self)
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
        .execute(self)
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
            .get_results(self)
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
            .first(self)
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
                    .execute(self)
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
            .execute(self)
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

        q.execute(self).map(|_| ()).map_err(|err| {
            let context = format!("Cannot close out leasings superseded_by: {}", err);
            Error::new(AppError::DbDieselError(err)).context(context)
        })
    }

    fn reopen_out_leasings_superseded_by(&self, current_superseded_by: &Vec<i64>) -> Result<()> {
        diesel::sql_query("UPDATE out_leasings SET superseded_by = $1 FROM (SELECT UNNEST($2) AS superseded_by) AS current WHERE out_leasings.superseded_by = current.superseded_by;")
            .bind::<BigInt, _>(MAX_UID)
            .bind::<Array<BigInt>, _>(current_superseded_by)
            .execute(self)
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
        .execute(self)
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
            .get_results(self)
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

    //
    // ASSET_NAMES
    //

    fn get_next_asset_names_uid(&self) -> Result<i64> {
        asset_names_uid_seq::table
            .select(asset_names_uid_seq::last_value)
            .first(self)
            .map_err(|err| {
                let context = format!("Cannot get next asset names update uid: {}", err);
                Error::new(AppError::DbDieselError(err)).context(context)
            })
    }

    fn insert_asset_names(&self, names: &Vec<InsertableAssetName>) -> Result<()> {
        let columns_count = asset_names_diesel::table::all_columns().len();
        let chunk_size = (PG_MAX_INSERT_FIELDS_COUNT / columns_count) / 10 * 10;
        names
            .to_owned()
            .chunks(chunk_size)
            .into_iter()
            .try_fold((), |_, chunk| {
                diesel::insert_into(asset_names_diesel::table)
                    .values(chunk)
                    .execute(self)
                    .map(|_| ())
            })
            .map_err(|err| {
                let context = format!("Cannot insert new asset names: {}", err);
                Error::new(AppError::DbDieselError(err)).context(context)
            })
    }

    fn update_asset_names_block_references(&self, block_uid: &i64) -> Result<()> {
        diesel::update(asset_names_diesel::table)
            .set((asset_names_diesel::block_uid.eq(block_uid),))
            .filter(asset_names_diesel::block_uid.gt(block_uid))
            .execute(self)
            .map(|_| ())
            .map_err(|err| {
                let context = format!("Cannot update asset_names block references: {}", err);
                Error::new(AppError::DbDieselError(err)).context(context)
            })
    }

    fn close_asset_names_superseded_by(&self, updates: &Vec<AssetNameOverride>) -> Result<()> {
        let mut asset_ids = vec![];
        let mut superseded_by_uids = vec![];

        updates.iter().for_each(|u| {
            asset_ids.push(&u.asset_id);
            superseded_by_uids.push(&u.superseded_by);
        });

        let q = diesel::sql_query("UPDATE asset_names SET superseded_by = updates.superseded_by FROM (SELECT UNNEST($1::text[]) as asset_id, UNNEST($2::int8[]) as superseded_by) AS updates WHERE asset_names.asset_id = updates.asset_id AND asset_names.superseded_by = $3;")
            .bind::<Array<VarChar>, _>(asset_ids)
            .bind::<Array<BigInt>, _>(superseded_by_uids)
            .bind::<BigInt, _>(MAX_UID);

        q.execute(self).map(|_| ()).map_err(|err| {
            let context = format!("Cannot close asset_names superseded_by: {}", err);
            Error::new(AppError::DbDieselError(err)).context(context)
        })
    }

    fn reopen_asset_names_superseded_by(&self, current_superseded_by: &Vec<i64>) -> Result<()> {
        diesel::sql_query("UPDATE asset_names SET superseded_by = $1 FROM (SELECT UNNEST($2) AS superseded_by) AS current WHERE asset_names.superseded_by = current.superseded_by;")
            .bind::<BigInt, _>(MAX_UID)
            .bind::<Array<BigInt>, _>(current_superseded_by)
            .execute(self)
            .map(|_| ())
            .map_err(|err| {
                let context = format!("Cannot reopen asset_names superseded_by: {}", err);
                Error::new(AppError::DbDieselError(err)).context(context)
            })
    }

    fn set_asset_names_next_update_uid(&self, new_uid: i64) -> Result<()> {
        diesel::sql_query(format!(
            "select setval('asset_names_uid_seq', {}, false);", // 3rd param - is called; in case of true, value'll be incremented before returning
            new_uid
        ))
        .execute(self)
        .map(|_| ())
        .map_err(|err| {
            let context = format!("Cannot set asset_names next update uid: {}", err);
            Error::new(AppError::DbDieselError(err)).context(context)
        })
    }

    fn rollback_asset_names(&self, block_uid: &i64) -> Result<Vec<DeletedAssetName>> {
        diesel::delete(asset_names_diesel::table)
            .filter(asset_names_diesel::block_uid.gt(block_uid))
            .returning((asset_names_diesel::uid, asset_names_diesel::asset_id))
            .get_results(self)
            .map(|bs| {
                bs.into_iter()
                    .map(|(uid, asset_id)| DeletedAssetName { uid, asset_id })
                    .collect()
            })
            .map_err(|err| {
                let context = format!("Cannot rollback asset_names: {}", err);
                Error::new(AppError::DbDieselError(err)).context(context)
            })
    }

    //
    // ASSET_DESCRIPTIONS
    //

    fn get_next_asset_descriptions_uid(&self) -> Result<i64> {
        asset_descriptions_uid_seq::table
            .select(asset_descriptions_uid_seq::last_value)
            .first(self)
            .map_err(|err| {
                let context = format!("Cannot get next asset descriptions update uid: {}", err);
                Error::new(AppError::DbDieselError(err)).context(context)
            })
    }

    fn insert_asset_descriptions(
        &self,
        descriptions: &Vec<InsertableAssetDescription>,
    ) -> Result<()> {
        let columns_count = asset_descriptions_diesel::table::all_columns().len();
        let chunk_size = (PG_MAX_INSERT_FIELDS_COUNT / columns_count) / 10 * 10;
        descriptions
            .to_owned()
            .chunks(chunk_size)
            .into_iter()
            .try_fold((), |_, chunk| {
                diesel::insert_into(asset_descriptions_diesel::table)
                    .values(chunk)
                    .execute(self)
                    .map(|_| ())
            })
            .map_err(|err| {
                let context = format!("Cannot insert new asset descriptions: {}", err);
                Error::new(AppError::DbDieselError(err)).context(context)
            })
    }

    fn update_asset_descriptions_block_references(&self, block_uid: &i64) -> Result<()> {
        diesel::update(asset_descriptions_diesel::table)
            .set((asset_descriptions_diesel::block_uid.eq(block_uid),))
            .filter(asset_descriptions_diesel::block_uid.gt(block_uid))
            .execute(self)
            .map(|_| ())
            .map_err(|err| {
                let context = format!("Cannot update asset_descriptions block references: {}", err);
                Error::new(AppError::DbDieselError(err)).context(context)
            })
    }

    fn close_asset_descriptions_superseded_by(
        &self,
        updates: &Vec<AssetDescriptionOverride>,
    ) -> Result<()> {
        let mut asset_ids = vec![];
        let mut superseded_by_uids = vec![];

        updates.iter().for_each(|u| {
            asset_ids.push(&u.asset_id);
            superseded_by_uids.push(&u.superseded_by);
        });

        let q = diesel::sql_query("UPDATE asset_descriptions SET superseded_by = updates.superseded_by FROM (SELECT UNNEST($1::text[]) as asset_id, UNNEST($2::int8[]) as superseded_by) AS updates WHERE asset_descriptions.asset_id = updates.asset_id AND asset_descriptions.superseded_by = $3")
            .bind::<Array<VarChar>, _>(asset_ids)
            .bind::<Array<BigInt>, _>(superseded_by_uids)
            .bind::<BigInt, _>(MAX_UID);

        q.execute(self).map(|_| ()).map_err(|err| {
            let context = format!("Cannot close asset_descriptions superseded_by: {}", err);
            Error::new(AppError::DbDieselError(err)).context(context)
        })
    }

    fn reopen_asset_descriptions_superseded_by(
        &self,
        current_superseded_by: &Vec<i64>,
    ) -> Result<()> {
        diesel::sql_query("UPDATE asset_descriptions SET superseded_by = $1 FROM (SELECT UNNEST($2) AS superseded_by) AS current WHERE asset_descriptions.superseded_by = current.superseded_by;")
            .bind::<BigInt, _>(MAX_UID)
            .bind::<Array<BigInt>, _>(current_superseded_by)
            .execute(self)
            .map(|_| ())
            .map_err(|err| {
                let context = format!("Cannot reopen asset_descriptions superseded_by: {}", err);
                Error::new(AppError::DbDieselError(err)).context(context)
            })
    }

    fn set_asset_descriptions_next_update_uid(&self, new_uid: i64) -> Result<()> {
        diesel::sql_query(format!(
            "select setval('asset_descriptions_uid_seq', {}, false);", // 3rd param - is called; in case of true, value'll be incremented before returning
            new_uid
        ))
        .execute(self)
        .map(|_| ())
        .map_err(|err| {
            let context = format!("Cannot set asset_descriptions next update uid: {}", err);
            Error::new(AppError::DbDieselError(err)).context(context)
        })
    }

    fn rollback_asset_descriptions(&self, block_uid: &i64) -> Result<Vec<DeletedAssetDescription>> {
        diesel::delete(asset_descriptions_diesel::table)
            .filter(asset_descriptions_diesel::block_uid.gt(block_uid))
            .returning((
                asset_descriptions_diesel::uid,
                asset_descriptions_diesel::asset_id,
            ))
            .get_results(self)
            .map(|bs| {
                bs.into_iter()
                    .map(|(uid, asset_id)| DeletedAssetDescription { uid, asset_id })
                    .collect()
            })
            .map_err(|err| {
                let context = format!("Cannot rollback asset_descriptions: {}", err);
                Error::new(AppError::DbDieselError(err)).context(context)
            })
    }

    // Methods for searching assets data for consumer
    fn data_entries(
        &self,
        asset_ids: &[String],
        oracle_address: String,
    ) -> Result<Vec<OracleDataEntry>> {
        data_entries::table
            .select((
                sql::<Text>("related_asset_id"),
                data_entries::address,
                data_entries::key,
                sql::<DataEntryValueTypeMapping>("data_type"),
                data_entries::bin_val,
                data_entries::bool_val,
                data_entries::int_val,
                data_entries::str_val,
            ))
            .filter(data_entries::superseded_by.eq(MAX_UID))
            .filter(data_entries::address.eq_all(oracle_address))
            .filter(data_entries::related_asset_id.eq_any(asset_ids))
            .filter(data_entries::data_type.is_not_null())
            .get_results(self)
            .map_err(|err| {
                let context = format!("Cannot get data_entries: {}", err);
                Error::new(AppError::DbDieselError(err)).context(context)
            })
    }

    fn mget_assets_by_ids(&self, ids: &[String]) -> Result<Vec<Option<Asset>>> {
        //use same query from api
        use crate::services::assets::repo::pg::ASSETS_BLOCKCHAIN_DATA_BASE_SQL_QUERY;

        let sql = format!(
            "{} WHERE a.uid IN (SELECT DISTINCT ON (a.id) a.uid FROM assets a WHERE a.nft = false AND a.superseded_by = $1 AND a.id = ANY($2) ORDER BY a.id, a.uid DESC)",
            ASSETS_BLOCKCHAIN_DATA_BASE_SQL_QUERY.as_str()
        );

        sql_query(&sql)
            .bind::<BigInt, _>(MAX_UID)
            .bind::<Array<Text>, _>(ids)
            .get_results(self)
            .map_err(|err| {
                let context = format!("Cannot get assets data: {}", err);
                Error::new(AppError::DbDieselError(err)).context(context)
            })
    }

    fn get_last_asset_ids_by_issuers(&self, issuers_ids: &[String]) -> Result<Vec<String>> {
        assets::table
            .select(assets::id)
            .distinct()
            .filter(assets::superseded_by.eq(MAX_UID))
            .filter(assets::issuer.eq_any(issuers_ids))
            .get_results(self)
            .map_err(|err| {
                let context = format!("Cannot get assets ids by issuers: {}", err);
                Error::new(AppError::DbDieselError(err)).context(context)
            })
    }

    fn mget_asset_user_defined_data(&self, asset_ids: &[String]) -> Result<Vec<UserDefinedData>> {
        sql_query(&format!(
            "{} WHERE a.id = ANY(ARRAY[$1]) AND a.superseded_by = $2",
            generate_assets_user_defined_data_base_sql_query()
        ))
        .bind::<Array<Text>, _>(asset_ids)
        .bind::<BigInt, _>(MAX_UID)
        .get_results(self)
        .map_err(|err| {
            let context = format!("Cannot get assets data: {}", err);
            Error::new(AppError::DbDieselError(err)).context(context)
        })
    }
}

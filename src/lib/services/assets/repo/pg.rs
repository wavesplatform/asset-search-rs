use diesel::dsl::sql;
use diesel::sql_types::{Array, BigInt, Integer, Text};
use diesel::{prelude::*, sql_query};
use itertools::Itertools;
use wavesexchange_log::error;

use super::{Asset, AssetId, FindParams, OracleDataEntry, Repo, TickerFilter, UserDefinedData};
use crate::db::enums::{
    DataEntryValueTypeMapping, VerificationStatusValueType, VerificationStatusValueTypeMapping,
};
use crate::db::PgPool;
use crate::error::Error as AppError;
use crate::models::AssetLabel;
use crate::schema::data_entries;

const MAX_UID: i64 = i64::MAX - 1;

pub struct PgRepo {
    pg_pool: PgPool,
}

impl PgRepo {
    pub fn new(pg_pool: PgPool) -> Self {
        Self { pg_pool }
    }
}

impl Repo for PgRepo {
    fn find(&self, params: FindParams) -> Result<Vec<AssetId>, AppError> {
        // conditions have to be collected before assets_cte_query construction
        // because of difference in searching by text and searching by ticker
        let mut conditions = vec![];

        if let Some(verification_statuses) = params.verification_status_in {
            conditions.push(format!(
                "pv.verification_status = ANY(ARRAY[{}])",
                verification_statuses
                    .iter()
                    .map(|vs| VerificationStatusValueType::from(vs))
                    .join(",")
            ));
        } else {
            conditions.push(format!(
                "(pv.verification_status = ANY(ARRAY[{}]) OR pv.verification_status IS NULL)",
                vec![
                    VerificationStatusValueType::Declined,
                    VerificationStatusValueType::Unknown,
                    VerificationStatusValueType::Verified,
                ]
                .iter()
                .join(","),
            ));
        }

        if let Some(asset_labels) = params.asset_label_in {
            if asset_labels.contains(&AssetLabel::WithoutLabels) {
                conditions.push(format!(
                    "(asset_wx_labels = ANY(ARRAY[{}) OR asset_wx_labels IS NULL",
                    asset_labels
                        .iter()
                        .filter(|al| **al != AssetLabel::WithoutLabels)
                        .map(|al| format!("'{}'", al))
                        .join(",")
                ));
            } else {
                conditions.push(format!(
                    "asset_wx_labels = ANY(ARRAY[{})",
                    asset_labels.iter().map(|al| format!("'{}'", al)).join(",")
                ));
            }
        }

        if let Some(smart) = params.smart {
            conditions.push(format!("a.smart = {}", smart));
        }

        let assets_cte_query = if let Some(search) = params.search.as_ref() {
            let min_block_uid_subquery =
                "SELECT min(block_uid) AS block_uid FROM assets WHERE id = a.id";

            let search_by_id_query = format!("SELECT a.id, ({}) as block_uid, CASE WHEN pv.ticker IS NULL THEN 128 ELSE 256 END AS rank FROM assets AS a LEFT JOIN predefined_verifications AS pv ON pv.asset_id = a.id WHERE a.superseded_by = {} AND a.nft = {} AND a.id ILIKE '{}'", min_block_uid_subquery, MAX_UID, false, search);
            // UNION
            let search_by_meta_query = format!("SELECT id, block_uid, ts_rank(to_tsvector('simple', name), plainto_tsquery('simple', '{}'), 3) * CASE WHEN ticker IS NULL THEN 64 ELSE 128 END AS rank FROM asset_metadatas WHERE name ILIKE '{}%'", search, search);
            // UNION
            let search_by_ticker_query = format!("SELECT a.id, ({}) as block_uid, 32 AS rank FROM assets AS a LEFT JOIN predefined_verifications AS pv ON pv.asset_id = a.id WHERE a.superseded_by = {} AND a.nft = {} AND pv.ticker ILIKE '{}%'", min_block_uid_subquery, MAX_UID, false, search);
            // UNION
            let search_by_tsquery_query = format!("SELECT a.id, ({}) as block_uid, ts_rank(to_tsvector('simple', a.name), plainto_tsquery('simple', '{}'), 3) * CASE WHEN pv.ticker IS NULL THEN 16 ELSE 32 END AS rank FROM assets a LEFT JOIN predefined_verifications AS pv ON pv.asset_id = a.id WHERE a.superseded_by = {} AND a.nft = {} AND to_tsvector('simple', a.name) @@ to_tsquery('simple', '{}:*')", min_block_uid_subquery, search, MAX_UID, false, search);
            // UNION
            let search_by_name_query = format!("SELECT a.id, ({}) as block_uid, ts_rank(to_tsvector('simple', a.name), plainto_tsquery('simple', '{}'), 3) * CASE WHEN pv.ticker IS NULL THEN 16 ELSE 32 END AS rank FROM assets a LEFT JOIN predefined_verifications AS pv ON pv.asset_id = a.id WHERE a.superseded_by = {} AND a.nft = {} AND a.name ILIKE '{}%'", min_block_uid_subquery, search, MAX_UID, false, search);

            let search_query = vec![
                search_by_id_query,
                search_by_meta_query,
                search_by_ticker_query,
                search_by_tsquery_query,
                search_by_name_query,
            ]
            .join(" UNION ");

            format!(
                "SELECT DISTINCT ON (a.id)
                    a.id,
                    ROW_NUMBER() OVER (ORDER BY a.rank DESC, a.block_uid ASC, a.id ASC) AS rn
                FROM
                    ({}) AS a
                LEFT JOIN predefined_verifications AS pv ON pv.asset_id = a.id
                LEFT JOIN asset_wx_labels AS awl ON awl.asset_id = a.id
                WHERE {}
                ORDER BY a.id ASC, a.rank DESC",
                search_query,
                conditions.iter().join(" AND ")
            )
        } else {
            // search by ticker only if there is not searching by text
            if let Some(ticker) = params.ticker.as_ref() {
                match ticker {
                    TickerFilter::One(ticker) => {
                        conditions.push(format!("pv.ticker = '{}'", ticker));
                    }
                    TickerFilter::Any => {
                        conditions.push(format!("pv.ticker IS NOT NULL"));
                    }
                }
            }

            format!(
                "SELECT DISTINCT ON (a.id, a.block_uid)
                    a.id,
                    ROW_NUMBER() OVER (ORDER BY a.block_uid ASC, a.id ASC) AS rn
                FROM
                    (SELECT a.id, (SELECT min(block_uid) FROM assets WHERE id = a.id) AS block_uid FROM assets AS a WHERE a.superseded_by = {} AND a.nft = {}) AS a
                LEFT JOIN predefined_verifications AS pv ON pv.asset_id = a.id
                LEFT JOIN asset_wx_labels AS awl ON awl.asset_id = a.id
                WHERE {}
                ORDER BY a.block_uid ASC",
                MAX_UID,
                false,
                conditions.iter().join(" AND ")
            )
        };

        let mut query = format!(
            "WITH assets_cte AS ({}) SELECT a.id FROM assets_cte AS a",
            assets_cte_query
        );

        if let Some(after) = params.after {
            query = format!(
                "{} WHERE a.rn > (SELECT rn FROM assets_cte WHERE id = '{}')",
                query, after
            );
        }

        let q =
            sql_query(format!("{} ORDER BY a.rn LIMIT $1", query)).bind::<BigInt, _>(params.limit);

        q.load(&self.pg_pool.get()?).map_err(|e| {
            error!("{:?}", e);
            AppError::from(e)
        })
    }

    fn get(&self, id: &str, oracle_address: &str) -> Result<Option<Asset>, AppError> {
        let q = sql_query("
            SELECT
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
                pv.ticker                                                                                                              AS ticker,
                COALESCE(pv.verification_status, $1)                                                                                        AS verification_status,
                COALESCE(awl.wx_labels, CASE WHEN wa_label.verification_status = 2 THEN ARRAY['WA_VERIFIED'] ELSE ARRAY[]::text[] END) AS labels,
                CASE WHEN a.min_sponsored_fee IS NULL THEN NULL ELSE (ib.regular_balance + COALESCE(ol.amount, 0)) END                 AS sponsor_balance
            FROM assets AS a
            LEFT JOIN blocks_microblocks bm ON a.block_uid = bm.uid
            LEFT JOIN predefined_verifications pv ON a.id = pv.asset_id
            LEFT JOIN (SELECT asset_id, array_agg(label::text) AS wx_labels FROM asset_wx_labels GROUP BY asset_id) AS awl ON awl.asset_id = a.id
            LEFT JOIN (SELECT int_val as verification_status, related_asset_id, key FROM data_entries WHERE address = '$2') AS wa_label ON wa_label.related_asset_id = a.id AND wa_label.key = 'status_<' || a.id || '>'
            LEFT JOIN issuer_balances ib ON ib.address = a.issuer AND ib.superseded_by = $3
            LEFT JOIN out_leasings ol ON ol.address = a.issuer AND ol.superseded_by = $3
            WHERE a.superseded_by = $3 AND a.id = $4
            LIMIT 1
        ")
        .bind::<VerificationStatusValueTypeMapping, _>(VerificationStatusValueType::Unknown)
            .bind::<Text, _>(oracle_address)
            .bind::<BigInt, _>(MAX_UID)
            .bind::<Text, _>(id);

        q.get_result(&self.pg_pool.get()?).optional().map_err(|e| {
            error!("{:?}", e);
            AppError::from(e)
        })
    }

    fn mget(&self, ids: &[&str], oracle_address: &str) -> Result<Vec<Option<Asset>>, AppError> {
        let q = sql_query("
            SELECT 
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
                pv.ticker                                                                                                              AS ticker,
                COALESCE(pv.verification_status, $1)                                                                                        AS verification_status,
                COALESCE(awl.wx_labels, CASE WHEN wa_label.verification_status = 2 THEN ARRAY['WA_VERIFIED'] ELSE ARRAY[]::text[] END) AS labels,
                CASE WHEN a.min_sponsored_fee IS NULL THEN NULL ELSE (ib.regular_balance + COALESCE(ol.amount, 0)) END                 AS sponsor_balance
            FROM assets AS a
            LEFT JOIN blocks_microblocks bm ON a.block_uid = bm.uid
            LEFT JOIN predefined_verifications pv ON a.id = pv.asset_id
            LEFT JOIN (SELECT asset_id, array_agg(label::text) AS wx_labels FROM asset_wx_labels GROUP BY asset_id) AS awl ON awl.asset_id = a.id
            LEFT JOIN (SELECT int_val as verification_status, related_asset_id, key FROM data_entries WHERE address = '$2') AS wa_label ON wa_label.related_asset_id = a.id AND wa_label.key = 'status_<' || a.id || '>'
            LEFT JOIN issuer_balances ib ON ib.address = a.issuer AND ib.superseded_by = $3
            LEFT JOIN out_leasings ol ON ol.address = a.issuer AND ol.superseded_by = $3
            WHERE a.superseded_by = $3 AND a.id = ANY($4)
        ")
            .bind::<VerificationStatusValueTypeMapping, _>(VerificationStatusValueType::Unknown)
            .bind::<Text, _>(oracle_address)
            .bind::<BigInt, _>(MAX_UID)
            .bind::<Array<Text>, _>(ids);

        q.load(&self.pg_pool.get()?).map_err(|e| {
            error!("{:?}", e);
            AppError::from(e)
        })
    }

    fn mget_for_height(
        &self,
        ids: &[&str],
        height: i32,
        oracle_address: &str,
    ) -> Result<Vec<Option<Asset>>, AppError> {
        let q = sql_query("
            SELECT 
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
                pv.ticker                                                                                                              AS ticker,
                COALESCE(pv.verification_status, 'unknown')                                                                                        AS verification_status,
                COALESCE(awl.wx_labels, CASE WHEN wa_label.verification_status = 2 THEN ARRAY['WA_VERIFIED'] ELSE ARRAY[]::text[] END) AS labels,
                CASE WHEN a.min_sponsored_fee IS NULL THEN NULL ELSE (ib.regular_balance + COALESCE(ol.amount, 0)) END                 AS sponsor_balance
            FROM assets AS a
            LEFT JOIN blocks_microblocks bm ON a.block_uid = bm.uid
            LEFT JOIN predefined_verifications pv ON a.id = pv.asset_id
            LEFT JOIN (SELECT asset_id, array_agg(label::text) AS wx_labels FROM asset_wx_labels GROUP BY asset_id) AS awl ON awl.asset_id = a.id
            LEFT JOIN (SELECT int_val as verification_status, related_asset_id, key FROM data_entries WHERE address = '$1') AS wa_label ON wa_label.related_asset_id = a.id AND wa_label.key = 'status_<' || a.id || '>'
            LEFT JOIN issuer_balances ib ON ib.address = a.issuer AND ib.superseded_by = $2
            LEFT JOIN out_leasings ol ON ol.address = a.issuer AND ol.superseded_by = $2
            WHERE a.id = ANY($3) AND a.block_uid <= (SELECT uid FROM blocks_microblocks WHERE height = $4 LIMIT 1)
        ")
            .bind::<Text, _>(oracle_address)
            .bind::<BigInt, _>(MAX_UID)
            .bind::<Array<Text>, _>(ids)
            .bind::<Integer, _>(height);

        q.load(&self.pg_pool.get()?).map_err(|e| {
            error!("{:?}", e);
            AppError::from(e)
        })
    }

    fn data_entries(
        &self,
        asset_ids: &[&str],
        oracle_address: &str,
    ) -> Result<Vec<OracleDataEntry>, AppError> {
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
            .filter(data_entries::address.eq_all(oracle_address))
            .filter(data_entries::related_asset_id.eq_any(asset_ids))
            .filter(data_entries::data_type.is_not_null());

        q.load(&self.pg_pool.get()?).map_err(|e| {
            error!("{:?}", e);
            AppError::from(e)
        })
    }

    fn get_asset_user_defined_data(
        &self,
        asset_id: &str,
        oracle_address: &str,
    ) -> Result<UserDefinedData, AppError> {
        let q = sql_query("
            SELECT 
                a.id,
                pv.ticker,
                COALESCE(pv.verification_status, 'unknown'::verification_status_value_type) AS verification_status,
                COALESCE(awl.wx_labels, CASE WHEN wa_label.verification_status = 2 THEN ARRAY['WA_VERIFIED'] ELSE ARRAY[]::text[] END) AS labels
            FROM assets a
            LEFT JOIN predefined_verifications pv ON a.id = pv.asset_id
            LEFT JOIN (SELECT asset_id, array_agg(label::text) AS wx_labels FROM asset_wx_labels GROUP BY asset_id) AS awl ON awl.asset_id = a.id
            LEFT JOIN (SELECT int_val as verification_status, related_asset_id, key FROM data_entries WHERE address = '$1') AS wa_label ON wa_label.related_asset_id = a.id AND wa_label.key = 'status_<' || a.id || '>'
            WHERE a.id = $2 AND a.superseded_by = $3
            LIMIT 1
        ")
            .bind::<Text, _>(oracle_address)
            .bind::<Text, _>(asset_id)
            .bind::<BigInt,_>(MAX_UID);

        q.get_result(&self.pg_pool.get()?).map_err(|e| {
            error!("{:?}", e);
            AppError::from(e)
        })
    }

    fn mget_asset_user_defined_data(
        &self,
        asset_ids: &[&str],
        oracle_address: &str,
    ) -> Result<Vec<UserDefinedData>, AppError> {
        let q = sql_query("
            SELECT 
                a.id as asset_id,
                pv.ticker,
                COALESCE(pv.verification_status, 'unknown'::verification_status_value_type) AS verification_status,
                COALESCE(awl.wx_labels, CASE WHEN wa_label.verification_status = 2 THEN ARRAY['WA_VERIFIED'] ELSE ARRAY[]::text[] END) AS labels
            FROM assets a
            LEFT JOIN predefined_verifications pv ON a.id = pv.asset_id
            LEFT JOIN (SELECT asset_id, array_agg(label::text) AS wx_labels FROM asset_wx_labels GROUP BY asset_id) AS awl ON awl.asset_id = a.id
            LEFT JOIN (SELECT int_val as verification_status, related_asset_id, key FROM data_entries WHERE address = '$1') AS wa_label ON wa_label.related_asset_id = a.id AND wa_label.key = 'status_<' || a.id || '>'
            WHERE a.id = ANY(ARRAY[$2]) AND a.superseded_by = $3
        ")
            .bind::<Text, _>(oracle_address)
            .bind::<Array<Text>, _>(asset_ids)
            .bind::<BigInt, _>(MAX_UID);

        q.load(&self.pg_pool.get()?).map_err(|e| {
            error!("{:?}", e);
            AppError::from(e)
        })
    }

    fn all_assets_user_defined_data(
        &self,
        oracle_address: &str,
    ) -> Result<Vec<UserDefinedData>, AppError> {
        let q = sql_query("
            SELECT 
                a.id as asset_id,
                pv.ticker,
                COALESCE(pv.verification_status, 'unknown'::verification_status_value_type) AS verification_status,
                COALESCE(awl.wx_labels, CASE WHEN wa_label.verification_status = 2 THEN ARRAY['WA_VERIFIED'] ELSE ARRAY[]::text[] END) AS labels
            FROM assets a
            LEFT JOIN predefined_verifications pv ON a.id = pv.asset_id
            LEFT JOIN (SELECT asset_id, array_agg(label::text) AS wx_labels FROM asset_wx_labels GROUP BY asset_id) AS awl ON awl.asset_id = a.id
            LEFT JOIN (SELECT int_val as verification_status, related_asset_id, key FROM data_entries WHERE address = '$1') AS wa_label ON wa_label.related_asset_id = a.id AND wa_label.key = 'status_<' || a.id || '>'
            WHERE a.superseded_by = $2
        ")
            .bind::<Text, _>(oracle_address)
            .bind::<BigInt, _>(MAX_UID);

        q.load(&self.pg_pool.get()?).map_err(|e| {
            error!("{:?}", e);
            AppError::from(e)
        })
    }
}

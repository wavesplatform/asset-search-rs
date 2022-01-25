use std::convert::TryFrom;

use diesel::dsl::sql;
use diesel::pg::Pg;
use diesel::sql_types::{Array, BigInt, Integer, Text};
use diesel::{debug_query, prelude::*, sql_query};
use itertools::Itertools;
use lazy_static::lazy_static;
use wavesexchange_log::error;

use super::{Asset, AssetId, FindParams, OracleDataEntry, Repo, TickerFilter, UserDefinedData};
use crate::db::enums::{
    AssetWxLabelValueType, DataEntryValueTypeMapping, VerificationStatusValueType,
};
use crate::db::PgPool;
use crate::error::Error as AppError;
use crate::models::AssetLabel;
use crate::schema::data_entries;

const MAX_UID: i64 = i64::MAX - 1;

lazy_static! {
    static ref ASSETS_BLOCKCHAIN_DATA_BASE_SQL_QUERY: String =  format!("SELECT DISTINCT ON (a.id)
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
        LEFT JOIN issuer_balances ib ON ib.address = a.issuer AND ib.superseded_by = {}
        LEFT JOIN out_leasings ol ON ol.address = a.issuer AND ol.superseded_by = {}
    ", MAX_UID, MAX_UID);
}

pub struct PgRepo {
    pg_pool: PgPool,
}

impl PgRepo {
    pub fn new(pg_pool: PgPool) -> Self {
        Self { pg_pool }
    }
}

impl Repo for PgRepo {
    fn find(&self, params: FindParams, oracle_address: &str) -> Result<Vec<AssetId>, AppError> {
        // conditions have to be collected before assets_cte_query construction
        // because of difference in searching by text and searching by ticker
        let mut conditions = vec![];

        // Verification Statuses Filtering
        if let Some(verification_statuses) = &params.verification_status_in {
            let verification_statuses = verification_statuses
                .iter()
                .map(|vs| VerificationStatusValueType::from(vs))
                .collect_vec();

            let verification_statuses_filter =
                if verification_statuses.contains(&VerificationStatusValueType::Unknown) {
                    format!(
                    "(pv.verification_status IS NULL OR pv.verification_status = ANY(ARRAY[{}]))",
                    verification_statuses.iter().join(",")
                )
                } else {
                    format!(
                        "pv.verification_status = ANY(ARRAY[{}])",
                        verification_statuses.iter().join(",")
                    )
                };
            conditions.push(verification_statuses_filter);
        }

        // AssetLabel Filtering
        if let Some(asset_labels) = params.asset_label_in {
            let mut label_filters = vec![];

            if asset_labels.contains(&AssetLabel::WithoutLabels) {
                label_filters.push(format!("awl.label IS NULL"));
            }

            let asset_labels = asset_labels
                .iter()
                // AssetLabel::WithoutLabels has not matching value in AssetWxLabelValueType
                .filter(|al| **al != AssetLabel::WithoutLabels)
                .map(|al| AssetWxLabelValueType::try_from(al))
                .collect::<Result<Vec<_>, AppError>>()?;

            if asset_labels.len() > 0 {
                let labels_filter = format!(
                    "awl.label = ANY(ARRAY[{}]::asset_wx_label_value_type[])",
                    asset_labels.iter().join(",")
                );
                label_filters.push(labels_filter);
            }

            conditions.push(format!("({})", label_filters.join(" OR ")));
        }

        if let Some(smart) = params.smart {
            conditions.push(format!("a.smart = {}", smart));
        }

        let assets_cte_query = if let Some(search) = params.search.as_ref() {
            let search = utils::pg_escape(search);
            let min_block_uid_subquery =
                "SELECT min(block_uid) AS block_uid FROM assets WHERE id = a.id";

            let search_escaped_for_like = utils::escape_for_like(&search);

            let search_by_id_query = format!("SELECT a.id, a.smart, ({}) as block_uid, CASE WHEN pv.ticker IS NULL THEN 128 ELSE 256 END AS rank FROM assets AS a LEFT JOIN predefined_verifications AS pv ON pv.asset_id = a.id WHERE a.superseded_by = {} AND a.nft = {} AND a.id ILIKE '{}'", min_block_uid_subquery, MAX_UID, false, search_escaped_for_like);
            // UNION
            let search_by_meta_query = format!("SELECT id, false AS smart, block_uid, ts_rank(to_tsvector('simple', name), plainto_tsquery('simple', '{}'), 3) * CASE WHEN ticker IS NULL THEN 64 ELSE 128 END AS rank FROM asset_metadatas WHERE name ILIKE '{}%'", search, search_escaped_for_like);
            // UNION
            let search_by_ticker_query = format!("SELECT a.id, a.smart, ({}) as block_uid, 32 AS rank FROM assets AS a LEFT JOIN predefined_verifications AS pv ON pv.asset_id = a.id WHERE a.superseded_by = {} AND a.nft = {} AND pv.ticker ILIKE '{}%'", min_block_uid_subquery, MAX_UID, false, search_escaped_for_like);
            // UNION
            let tsquery_condition = {
                let search_escaped_for_tsquery = utils::escape_for_tsquery(&search);
                if search_escaped_for_tsquery.len() > 0 {
                    format!(
                        "to_tsvector('simple', a.name) @@ to_tsquery('simple', '{}:*')",
                        search_escaped_for_tsquery
                    )
                } else {
                    "1=1".to_owned()
                }
            };
            let search_by_tsquery_query = format!("SELECT a.id, a.smart, ({}) as block_uid, ts_rank(to_tsvector('simple', a.name), plainto_tsquery('simple', '{}'), 3) * CASE WHEN pv.ticker IS NULL THEN 16 ELSE 32 END AS rank FROM assets a LEFT JOIN predefined_verifications AS pv ON pv.asset_id = a.id WHERE a.superseded_by = {} AND a.nft = {} AND {}", min_block_uid_subquery, search, MAX_UID, false, tsquery_condition);
            // UNION
            let search_by_name_query = format!("SELECT a.id, a.smart, ({}) as block_uid, ts_rank(to_tsvector('simple', a.name), plainto_tsquery('simple', '{}'), 3) * CASE WHEN pv.ticker IS NULL THEN 16 ELSE 32 END AS rank FROM assets a LEFT JOIN predefined_verifications AS pv ON pv.asset_id = a.id WHERE a.superseded_by = {} AND a.nft = {} AND a.name ILIKE '{}%'", min_block_uid_subquery, search, MAX_UID, false, search_escaped_for_like);

            let search_query = vec![
                search_by_id_query,
                search_by_meta_query,
                search_by_ticker_query,
                search_by_tsquery_query,
                search_by_name_query,
            ]
            .join(" UNION ");

            let conditions = if conditions.len() > 0 {
                format!("WHERE {}", conditions.iter().join(" AND "))
            } else {
                "".to_owned()
            };

            format!(
                "SELECT DISTINCT ON (a.id)
                    a.id,
                    ROW_NUMBER() OVER (ORDER BY a.rank DESC, a.block_uid ASC, a.id ASC) AS rn
                FROM
                    ({}) AS a
                LEFT JOIN (SELECT asset_id, ticker, COALESCE(verification_status, 'unknown') as verification_status FROM predefined_verifications) AS pv ON pv.asset_id = a.id
                LEFT JOIN (SELECT asset_id, label FROM asset_wx_labels UNION SELECT related_asset_id, 'wa_verified'::asset_wx_label_value_type FROM data_entries WHERE address = '{}' AND key = 'status_<' || related_asset_id || '>' AND int_val = 2 AND related_asset_id IS NOT NULL AND superseded_by = {}) AS awl ON awl.asset_id = a.id
                {}
                ORDER BY a.id ASC, a.rank DESC",
                search_query,
                oracle_address,
                MAX_UID,
                conditions
            )
        } else {
            // search by ticker only if there is not searching by text
            if let Some(ticker) = params.ticker.as_ref() {
                match ticker {
                    TickerFilter::One(ticker) => {
                        let ticker = utils::pg_escape(ticker);
                        conditions.push(format!("pv.ticker = '{}'", ticker));
                    }
                    TickerFilter::Any => {
                        conditions.push(format!("pv.ticker IS NOT NULL"));
                    }
                }
            }

            let conditions = if conditions.len() > 0 {
                format!("WHERE {}", conditions.iter().join(" AND "))
            } else {
                "".to_owned()
            };

            format!(
                "SELECT DISTINCT ON (a.id, a.block_uid)
                    a.id,
                    ROW_NUMBER() OVER (ORDER BY a.block_uid ASC, a.id ASC) AS rn
                FROM
                    (SELECT a.id, a.smart, (SELECT min(a1.block_uid) FROM assets a1 WHERE a1.id = a.id) AS block_uid FROM assets AS a WHERE a.superseded_by = {} AND a.nft = {}) AS a
                LEFT JOIN (SELECT asset_id, ticker, COALESCE(verification_status, 'unknown') as verification_status FROM predefined_verifications) AS pv ON pv.asset_id = a.id
                LEFT JOIN (SELECT asset_id, label FROM asset_wx_labels UNION SELECT related_asset_id, 'wa_verified'::asset_wx_label_value_type FROM data_entries WHERE address = '{}' AND key = 'status_<' || related_asset_id || '>' AND int_val = 2 AND related_asset_id IS NOT NULL AND superseded_by = {}) AS awl ON awl.asset_id = a.id
                {}
                ORDER BY a.block_uid ASC",
                MAX_UID,
                false,
                oracle_address,
                MAX_UID,
                conditions
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

        let q = sql_query(format!("{} ORDER BY a.rn LIMIT $1", query))
            .bind::<Integer, _>(params.limit as i32);

        let dbg = debug_query::<Pg, _>(&q).to_string();
        println!("DEBUG: {}", dbg);

        q.load(&self.pg_pool.get()?).map_err(|e| {
            error!("{:?}", e);
            AppError::from(e)
        })
    }

    fn get(&self, id: &str) -> Result<Option<Asset>, AppError> {
        let q = sql_query(&format!(
            "{} WHERE a.nft = false AND a.superseded_by = $1 AND a.id = $2 ORDER BY a.id, a.uid DESC LIMIT 1",
            ASSETS_BLOCKCHAIN_DATA_BASE_SQL_QUERY.as_str()
        ))
        .bind::<BigInt, _>(MAX_UID)
        .bind::<Text, _>(id);

        q.get_result(&self.pg_pool.get()?).optional().map_err(|e| {
            error!("{:?}", e);
            AppError::from(e)
        })
    }

    fn mget(&self, ids: &[&str]) -> Result<Vec<Option<Asset>>, AppError> {
        let q = sql_query(&format!(
            "{} WHERE a.nft = false AND a.superseded_by = $1 AND a.id = ANY($2) ORDER BY a.id, a.uid DESC",
            ASSETS_BLOCKCHAIN_DATA_BASE_SQL_QUERY.as_str()
        ))
        .bind::<BigInt, _>(MAX_UID)
        .bind::<Array<Text>, _>(ids);

        q.load(&self.pg_pool.get()?).map_err(|e| {
            error!("{:?}", e);
            AppError::from(e)
        })
    }

    fn mget_for_height(&self, ids: &[&str], height: i32) -> Result<Vec<Option<Asset>>, AppError> {
        let q = sql_query(&format!("
            {} WHERE a.nft = false AND a.id = ANY($1) AND a.block_uid <= (SELECT uid FROM blocks_microblocks WHERE height = $2 LIMIT 1) ORDER BY a.id, a.uid DESC", ASSETS_BLOCKCHAIN_DATA_BASE_SQL_QUERY.as_str()))
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
        let q = sql_query(&format!(
            "{} WHERE a.id = $1 AND a.superseded_by = $2 LIMIT 1",
            generate_assets_user_defined_data_base_sql_query(oracle_address)
        ))
        .bind::<Text, _>(asset_id)
        .bind::<BigInt, _>(MAX_UID);

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
        let q = sql_query(&format!(
            "{} WHERE a.id = ANY(ARRAY[$1]) AND a.superseded_by = $2",
            generate_assets_user_defined_data_base_sql_query(oracle_address)
        ))
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
        let q = sql_query(&format!(
            "{} WHERE a.superseded_by = $1",
            generate_assets_user_defined_data_base_sql_query(oracle_address)
        ))
        .bind::<BigInt, _>(MAX_UID);

        q.load(&self.pg_pool.get()?).map_err(|e| {
            error!("{:?}", e);
            AppError::from(e)
        })
    }
}

fn generate_assets_user_defined_data_base_sql_query(oracle_address: &str) -> String {
    format!("SELECT 
        a.id as asset_id,
        pv.ticker,
        COALESCE(pv.verification_status, 'unknown'::verification_status_value_type) AS verification_status,
        COALESCE(awl.labels, ARRAY[]::asset_wx_label_value_type[])  AS labels
        FROM assets a
        LEFT JOIN predefined_verifications pv ON a.id = pv.asset_id
        LEFT JOIN (SELECT asset_id, ARRAY_AGG(label) as labels FROM (SELECT asset_id, label FROM asset_wx_labels UNION SELECT related_asset_id AS asset_id, 'wa_verified'::asset_wx_label_value_type AS label FROM data_entries WHERE address = '{}' AND key = 'status_<' || related_asset_id || '>' AND int_val = 2 AND related_asset_id IS NOT NULL AND superseded_by = {}) AS l GROUP BY asset_id) AS awl ON awl.asset_id = a.id
    ", oracle_address, MAX_UID)
}

mod utils {
    use regex::Regex;
    use std::borrow::Cow;

    pub(super) fn escape_for_tsquery(query: &str) -> String {
        let p1 = Regex::new(r"[^\w\s]|_").unwrap();
        let p2 = Regex::new(r"\s+").unwrap();
        let query = p1.replace_all(&query, "").to_string();
        p2.replace_all(query.trim(), " & ").to_string()
    }

    pub(super) fn escape_for_like(query: &str) -> String {
        let p = Regex::new(r"%").unwrap();
        p.replace_all(&query, "\\%").to_string()
    }

    pub(super) fn pg_escape<'a>(text: &'a str) -> Cow<'a, str> {
        let bytes = text.as_bytes();

        let mut owned = None;

        for pos in 0..bytes.len() {
            let special = match bytes[pos] {
                0x07 => Some(b'a'),
                0x08 => Some(b'b'),
                b'\t' => Some(b't'),
                b'\n' => Some(b'n'),
                0x0b => Some(b'v'),
                0x0c => Some(b'f'),
                b'\r' => Some(b'r'),
                b' ' => Some(b' '),
                b'\\' => Some(b'\\'),
                b'\'' => Some(b'\''),
                _ => None,
            };

            if let Some(s) = special {
                if owned.is_none() {
                    owned = Some(bytes[0..pos].to_owned());
                }
                if s.eq(&b'\'') {
                    owned.as_mut().unwrap().push(b'\'');
                } else {
                    owned.as_mut().unwrap().push(b'\\');
                }
                owned.as_mut().unwrap().push(s);
            } else if let Some(owned) = owned.as_mut() {
                owned.push(bytes[pos]);
            }
        }

        if let Some(owned) = owned {
            unsafe { Cow::Owned(String::from_utf8_unchecked(owned)) }
        } else {
            unsafe { Cow::Borrowed(std::str::from_utf8_unchecked(bytes)) }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::utils::escape_for_tsquery;

    #[test]
    fn should_escape_for_tsquery() {
        let test_cases = vec![("asd", "asd"), ("asd+", "asd"), ("asd dsa", "asd & dsa")];

        test_cases.into_iter().for_each(|(src, expected)| {
            assert_eq!(escape_for_tsquery(src), expected);
        });
    }
}

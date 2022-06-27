use diesel::dsl::sql;
use diesel::sql_types::{Array, BigInt, Integer, Text};
use diesel::{prelude::*, sql_query};
use itertools::Itertools;
use lazy_static::lazy_static;
use wavesexchange_log::error;

use super::{Asset, AssetId, FindParams, OracleDataEntry, Repo, TickerFilter, UserDefinedData};
use crate::db::enums::DataEntryValueTypeMapping;
use crate::db::PgPool;
use crate::error::Error as AppError;
use crate::schema::data_entries;

const MAX_UID: i64 = i64::MAX - 1;

lazy_static! {
    static ref ASSETS_BLOCKCHAIN_DATA_BASE_SQL_QUERY: String =  format!("SELECT
        a.id,
        a.name,
        a.precision,
        a.description,
        bm.height,
        (SELECT DATE_TRUNC('second', MIN(time_stamp)) FROM assets WHERE id = a.id) as timestamp,
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
        LEFT JOIN blocks_microblocks bm ON (SELECT min(block_uid) FROM assets WHERE id = a.id) = bm.uid
        LEFT JOIN issuer_balances ib ON ib.address = a.issuer AND ib.superseded_by = {}
        LEFT JOIN out_leasings ol ON ol.address = a.issuer AND ol.superseded_by = {}
        LEFT JOIN asset_tickers ast ON a.id = ast.asset_id AND ast.superseded_by = {}
    ", MAX_UID, MAX_UID, MAX_UID);
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
    fn find(&self, params: FindParams) -> Result<Vec<AssetId>, AppError> {
        // conditions have to be collected before assets_cte_query construction
        // because of difference in searching by text and searching by ticker
        let mut conditions = vec![];

        // AssetLabel Filtering
        if let Some(asset_labels) = params.asset_label_in {
            let mut label_filters = vec![];

            if asset_labels.contains(&"null".to_string()) {
                label_filters.push(format!("awl.labels IS NULL"));
            }

            if asset_labels.len() > 0 {
                let labels_filter = format!(
                    "awl.labels && ARRAY[{}]",
                    asset_labels
                        .iter()
                        .map(|label| format!("'{}'", label))
                        .join(",")
                );
                label_filters.push(labels_filter);
            }

            conditions.push(format!("({})", label_filters.join(" OR ")));
        }

        if let Some(smart) = params.smart {
            conditions.push(format!("a.smart = {}", smart));
        }

        if let Some(issuer_in) = params.issuer_in {
            conditions.push(format!(
                "a.issuer = ANY(ARRAY[{}])",
                issuer_in.iter().map(|addr| format!("'{}'", addr)).join(",")
            ));
        }

        let assets_cte_query = if let Some(search) = params.search.as_ref() {
            let search = utils::pg_escape(search);
            let min_block_uid_subquery =
                "SELECT min(block_uid) AS block_uid FROM assets WHERE id = a.id";

            let search_escaped_for_like = utils::escape_for_like(&search);

            let search_by_id_query = format!("SELECT a.id, a.smart, ({}) as block_uid, CASE WHEN ast.ticker IS NULL THEN 128 ELSE 256 END AS rank FROM assets AS a LEFT JOIN asset_tickers AS ast ON ast.asset_id = a.id and ast.superseded_by = {} WHERE a.superseded_by = {} AND a.nft = {} AND a.id ILIKE '{}'", min_block_uid_subquery, MAX_UID, MAX_UID, false, search_escaped_for_like);
            // UNION
            let search_by_meta_query = format!("SELECT id, false AS smart, block_uid, ts_rank(to_tsvector('simple', name), plainto_tsquery('simple', '{}'), 3) * CASE WHEN ticker IS NULL THEN 64 ELSE 128 END AS rank FROM asset_metadatas WHERE name ILIKE '{}%'", search, search_escaped_for_like);
            // UNION
            let search_by_ticker_query = format!("SELECT a.id, a.smart, ({}) as block_uid, 32 AS rank FROM assets AS a LEFT JOIN asset_tickers AS ast ON a.id = ast.asset_id and ast.superseded_by = {} WHERE a.superseded_by = {} AND a.nft = {} AND ast.ticker ILIKE '{}%'", min_block_uid_subquery, MAX_UID, MAX_UID, false, search_escaped_for_like);
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
            let search_by_tsquery_query = format!("SELECT a.id, a.smart, ({}) as block_uid, ts_rank(to_tsvector('simple', a.name), plainto_tsquery('simple', '{}'), 3) * CASE WHEN ast.ticker IS NULL THEN 16 ELSE 32 END AS rank FROM assets a LEFT JOIN asset_tickers AS ast ON ast.asset_id = a.id and ast.superseded_by = {} WHERE a.superseded_by = {} AND a.nft = {} AND {}", min_block_uid_subquery, search, MAX_UID, MAX_UID, false, tsquery_condition);
            // UNION
            let search_by_name_query = format!("SELECT a.id, a.smart, ({}) as block_uid, ts_rank(to_tsvector('simple', a.name), plainto_tsquery('simple', '{}'), 3) * CASE WHEN ast.ticker IS NULL THEN 16 ELSE 32 END AS rank FROM assets a LEFT JOIN asset_tickers AS ast ON ast.asset_id = a.id and ast.superseded_by = {} WHERE a.superseded_by = {} AND a.nft = {} AND a.name ILIKE '{}%'", min_block_uid_subquery, search, MAX_UID, MAX_UID, false, search_escaped_for_like);

            let search_query = vec![
                search_by_id_query,
                search_by_meta_query,
                search_by_ticker_query,
                search_by_tsquery_query,
                search_by_name_query,
            ]
            .join("\n UNION \n");

            let conditions = if conditions.len() > 0 {
                format!("WHERE {}", conditions.iter().join(" AND "))
            } else {
                "".to_owned()
            };

            format!(
                "SELECT DISTINCT ON (search.id)
                    search.id,
                    ROW_NUMBER() OVER (ORDER BY search.rank DESC, search.block_uid ASC, search.id ASC) AS rn
                FROM
                    ({}) AS search
                LEFT JOIN assets AS a ON a.id = search.id AND a.superseded_by = {}
                LEFT JOIN (
                    SELECT asset_id, ARRAY_AGG(DISTINCT labels_list) AS labels
                    FROM (
                        SELECT al.asset_id as asset_id, al.labels
                        FROM asset_labels AS al
                        WHERE al.superseded_by = {}
                        UNION
                        SELECT awl.asset_id as asset_id, ARRAY_AGG(awl.label) as labels
                        FROM asset_wx_labels AS awl
                        GROUP BY awl.asset_id
                    ) AS data, UNNEST(labels) AS labels_list
                    GROUP BY asset_id
                ) AS awl ON awl.asset_id = search.id
                {}
                ORDER BY search.id ASC, search.rank DESC",
                search_query,
                MAX_UID,
                MAX_UID,
                conditions
            )
        } else {
            // search by ticker only if there is not searching by text
            if let Some(ticker) = params.ticker.as_ref() {
                match ticker {
                    TickerFilter::One(ticker) => {
                        let ticker = utils::pg_escape(ticker);
                        conditions.push(format!("ast.ticker = '{}'", ticker));
                    }
                    TickerFilter::Any => {
                        conditions.push(format!("ast.ticker IS NOT NULL"));
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
                    (SELECT a.id, a.smart, (SELECT min(a1.block_uid) FROM assets a1 WHERE a1.id = a.id) AS block_uid, a.issuer FROM assets AS a WHERE a.superseded_by = {} AND a.nft = {}) AS a
                LEFT JOIN asset_tickers AS ast ON ast.asset_id = a.id and ast.superseded_by = {}
                LEFT JOIN (
                    SELECT asset_id, ARRAY_AGG(DISTINCT labels_list) AS labels
                    FROM (
                        SELECT al.asset_id as asset_id, al.labels
                        FROM asset_labels AS al
                        WHERE al.superseded_by = {}
                        UNION
                        SELECT awl.asset_id as asset_id, ARRAY_AGG(awl.label) as labels
                        FROM asset_wx_labels AS awl
                        GROUP BY awl.asset_id
                    ) AS data, UNNEST(labels) AS labels_list
                    GROUP BY asset_id
                ) AS awl ON awl.asset_id = a.id
                {}
                ORDER BY a.block_uid ASC",
                MAX_UID,
                false,
                MAX_UID,
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

        let sql = format!("{} ORDER BY a.rn LIMIT $1", query);

        println!("sql: {sql}");

        let q = sql_query(sql).bind::<Integer, _>(params.limit as i32);

        q.load(&self.pg_pool.get()?).map_err(|e| {
            error!("{:?}", e);
            AppError::from(e)
        })
    }

    fn get(&self, id: &str) -> Result<Option<Asset>, AppError> {
        let q = sql_query(&format!(
            "{} WHERE a.uid = (SELECT DISTINCT ON (a.id) a.uid FROM assets a WHERE a.nft = false AND a.superseded_by = $1 AND a.id = $2 ORDER BY a.id, a.uid DESC LIMIT 1)",
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
            "{} WHERE a.uid IN (SELECT DISTINCT ON (a.id) a.uid FROM assets a WHERE a.nft = false AND a.superseded_by = $1 AND a.id = ANY($2) ORDER BY a.id, a.uid DESC)",
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
            {} WHERE a.uid IN (SELECT DISTINCT ON (a.id) a.uid FROM assets a WHERE a.nft = false AND a.id = ANY($1) AND a.block_uid <= (SELECT uid FROM blocks_microblocks WHERE height = $2 LIMIT 1) ORDER BY a.id, a.uid DESC)", ASSETS_BLOCKCHAIN_DATA_BASE_SQL_QUERY.as_str()))
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

    fn get_asset_user_defined_data(&self, asset_id: &str) -> Result<UserDefinedData, AppError> {
        let q = sql_query(&format!(
            "{} WHERE a.id = $1 AND a.superseded_by = $2 LIMIT 1",
            generate_assets_user_defined_data_base_sql_query()
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
    ) -> Result<Vec<UserDefinedData>, AppError> {
        let q = sql_query(&format!(
            "{} WHERE a.id = ANY(ARRAY[$1]) AND a.superseded_by = $2",
            generate_assets_user_defined_data_base_sql_query()
        ))
        .bind::<Array<Text>, _>(asset_ids)
        .bind::<BigInt, _>(MAX_UID);

        q.load(&self.pg_pool.get()?).map_err(|e| {
            error!("{:?}", e);
            AppError::from(e)
        })
    }

    fn all_assets_user_defined_data(&self) -> Result<Vec<UserDefinedData>, AppError> {
        let q = sql_query(&format!(
            "{} WHERE a.superseded_by = $1",
            generate_assets_user_defined_data_base_sql_query()
        ))
        .bind::<BigInt, _>(MAX_UID);

        q.load(&self.pg_pool.get()?).map_err(|e| {
            error!("{:?}", e);
            AppError::from(e)
        })
    }
}

fn generate_assets_user_defined_data_base_sql_query() -> String {
    format!(
        "SELECT 
        a.id as asset_id,
        ast.ticker,
        COALESCE(awl.labels, ARRAY[]::text[])  AS labels
        FROM assets a
        LEFT JOIN asset_tickers ast ON a.id = ast.asset_id and ast.superseded_by = {}
        LEFT JOIN (
            SELECT asset_id, ARRAY_AGG(DISTINCT labels_list) AS labels
            FROM (
                SELECT al.asset_id as asset_id, al.labels
                FROM asset_labels AS al
                WHERE al.superseded_by = {}
                UNION
                SELECT awl.asset_id as asset_id, ARRAY_AGG(awl.label) as labels
                FROM asset_wx_labels AS awl
                GROUP BY awl.asset_id
            ) AS data, UNNEST(labels) AS labels_list
            GROUP BY asset_id
        ) AS awl ON awl.asset_id = a.id
    ",
        MAX_UID, MAX_UID
    )
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

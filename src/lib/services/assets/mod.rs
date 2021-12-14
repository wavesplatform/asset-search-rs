pub mod dtos;
pub mod entities;
pub mod repo;

use itertools::Itertools;
use std::{collections::HashMap, convert::TryFrom};
use wavesexchange_log::trace;

pub use self::dtos::SearchRequest;
use crate::cache;
use crate::cache::{AssetBlockchainData, AssetUserDefinedData};
use crate::error::Error as AppError;
use crate::models::AssetInfo;

use repo::{FindParams, TickerFilter};

pub trait Service {
    fn get(&self, id: &str) -> Result<Option<AssetInfo>, AppError>;

    fn mget(&self, ids: &[&str], height: Option<i32>) -> Result<Vec<Option<AssetInfo>>, AppError>;

    fn search(&self, req: &SearchRequest) -> Result<Vec<String>, AppError>;
}

pub struct AssetsService {
    repo: Box<dyn repo::Repo + Send + Sync>,
    asset_blockhaind_data_cache: Box<dyn cache::SyncReadCache<AssetBlockchainData> + Send + Sync>,
    asset_user_defined_data_cache:
        Box<dyn cache::SyncReadCache<AssetUserDefinedData> + Send + Sync>,
    oracle_address: String,
}

impl AssetsService {
    pub fn new(
        repo: Box<dyn repo::Repo + Send + Sync>,
        asset_blockhaind_data_cache: Box<
            dyn cache::SyncReadCache<AssetBlockchainData> + Send + Sync,
        >,
        asset_user_defined_data_cache: Box<
            dyn cache::SyncReadCache<AssetUserDefinedData> + Send + Sync,
        >,
        oracle_address: &str,
    ) -> Self {
        Self {
            repo,
            asset_blockhaind_data_cache,
            asset_user_defined_data_cache,
            oracle_address: oracle_address.to_owned(),
        }
    }
}

impl Service for AssetsService {
    fn get(&self, id: &str) -> Result<Option<AssetInfo>, AppError> {
        // fetch asset blockchain data
        //   if is some -> return cached
        //   else -> go to pg
        // fetch asset user defined data
        //   if is some -> return cached
        //   else -> go to pg
        let cached_asset = self.asset_blockhaind_data_cache.get(id)?;

        let asset_blockchain_data = if let Some(cached) = cached_asset {
            Some(cached)
        } else {
            let not_cached_asset = self.repo.get(&id)?;

            let asset_oracles_data = self.repo.data_entries(&[id], &self.oracle_address)?;

            let asset_oracles_data =
                asset_oracles_data
                    .into_iter()
                    .fold(HashMap::new(), |mut acc, cur| {
                        let asset_oracle_data =
                            acc.entry(cur.oracle_address.clone()).or_insert(vec![]);
                        asset_oracle_data.push(cur);
                        acc
                    });

            let not_cached_asset_with_oracles_data = match not_cached_asset {
                Some(a) => {
                    let abd = AssetBlockchainData::try_from((&a, &asset_oracles_data))?;
                    Some(abd)
                }
                _ => None,
            };

            not_cached_asset_with_oracles_data
        };

        if let Some(asset_blockchain_data) = asset_blockchain_data {
            let cached_asset_user_defined_data = self.asset_user_defined_data_cache.get(id)?;

            let asset_user_defined_data = if let Some(cached) = cached_asset_user_defined_data {
                cached
            } else {
                let data = self
                    .repo
                    .get_asset_user_defined_data(&id, &self.oracle_address)?;
                AssetUserDefinedData::from(&data)
            };

            let asset_info = AssetInfo::from((&asset_blockchain_data, &asset_user_defined_data));

            Ok(Some(asset_info))
        } else {
            Ok(None)
        }
    }

    fn mget(&self, ids: &[&str], height: Option<i32>) -> Result<Vec<Option<AssetInfo>>, AppError> {
        match height {
            Some(height) => {
                let assets = self.repo.mget_for_height(ids, height)?;

                let asset_oracles_data = self.repo.data_entries(&ids, &self.oracle_address)?;

                let assets_oracles_data =
                    asset_oracles_data
                        .into_iter()
                        .fold(HashMap::new(), |mut acc, cur| {
                            let asset_data =
                                acc.entry(cur.asset_id.clone()).or_insert(HashMap::new());
                            let asset_oracle_data = asset_data
                                .entry(cur.oracle_address.clone())
                                .or_insert(vec![]);
                            asset_oracle_data.push(cur);
                            acc
                        });

                let assets_user_defined_data = self
                    .repo
                    .mget_asset_user_defined_data(&ids, &self.oracle_address)?;

                let assets_user_defined_data =
                    assets_user_defined_data
                        .into_iter()
                        .fold(HashMap::new(), |mut acc, cur| {
                            acc.insert(cur.asset_id.clone(), cur);
                            acc
                        });

                let assets_info = assets
                    .into_iter()
                    .map(|o| match o {
                        Some(a) => {
                            let asset_oracles_data =
                                assets_oracles_data.get(&a.id).cloned().unwrap_or_default();

                            let asset_blockchain_data =
                                AssetBlockchainData::try_from((&a, &asset_oracles_data))
                                    .map(|ai| ai)?;

                            // user defined data exists for all existing assets
                            // therefore unwrap-safety is guaranteed
                            let asset_user_defined_data =
                                assets_user_defined_data.get(&a.id).unwrap();

                            let asset_user_defined_data =
                                AssetUserDefinedData::from(asset_user_defined_data);

                            let ai =
                                AssetInfo::from((&asset_blockchain_data, &asset_user_defined_data));

                            Ok(Some(ai))
                        }
                        _ => Ok(None),
                    })
                    .collect::<Result<Vec<_>, AppError>>()?;

                Ok(assets_info)
            }
            None => {
                trace!("fetch assets from cache"; "ids" => format!("{:?}", ids));

                let cached_assets = self.asset_blockhaind_data_cache.mget(ids)?;

                let not_cached_asset_ids = cached_assets
                    .iter()
                    .zip(ids)
                    .filter_map(|(m, id)| {
                        if m.is_some() {
                            None
                        } else {
                            Some(id.to_owned())
                        }
                    })
                    .collect_vec();

                let assets_blockchain_data = if not_cached_asset_ids.len() > 0 {
                    let assets = self.repo.mget(&not_cached_asset_ids)?;

                    let asset_oracles_data = self
                        .repo
                        .data_entries(&not_cached_asset_ids, &self.oracle_address)?;

                    // AssetId -> OracleAddress -> Vec<DataEntry>
                    let assets_oracles_data =
                        asset_oracles_data
                            .into_iter()
                            .fold(HashMap::new(), |mut acc, cur| {
                                let asset_data =
                                    acc.entry(cur.asset_id.clone()).or_insert(HashMap::new());

                                let asset_oracle_data = asset_data
                                    .entry(cur.oracle_address.clone())
                                    .or_insert(vec![]);

                                asset_oracle_data.push(cur);
                                acc
                            });

                    let assets_blockchain_data = assets
                        .into_iter()
                        .map(|o| match o {
                            Some(a) => {
                                let asset_id = a.id.clone();
                                let asset_oracles_data = assets_oracles_data
                                    .get(&asset_id)
                                    .cloned()
                                    .unwrap_or_default();

                                let asset_blockchain_data =
                                    AssetBlockchainData::try_from((&a, &asset_oracles_data))?;

                                Ok(Some(asset_blockchain_data))
                            }
                            _ => Ok(None),
                        })
                        .collect::<Result<Vec<_>, AppError>>()?;

                    cached_assets
                        .into_iter()
                        .chain(assets_blockchain_data.into_iter())
                        .collect_vec()
                } else {
                    cached_assets
                };

                let cached_assets_user_defined_data =
                    self.asset_user_defined_data_cache.mget(ids)?;

                let not_cached_asset_user_defined_data_ids = cached_assets_user_defined_data
                    .iter()
                    .zip(ids)
                    .filter_map(|(m, id)| {
                        if m.is_some() {
                            None
                        } else {
                            Some(id.to_owned())
                        }
                    })
                    .collect_vec();

                let assets_user_defined_data = if not_cached_asset_user_defined_data_ids.len() > 0 {
                    let assets_user_defined_data = self
                        .repo
                        .mget_asset_user_defined_data(&ids, &self.oracle_address)?;

                    cached_assets_user_defined_data
                        .into_iter()
                        .filter_map(|o| o)
                        .chain(
                            assets_user_defined_data
                                .into_iter()
                                .map(|udd| AssetUserDefinedData::from(&udd)),
                        )
                        .fold(HashMap::new(), |mut acc, cur| {
                            acc.insert(cur.asset_id.clone(), cur);
                            acc
                        })
                } else {
                    cached_assets_user_defined_data
                        .into_iter()
                        .filter_map(|o| o)
                        .fold(HashMap::new(), |mut acc, cur| {
                            acc.insert(cur.asset_id.clone(), cur);
                            acc
                        })
                };

                let assets =
                    assets_blockchain_data
                        .into_iter()
                        .fold(HashMap::new(), |mut acc, cur| {
                            if let Some(abd) = cur {
                                let asset_user_defined_data =
                                    assets_user_defined_data.get(&abd.id).unwrap();
                                let asset_info = AssetInfo::from((&abd, asset_user_defined_data));
                                acc.insert(abd.id.clone(), asset_info);
                            }
                            acc
                        });

                Ok(ids.iter().map(|id| assets.get(*id).cloned()).collect_vec())
            }
        }
    }

    fn search(&self, req: &SearchRequest) -> Result<Vec<String>, AppError> {
        let find_params = FindParams {
            search: req.search.clone(),
            ticker: req.ticker.as_ref().map(|ticker| {
                if ticker.as_str() == "*" {
                    TickerFilter::Any
                } else {
                    TickerFilter::One(ticker.to_owned())
                }
            }),
            smart: req.smart,
            verification_status_in: req.verification_status_in.clone(),
            asset_label_in: req.asset_label_in.clone(),
            after: req.after.clone(),
            limit: req.limit,
        };

        self.repo.find(find_params).map(|asset_ids| {
            asset_ids
                .iter()
                .map(|asset_id| asset_id.id.to_owned())
                .collect()
        })
    }
}

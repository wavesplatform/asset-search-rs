pub mod dtos;
pub mod entities;
pub mod repo;

use itertools::Itertools;
use std::collections::HashMap;
use std::sync::Arc;
use wavesexchange_log::timer;

pub use self::dtos::SearchRequest;
use crate::cache;
use crate::cache::{AssetBlockchainData, AssetUserDefinedData};
use crate::error::Error as AppError;
use crate::models::AssetInfo;

use entities::UserDefinedData;
use repo::{FindParams, TickerFilter};

#[derive(Clone, Debug, Default)]
pub struct GetOptions {
    bypass_cache: bool,
}

#[derive(Clone, Debug, Default)]
pub struct MgetOptions {
    height: Option<i32>,
    bypass_cache: bool,
}

impl MgetOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_height(&self, height: i32) -> Self {
        let mut opts = self.clone();
        opts.height = Some(height);
        opts
    }

    pub fn set_bypass_cache(&self, bypass_cache: bool) -> Self {
        let mut opts = self.clone();
        opts.bypass_cache = bypass_cache;
        opts
    }

    pub fn with_height(height: i32) -> Self {
        Self::default().set_height(height)
    }

    pub fn with_bypass_cache(bypass_cache: bool) -> Self {
        Self::default().set_bypass_cache(bypass_cache)
    }
}

#[async_trait::async_trait]
pub trait Service {
    async fn get(&self, id: &str, opts: &GetOptions) -> Result<Option<AssetInfo>, AppError>;

    async fn mget(
        &self,
        ids: &[&str],
        opts: &MgetOptions,
    ) -> Result<Vec<Option<AssetInfo>>, AppError>;

    fn search(&self, req: &SearchRequest) -> Result<Vec<String>, AppError>;

    fn user_defined_data(&self) -> Result<Vec<UserDefinedData>, AppError>;
}

pub struct AssetsService {
    repo: Arc<dyn repo::Repo + Send + Sync>,
    asset_blockhaind_data_cache: Box<dyn cache::AsyncReadCache<AssetBlockchainData> + Send + Sync>,
    asset_user_defined_data_cache:
        Box<dyn cache::AsyncReadCache<AssetUserDefinedData> + Send + Sync>,
    waves_association_address: String,
}

impl AssetsService {
    pub fn new(
        repo: Arc<dyn repo::Repo + Send + Sync>,
        asset_blockhaind_data_cache: Box<
            dyn cache::AsyncReadCache<AssetBlockchainData> + Send + Sync,
        >,
        asset_user_defined_data_cache: Box<
            dyn cache::AsyncReadCache<AssetUserDefinedData> + Send + Sync,
        >,
        waves_association_address: &str,
    ) -> Self {
        Self {
            repo,
            asset_blockhaind_data_cache,
            asset_user_defined_data_cache,
            waves_association_address: waves_association_address.to_owned(),
        }
    }
}

#[async_trait::async_trait]
impl Service for AssetsService {
    async fn get(&self, id: &str, opts: &GetOptions) -> Result<Option<AssetInfo>, AppError> {
        // fetch asset blockchain data
        //   if is some -> return cached
        //   else -> go to pg
        // fetch asset user defined data
        //   if is some -> return cached
        //   else -> go to pg

        let cached_asset = if opts.bypass_cache {
            None
        } else {
            self.asset_blockhaind_data_cache.get(id).await?
        };

        let asset_blockchain_data = if let Some(cached) = cached_asset {
            Some(cached)
        } else {
            let not_cached_asset = self.repo.get(&id)?;

            let asset_oracles_data = self
                .repo
                .data_entries(&[id], &self.waves_association_address)?;

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
                    let abd = AssetBlockchainData::try_from_asset_and_oracles_data(
                        &a,
                        &asset_oracles_data,
                    )?;
                    Some(abd)
                }
                _ => None,
            };

            not_cached_asset_with_oracles_data
        };

        if let Some(asset_blockchain_data) = asset_blockchain_data {
            let cached_asset_user_defined_data = if opts.bypass_cache {
                None
            } else {
                self.asset_user_defined_data_cache.get(id).await?
            };

            let asset_user_defined_data = if let Some(cached) = cached_asset_user_defined_data {
                cached
            } else {
                let data = self.repo.get_asset_user_defined_data(&id)?;
                AssetUserDefinedData::from(&data)
            };

            let asset_info = AssetInfo::from((&asset_blockchain_data, &asset_user_defined_data));

            Ok(Some(asset_info))
        } else {
            Ok(None)
        }
    }

    async fn mget(
        &self,
        ids: &[&str],
        opts: &MgetOptions,
    ) -> Result<Vec<Option<AssetInfo>>, AppError> {
        dbg!("AssetsService:mget");

        let assets = match opts.height {
            Some(height) => {
                let assets = {
                    timer!("assets_service::mget::mget_for_height");
                    self.repo.mget_for_height(ids, height)?
                };

                let asset_oracles_data = {
                    timer!("assets_service::mget::data_entries");
                    self.repo
                        .data_entries(&ids, &self.waves_association_address)?
                };

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

                let assets_user_defined_data = {
                    timer!("assets_service::mget::mget_asset_user_defined_data");
                    self.repo.mget_asset_user_defined_data(&ids)?
                };

                let assets_user_defined_data =
                    assets_user_defined_data
                        .into_iter()
                        .fold(HashMap::new(), |mut acc, cur| {
                            acc.insert(cur.asset_id.clone(), cur);
                            acc
                        });

                let assets = assets.into_iter().try_fold::<_, _, Result<_, AppError>>(
                    HashMap::new(),
                    |mut acc, o| {
                        if let Some(a) = o {
                            let asset_oracles_data =
                                assets_oracles_data.get(&a.id).cloned().unwrap_or_default();

                            let asset_blockchain_data =
                                AssetBlockchainData::try_from_asset_and_oracles_data(
                                    &a,
                                    &asset_oracles_data,
                                )?;

                            let asset_user_defined_data =
                                assets_user_defined_data.get(&a.id).unwrap();

                            let asset_user_defined_data =
                                AssetUserDefinedData::from(asset_user_defined_data);

                            let ai =
                                AssetInfo::from((&asset_blockchain_data, &asset_user_defined_data));

                            acc.insert(a.id, ai);
                        }
                        Ok(acc)
                    },
                )?;

                ids.iter()
                    .map(|id| assets.get(*id).cloned())
                    .collect::<Vec<Option<_>>>()
            }
            None => {
                let cached_assets = if opts.bypass_cache {
                    vec![None; ids.len()]
                } else {
                    self.asset_blockhaind_data_cache.mget(ids).await?
                };

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
                        .data_entries(&not_cached_asset_ids, &self.waves_association_address)?;

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
                                    AssetBlockchainData::try_from_asset_and_oracles_data(
                                        &a,
                                        &asset_oracles_data,
                                    )?;

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

                let cached_assets_user_defined_data = if opts.bypass_cache {
                    vec![None; ids.len()]
                } else {
                    self.asset_user_defined_data_cache.mget(ids).await?
                };

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
                    let assets_user_defined_data = self.repo.mget_asset_user_defined_data(&ids)?;

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

                ids.iter()
                    .map(|id| assets.get(*id).cloned())
                    .collect::<Vec<Option<_>>>()
            }
        };

        // not found assets should be returned as nulls
        let nft_filtered_assets = assets
            .into_iter()
            .map(|o| o.and_then(|ai| if ai.asset.nft { None } else { Some(ai) }))
            .collect::<Vec<_>>();

        Ok(nft_filtered_assets)
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
            asset_label_in: req.asset_label_in.clone(),
            issuer_in: req.issuer_in.clone(),
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

    fn user_defined_data(&self) -> Result<Vec<UserDefinedData>, AppError> {
        self.repo.all_assets_user_defined_data()
    }
}

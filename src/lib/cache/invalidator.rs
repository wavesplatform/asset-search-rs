use anyhow::Result;
use futures::{stream, StreamExt};
use std::sync::Arc;
use wavesexchange_log::{debug, info, timer};

use super::{AssetBlockchainData, AssetUserDefinedData, AsyncWriteCache, InvalidateCacheMode};
use crate::services::assets::{MgetOptions, SearchRequest, Service};

const REDIS_CONCURRENCY_LIMIT: usize = 10;

pub async fn run<S, BDC, UDDC>(
    assets_service: Arc<S>,
    assets_blockchain_data_cache: Arc<BDC>,
    assets_user_defined_data_cache: Arc<UDDC>,
    invalidate_cache_mode: &InvalidateCacheMode,
) -> Result<()>
where
    S: Service,
    BDC: AsyncWriteCache<AssetBlockchainData>,
    UDDC: AsyncWriteCache<AssetUserDefinedData>,
{
    timer!("cache invalidating");

    if *invalidate_cache_mode == InvalidateCacheMode::AllData
        || *invalidate_cache_mode == InvalidateCacheMode::BlockchainData
    {
        info!("starting assets blockchain data cache invalidation");

        const REQUEST_LIMIT: u32 = 1000;

        let mut all_assets_blockchain_data = vec![];
        let mut req = SearchRequest::default().with_limit(REQUEST_LIMIT);

        loop {
            timer!("fetching assets from the assets service");
            let assets_blockchain_data_ids = assets_service.search(&req)?;
            let assets_blockchain_data_ids = assets_blockchain_data_ids
                .iter()
                .map(|s| s.as_str())
                .collect::<Vec<_>>();

            let mut assets_blockchain_data = assets_service
                .mget(
                    &assets_blockchain_data_ids,
                    &MgetOptions::with_bypass_cache(true),
                )
                .await?
                .into_iter()
                .filter_map(|o| o)
                .collect::<Vec<_>>();

            all_assets_blockchain_data.append(&mut assets_blockchain_data);

            if assets_blockchain_data_ids.len() as u32 >= REQUEST_LIMIT {
                let last = assets_blockchain_data_ids.last().cloned().unwrap();
                req = req.with_after(last.to_owned());
            } else {
                break;
            }
        }

        {
            timer!("invalidating assets blockchain data cache");

            debug!("clearing cache");
            assets_blockchain_data_cache.clear().await?;

            debug!("setting new cache"; "assets count" => all_assets_blockchain_data.len());
            stream::iter(all_assets_blockchain_data)
                .for_each_concurrent(REDIS_CONCURRENCY_LIMIT, |asset_info| {
                    let cache = assets_blockchain_data_cache.clone();
                    async move {
                        let a = AssetBlockchainData::from(&asset_info);
                        cache.set(a.id.clone(), a).await.unwrap()
                    }
                })
                .await;
        }

        info!("cache succcessfully invalidated");
    }

    if *invalidate_cache_mode == InvalidateCacheMode::AllData
        || *invalidate_cache_mode == InvalidateCacheMode::UserDefinedData
    {
        info!("starting assets user defined data cache invalidation");

        let assets_user_defined_data = assets_service.user_defined_data()?;

        debug!("clearing cache");
        assets_user_defined_data_cache.clear().await?;

        debug!("setting new cache"; "assets_user_defined_data count" => assets_user_defined_data.len());

        stream::iter(&assets_user_defined_data)
            .for_each_concurrent(REDIS_CONCURRENCY_LIMIT, |asset_user_defined_data| {
                let cache = assets_user_defined_data_cache.clone();
                async move {
                    let asset_user_defined_data =
                        AssetUserDefinedData::from(asset_user_defined_data);
                    cache
                        .set(
                            asset_user_defined_data.asset_id.clone(),
                            asset_user_defined_data.clone(),
                        )
                        .await
                        .unwrap();
                }
            })
            .await;
    }

    Ok(())
}

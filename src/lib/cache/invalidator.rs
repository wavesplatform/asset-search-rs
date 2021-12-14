use anyhow::Result;
use std::sync::Arc;
use wavesexchange_log::{debug, info, timer};

use super::{AssetBlockchainData, AssetUserDefinedData, InvalidateCacheMode, SyncWriteCache};
use crate::services::assets::{MgetOptions, SearchRequest, Service};

pub fn run<S, BDC, UDDC>(
    assets_service: Arc<S>,
    assets_blockchain_data_cache: Arc<BDC>,
    assets_user_defined_data_redis_cache: Arc<UDDC>,
    invalidate_cache_mode: &InvalidateCacheMode,
) -> Result<()>
where
    S: Service,
    BDC: SyncWriteCache<AssetBlockchainData>,
    UDDC: SyncWriteCache<AssetUserDefinedData>,
{
    timer!("cache invalidating");

    if *invalidate_cache_mode == InvalidateCacheMode::AllData
        || *invalidate_cache_mode == InvalidateCacheMode::UserDefinedData
    {
        info!("starting assets user defined data cache invalidation");

        let assets_user_defined_data = assets_service.user_defined_data()?;

        assets_user_defined_data
            .iter()
            .try_for_each(|asset_user_defined_data| {
                let asset_user_defined_data = AssetUserDefinedData::from(asset_user_defined_data);
                assets_user_defined_data_redis_cache.set(
                    &asset_user_defined_data.asset_id.clone(),
                    asset_user_defined_data,
                )
            })?;
    }

    if *invalidate_cache_mode == InvalidateCacheMode::AllData
        || *invalidate_cache_mode == InvalidateCacheMode::BlockchainData
    {
        info!("starting assets blockchain data cache invalidation");

        const REQUEST_LIMIT: u32 = 1000;

        let mut all_assets_blockchain_data = vec![];
        let mut req = SearchRequest {
            ids: None,
            ticker: None,
            search: None,
            smart: None,
            verification_status_in: None,
            asset_label_in: None,
            limit: REQUEST_LIMIT,
            after: None,
        };

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
                )?
                .into_iter()
                .filter_map(|o| o)
                .collect::<Vec<_>>();

            all_assets_blockchain_data.append(&mut assets_blockchain_data);

            if assets_blockchain_data_ids.len() as u32 >= REQUEST_LIMIT {
                let last = assets_blockchain_data_ids
                    .last()
                    .cloned()
                    .unwrap()
                    .to_owned();
                req = req.with_after(last);
            } else {
                break;
            }
        }

        debug!("assets fetched"; "assets_count" => all_assets_blockchain_data.len());

        {
            timer!("invalidating assets blockchain data cache");
            all_assets_blockchain_data
                .iter()
                .try_for_each(|asset_info| {
                    let a = AssetBlockchainData::from(asset_info);
                    assets_blockchain_data_cache.set(&a.id.clone(), a)
                })?;
        }

        info!("cache succcessfully invalidated");
    }

    Ok(())
}

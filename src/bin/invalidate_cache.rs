use anyhow::Result;

use app_lib::{
    cache::{
        self, AssetBlockchainData, AssetUserDefinedData, SyncWriteCache, ASSET_KEY_PREFIX,
        ASSET_USER_DEFINED_DATA_KEY_PREFIX,
    },
    config, db, redis,
    services::assets::{repo::Repo, SearchRequest, Service},
    InvalidateCacheMode,
};
use wavesexchange_log::{debug, info, timer};

#[tokio::main]
async fn main() -> Result<()> {
    let config = config::load_invalidate_cache_config().await?;

    let pg_pool = db::pool(&config.postgres)?;
    let redis_pool = redis::pool(&config.redis)?;

    let pg_repo = app_lib::services::assets::repo::pg::PgRepo::new(pg_pool);

    let assets_user_defined_data_redis_cache = cache::redis::new(
        redis_pool.clone(),
        ASSET_USER_DEFINED_DATA_KEY_PREFIX.to_owned(),
    );

    info!(
        "starting cache invalidating, mode={:?}",
        config.app.invalidate_cache_mode
    );

    timer!("cache invalidating");

    if config.app.invalidate_cache_mode == InvalidateCacheMode::AllData
        || config.app.invalidate_cache_mode == InvalidateCacheMode::UserDefinedData
    {
        info!("starting assets user defined data cache invalidation");

        let assets_user_defined_data =
            pg_repo.all_assets_user_defined_data(&config.app.waves_association_address)?;

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

    if config.app.invalidate_cache_mode == InvalidateCacheMode::AllData
        || config.app.invalidate_cache_mode == InvalidateCacheMode::BlockchainData
    {
        info!("starting assets blockchain data cache invalidation");

        let assets_blockchain_data_cache =
            cache::redis::new(redis_pool, ASSET_KEY_PREFIX.to_owned());

        let assets_service = app_lib::services::assets::AssetsService::new(
            Box::new(pg_repo),
            Box::new(assets_blockchain_data_cache.clone()),
            Box::new(assets_user_defined_data_redis_cache),
            &config.app.waves_association_address,
        );

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
                .mget(&assets_blockchain_data_ids, None)?
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

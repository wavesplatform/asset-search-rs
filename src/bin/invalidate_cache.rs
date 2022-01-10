use std::sync::Arc;

use anyhow::Result;

use app_lib::{
    cache::{self, ASSET_KEY_PREFIX, ASSET_USER_DEFINED_DATA_KEY_PREFIX, KEY_SEPARATOR},
    config, db, redis,
};
use wavesexchange_log::info;

#[tokio::main]
async fn main() -> Result<()> {
    let config = config::load_invalidate_cache_config().await?;

    let pg_pool = db::pool(&config.postgres)?;
    let redis_pool = redis::pool(&config.redis)?;

    let pg_repo = {
        let r = app_lib::services::assets::repo::pg::PgRepo::new(pg_pool.clone());
        Arc::new(r)
    };

    let assets_blockchain_data_cache =
        cache::redis::new(redis_pool.clone(), ASSET_KEY_PREFIX, KEY_SEPARATOR);

    let assets_user_defined_data_redis_cache = cache::redis::new(
        redis_pool.clone(),
        ASSET_USER_DEFINED_DATA_KEY_PREFIX,
        KEY_SEPARATOR,
    );

    info!(
        "starting cache invalidating, mode={:?}",
        config.app.invalidate_cache_mode
    );

    let assets_service = app_lib::services::assets::AssetsService::new(
        pg_repo.clone(),
        Box::new(assets_blockchain_data_cache.clone()),
        Box::new(assets_user_defined_data_redis_cache.clone()),
        &config.app.waves_association_address,
    );

    cache::invalidator::run(
        Arc::new(assets_service),
        Arc::new(assets_blockchain_data_cache),
        Arc::new(assets_user_defined_data_redis_cache),
        &config.app.invalidate_cache_mode,
    )?;

    Ok(())
}

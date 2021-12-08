use anyhow::Result;

use app_lib::{
    cache::{self, AssetUserDefinedData, SyncWriteCache, ASSET_USER_DEFINED_DATA_KEY_PREFIX},
    config, db, redis,
    services::assets::repo::Repo,
};

#[tokio::main]
async fn main() -> Result<()> {
    let config = config::load_invalidate_cache_config().await?;

    let pg_pool = db::pool(&config.postgres)?;
    let redis_pool = redis::pool(&config.redis)?;

    let pg_repo = app_lib::services::assets::repo::pg::PgRepo::new(pg_pool);

    let assets_user_defined_data_redis_cache =
        cache::redis::new(redis_pool, ASSET_USER_DEFINED_DATA_KEY_PREFIX.to_owned());

    let assets_user_defined_data =
        pg_repo.all_assets_user_defined_data(&config.app.oracle_address)?;

    assets_user_defined_data
        .iter()
        .try_for_each(|asset_user_defined_data| {
            let asset_user_defined_data = AssetUserDefinedData::from(asset_user_defined_data);
            assets_user_defined_data_redis_cache.set(
                &asset_user_defined_data.asset_id.clone(),
                asset_user_defined_data,
            )
        })?;

    Ok(())
}

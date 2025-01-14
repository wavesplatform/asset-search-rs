use anyhow::Result;
use app_lib::{
    cache::{
        self, ASSET_BLOCKCHAIN_DATA_KEY_PREFIX, ASSET_USER_DEFINED_DATA_KEY_PREFIX, KEY_SEPARATOR,
    },
    config, consumer, db, sync_redis,
};
use std::sync::Arc;
use tokio::select;
use wavesexchange_log::{error, info};
use wavesexchange_warp::MetricsWarpBuilder;

#[tokio::main]
async fn main() -> Result<()> {
    let config = config::load_consumer_config().await?;

    info!(
        "Starting asset-search consumer with config: {:?}",
        config.consumer
    );

    let conn = db::unpooled(&config.postgres)?;

    let updates_src = consumer::updates::new(&config.consumer.blockchain_updates_url).await?;

    let pg_repo = Arc::new(consumer::repo::pg::new(conn));

    let redis_pool = sync_redis::pool(&config.redis)?;

    let blockchain_data_cache = cache::sync_redis_cache::new(
        redis_pool.clone(),
        ASSET_BLOCKCHAIN_DATA_KEY_PREFIX,
        KEY_SEPARATOR,
    );
    let user_defined_data_cache = cache::sync_redis_cache::new(
        redis_pool,
        ASSET_USER_DEFINED_DATA_KEY_PREFIX,
        KEY_SEPARATOR,
    );

    let consumer = consumer::start(
        config.consumer.starting_height,
        updates_src,
        pg_repo,
        blockchain_data_cache,
        user_defined_data_cache,
        config.consumer.updates_per_request,
        config.consumer.max_wait_time_in_secs,
        config.consumer.chain_id,
        &config.consumer.waves_association_address,
    );

    let metrics = MetricsWarpBuilder::new()
        .with_metrics_port(config.consumer.metrics_port)
        .run_async();

    select! {
        Err(err) = consumer =>
        {
            error!("{}", err);
            panic!("asset-search consumer panic: {}", err);
        },
        _ = metrics => {
            error!("metrics server stopped")
        }
    }
    Ok(())
}

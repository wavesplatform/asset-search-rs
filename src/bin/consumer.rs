use anyhow::Result;
use app_lib::{
    cache::{self, ASSET_KEY_PREFIX},
    config, consumer, db, redis,
};
use std::sync::Arc;
use wavesexchange_log::{error, info};

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

    let redis_pool = redis::pool(&config.redis)?;
    let redis_cache = cache::redis::new(redis_pool, ASSET_KEY_PREFIX.to_owned());

    if let Err(err) = consumer::start(
        config.consumer.starting_height,
        updates_src,
        pg_repo,
        redis_cache,
        config.consumer.updates_per_request,
        config.consumer.max_wait_time_in_secs,
        config.consumer.chain_id,
        &config.consumer.oracle_addresses,
    )
    .await
    {
        error!("{}", err);
        panic!("asset-search consumer panic: {}", err);
    }
    Ok(())
}
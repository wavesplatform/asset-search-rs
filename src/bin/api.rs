use std::sync::Arc;
use anyhow::Result;
use wavesexchange_log::info;

use app_lib::{
    api::{self},
    api_clients,
    cache::{self, ASSET_BLOCKCHAIN_DATA_KEY_PREFIX, ASSET_USER_DEFINED_DATA_KEY_PREFIX, KEY_SEPARATOR},
    config, db, redis,
};

#[tokio::main]
async fn main() -> Result<()> {
    let config = config::load_api_config().await?;

    let pg_pool = db::pool(&config.postgres)?;
    let redis_pool = redis::pool(&config.redis)?;

    let assets_service = {
        let pg_repo = app_lib::services::assets::repo::pg::PgRepo::new(pg_pool);
        let assets_redis_cache =
            cache::redis::new(redis_pool.clone(), ASSET_BLOCKCHAIN_DATA_KEY_PREFIX, KEY_SEPARATOR);
        let assets_user_defined_data_redis_cache = cache::redis::new(
            redis_pool,
            ASSET_USER_DEFINED_DATA_KEY_PREFIX,
            KEY_SEPARATOR,
        );
        app_lib::services::assets::AssetsService::new(
            Arc::new(pg_repo),
            Box::new(assets_redis_cache),
            Box::new(assets_user_defined_data_redis_cache),
            &config.app.waves_association_address,
        )
    };

    let port = config.api.port;

    if config.api.image_service_bypass {
        info!("Bypassing Images service");
        api::server::start(
            port,
            assets_service,
            app_lib::services::images::dummy::DummyService::new(),
        )
        .await;
    } else {
        let images_service = {
            let images_api_client = api_clients::HttpClient::new(&config.api.image_service_url)?
                .with_user_agent("Asset search Service");
            app_lib::services::images::http::HttpService::new(images_api_client)
        };
        api::server::start(port, assets_service, images_service).await;
    }

    Ok(())
}

use std::sync::Arc;

use anyhow::Result;
use app_lib::{
    admin, api_clients, async_redis,
    cache::{
        self, ASSET_BLOCKCHAIN_DATA_KEY_PREFIX, ASSET_USER_DEFINED_DATA_KEY_PREFIX, KEY_SEPARATOR,
    },
    config, db,
};
use wavesexchange_log::info;

#[tokio::main]
async fn main() -> Result<()> {
    let admin_config = config::load_admin_config().await?;

    let pg_pool = db::pool(&admin_config.postgres)?;
    let redis_pool = async_redis::pool(&admin_config.redis).await?;

    let assets_blockchain_data_cache = cache::async_redis_cache::new(
        redis_pool.clone(),
        ASSET_BLOCKCHAIN_DATA_KEY_PREFIX,
        KEY_SEPARATOR,
    );

    let assets_user_defined_data_redis_cache = cache::async_redis_cache::new(
        redis_pool.clone(),
        ASSET_USER_DEFINED_DATA_KEY_PREFIX,
        KEY_SEPARATOR,
    );

    let assets_service = {
        let pg_repo = app_lib::services::assets::repo::pg::PgRepo::new(pg_pool.clone());

        app_lib::services::assets::AssetsService::new(
            Arc::new(pg_repo),
            Box::new(assets_blockchain_data_cache.clone()),
            Box::new(assets_user_defined_data_redis_cache.clone()),
            &admin_config.app.waves_association_address,
        )
    };

    let admin_assets_service = {
        let pg_repo = app_lib::services::admin_assets::repo::pg::PgRepo::new(pg_pool);
        let redis_cache = cache::async_redis_cache::new(
            redis_pool,
            ASSET_USER_DEFINED_DATA_KEY_PREFIX,
            KEY_SEPARATOR,
        );
        app_lib::services::admin_assets::AdminAssetsService::new(
            Arc::new(pg_repo),
            Box::new(redis_cache),
        )
    };

    let port = admin_config.api.port;
    let api_key = admin_config.admin.api_key.to_owned();

    if admin_config.api.image_service_bypass {
        info!("Bypassing Images service");
        admin::server::start(
            port,
            assets_service,
            app_lib::services::images::dummy::DummyService::new(),
            admin_assets_service,
            assets_blockchain_data_cache,
            assets_user_defined_data_redis_cache,
            api_key.clone(),
        )
        .await;
    } else {
        let images_service = {
            let images_api_client =
                api_clients::HttpClient::new(&admin_config.api.image_service_url)?
                    .with_user_agent("Asset search Service");
            app_lib::services::images::http::HttpService::new(images_api_client)
        };

        admin::server::start(
            port,
            assets_service,
            images_service,
            admin_assets_service,
            assets_blockchain_data_cache,
            assets_user_defined_data_redis_cache,
            api_key.clone(),
        )
        .await;
    }

    Ok(())
}

use anyhow::Result;
use app_lib::{
    admin, api_clients,
    cache::{self, ASSET_KEY_PREFIX, ASSET_USER_DEFINED_DATA_KEY_PREFIX},
    config, db, redis,
};
use wavesexchange_log::info;

#[tokio::main]
async fn main() -> Result<()> {
    let admin_config = config::load_admin_config().await?;

    let pg_pool = db::pool(&admin_config.postgres)?;
    let redis_pool = redis::pool(&admin_config.redis)?;

    let assets_service = {
        let pg_repo = app_lib::services::assets::repo::pg::PgRepo::new(pg_pool.clone());
        let assets_redis_cache = cache::redis::new(redis_pool.clone(), ASSET_KEY_PREFIX.to_owned());
        let assets_user_defined_data_redis_cache = cache::redis::new(
            redis_pool.clone(),
            ASSET_USER_DEFINED_DATA_KEY_PREFIX.to_owned(),
        );
        app_lib::services::assets::AssetsService::new(
            Box::new(pg_repo),
            Box::new(assets_redis_cache),
            Box::new(assets_user_defined_data_redis_cache),
            &admin_config.app.oracle_address,
        )
    };

    let admin_assets_service = {
        let pg_repo = app_lib::services::admin_assets::repo::pg::PgRepo::new(pg_pool);
        let redis_cache =
            cache::redis::new(redis_pool, ASSET_USER_DEFINED_DATA_KEY_PREFIX.to_owned());
        app_lib::services::admin_assets::AdminAssetsService::new(
            Box::new(pg_repo),
            Box::new(redis_cache),
        )
    };

    let port = admin_config.admin.api_port;
    let api_key = admin_config.admin.api_key.to_owned();

    if admin_config.admin.bypass_images_service {
        info!("Bypassing Images service");
        admin::server::start(
            port,
            assets_service,
            app_lib::services::images::dummy::DummyService::new(),
            admin_assets_service,
            api_key.clone(),
        )
        .await;
    } else {
        let images_service = {
            let images_api_client =
                api_clients::HttpClient::new(&admin_config.admin.images_service_base_url)?
                    .with_user_agent("Asset search Service");
            app_lib::services::images::http::HttpService::new(images_api_client)
        };
        admin::server::start(
            port,
            assets_service,
            images_service,
            admin_assets_service,
            api_key,
        )
        .await;
    }

    Ok(())
}
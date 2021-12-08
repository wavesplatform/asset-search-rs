pub mod admin;
pub mod api;
pub mod app;
pub mod cache;
pub mod consumer;
pub mod postgres;
pub mod redis;

use crate::error::Error;

#[derive(Debug, Clone)]
pub struct APIConfig {
    pub api: api::Config,
    pub app: app::Config,
    pub postgres: postgres::Config,
    pub redis: redis::Config,
    pub cache: cache::Config,
}

#[derive(Debug, Clone)]
pub struct AdminConfig {
    pub admin: admin::Config,
    pub app: app::Config,
    pub postgres: postgres::Config,
    pub redis: redis::Config,
    pub cache: cache::Config,
}

#[derive(Debug, Clone)]
pub struct ConsumerConfig {
    pub app: app::Config,
    pub consumer: consumer::Config,
    pub postgres: postgres::Config,
    pub redis: redis::Config,
    pub cache: cache::Config,
}

#[derive(Debug, Clone)]
pub struct MigrationConfig {
    pub postgres: postgres::Config,
}
#[derive(Debug, Clone)]
pub struct InvalidateCacheConfig {
    pub app: app::Config,
    pub postgres: postgres::Config,
    pub redis: redis::Config,
}

pub async fn load_api_config() -> Result<APIConfig, Error> {
    let api_config = api::load()?;
    let app_config = app::load()?;
    let cache_config = cache::load()?;
    let postgres_config = postgres::load()?;
    let redis_config = redis::load()?;

    Ok(APIConfig {
        api: api_config,
        app: app_config,
        cache: cache_config,
        postgres: postgres_config,
        redis: redis_config,
    })
}

pub async fn load_admin_config() -> Result<AdminConfig, Error> {
    let app_config = app::load()?;
    let admin_config = admin::load()?;
    let cache_config = cache::load()?;
    let redis_config = redis::load()?;
    let postgres_config = postgres::load()?;

    Ok(AdminConfig {
        admin: admin_config,
        app: app_config,
        cache: cache_config,
        postgres: postgres_config,
        redis: redis_config,
    })
}

pub async fn load_consumer_config() -> Result<ConsumerConfig, Error> {
    let app_config = app::load()?;
    let cache_config = cache::load()?;
    let consumer_config = consumer::load()?;
    let postgres_config = postgres::load()?;
    let redis_config = redis::load()?;

    Ok(ConsumerConfig {
        app: app_config,
        cache: cache_config,
        consumer: consumer_config,
        postgres: postgres_config,
        redis: redis_config,
    })
}

pub async fn load_invalidate_cache_config() -> Result<InvalidateCacheConfig, Error> {
    let app_config = app::load()?;
    let redis_config = redis::load()?;
    let postgres_config = postgres::load()?;

    Ok(InvalidateCacheConfig {
        app: app_config,
        postgres: postgres_config,
        redis: redis_config,
    })
}

pub fn load_migration_config() -> Result<MigrationConfig, Error> {
    let postgres_config = postgres::load()?;

    Ok(MigrationConfig {
        postgres: postgres_config,
    })
}

pub mod admin;
pub mod api;
pub mod app;
pub mod consumer;
pub mod migration;
pub mod postgres;
pub mod redis;

use crate::error::Error;

#[derive(Debug, Clone)]
pub struct APIConfig {
    pub api: api::Config,
    pub app: app::Config,
    pub postgres: postgres::Config,
    pub redis: redis::Config,
}

#[derive(Debug, Clone)]
pub struct AdminConfig {
    pub admin: admin::Config,
    pub api: api::Config,
    pub app: app::Config,
    pub postgres: postgres::Config,
    pub redis: redis::Config,
}

#[derive(Debug, Clone)]
pub struct ConsumerConfig {
    pub consumer: consumer::Config,
    pub postgres: postgres::Config,
    pub redis: redis::Config,
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
    let postgres_config = postgres::load()?;
    let redis_config = redis::load()?;

    Ok(APIConfig {
        api: api_config,
        app: app_config,
        postgres: postgres_config,
        redis: redis_config,
    })
}

pub async fn load_admin_config() -> Result<AdminConfig, Error> {
    let api_config = api::load()?;
    let app_config = app::load()?;
    let admin_config = admin::load()?;
    let redis_config = redis::load()?;
    let postgres_config = postgres::load()?;

    Ok(AdminConfig {
        admin: admin_config,
        api: api_config,
        app: app_config,
        postgres: postgres_config,
        redis: redis_config,
    })
}

pub async fn load_consumer_config() -> Result<ConsumerConfig, Error> {
    let consumer_config = consumer::load()?;
    let postgres_config = postgres::load()?;
    let redis_config = redis::load()?;

    Ok(ConsumerConfig {
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

pub fn load_migration_config() -> Result<migration::Config, Error> {
    migration::load()
}

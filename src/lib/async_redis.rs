use anyhow::Result;
use bb8_redis::{bb8::Pool, RedisConnectionManager};
use std::time::Duration;

use crate::config::redis::Config;
use crate::error::Error as AppError;

pub type RedisPool = Pool<RedisConnectionManager>;

pub async fn pool(config: &Config) -> Result<RedisPool, AppError> {
    let redis_connection_url = format!(
        "redis://{}:{}@{}:{}",
        config.user, config.password, config.host, config.port
    );

    let manager = RedisConnectionManager::new(redis_connection_url)?;

    Pool::builder()
        .min_idle(Some(1))
        .max_size(config.poolsize as u32)
        .idle_timeout(Some(Duration::from_secs(5 * 60)))
        .connection_timeout(Duration::from_secs(5))
        .build(manager)
        .await
        .map_err(|e| AppError::RedisError(e))
}

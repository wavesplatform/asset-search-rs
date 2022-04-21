use anyhow::Result;
use r2d2::Pool;
use redis::Client;
use std::time::Duration;

use crate::config::redis::Config;
use crate::error::Error as AppError;

pub type RedisPool = Pool<Client>;

pub fn pool(config: &Config) -> Result<RedisPool, AppError> {
    let redis_connection_url = format!(
        "redis://{}:{}@{}:{}",
        config.user, config.password, config.host, config.port
    );

    let redis_client = Client::open(redis_connection_url)?;

    Ok(Pool::builder()
        .min_idle(Some(1))
        .max_size(config.poolsize as u32)
        .idle_timeout(Some(Duration::from_secs(5 * 60)))
        .connection_timeout(Duration::from_secs(5))
        .build(redis_client)?)
}

use serde::Deserialize;

use crate::error::Error;

fn default_redis_port() -> u16 {
    6379
}

fn default_redis_poolsize() -> u32 {
    1
}

#[derive(Deserialize)]
pub struct ConfigFlat {
    pub redis_host: String,
    #[serde(default = "default_redis_port")]
    pub redis_port: u16,
    pub redis_user: String,
    pub redis_password: String,
    #[serde(default = "default_redis_poolsize")]
    pub redis_poolsize: u32,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub poolsize: u32,
}

pub fn load() -> Result<Config, Error> {
    let config_flat = envy::from_env::<ConfigFlat>()?;

    Ok(Config {
        host: config_flat.redis_host,
        port: config_flat.redis_port,
        user: config_flat.redis_user,
        password: config_flat.redis_password,
        poolsize: config_flat.redis_poolsize,
    })
}

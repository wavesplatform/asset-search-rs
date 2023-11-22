use serde::Deserialize;
use wavesexchange_liveness::PostgresConfig as LivenessPostgresConfig;

use crate::error::Error;

fn default_port() -> u16 {
    5432
}

fn default_poolsize() -> u32 {
    1
}

impl From<Config> for LivenessPostgresConfig {
    fn from(config: Config) -> Self {
        LivenessPostgresConfig {
            host: config.host,
            port: config.port,
            database: config.database,
            user: config.user,
            password: config.password,
            poolsize: config.pool_size,
        }
    }
}

#[derive(Deserialize)]
pub struct ConfigFlat {
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
    pub database: String,
    pub user: String,
    pub password: String,
    #[serde(default = "default_poolsize")]
    pub poolsize: u32,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub user: String,
    pub password: String,
    pub pool_size: u32,
}

pub fn load() -> Result<Config, Error> {
    let config_flat = envy::prefixed("POSTGRES__").from_env::<ConfigFlat>()?;

    Ok(Config {
        host: config_flat.host,
        port: config_flat.port,
        database: config_flat.database,
        user: config_flat.user,
        password: config_flat.password,
        pool_size: config_flat.poolsize,
    })
}

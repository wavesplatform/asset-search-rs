use serde::Deserialize;

use crate::error::Error;

fn default_port() -> u16 {
    5432
}

fn default_poolsize() -> u32 {
    1
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

#[derive(Clone, Debug)]
pub struct PostgresConfig {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub user: String,
    pub password: String,
}

#[derive(Clone, Debug)]
pub struct Config {
    pub postgres: PostgresConfig,
}

pub fn load() -> Result<Config, Error> {
    let config_flat = envy::prefixed("PG").from_env::<ConfigFlat>()?;

    Ok(Config {
        postgres: PostgresConfig {
            host: config_flat.host,
            port: config_flat.port,
            database: config_flat.database,
            user: config_flat.user,
            password: config_flat.password,
        },
    })
}

use serde::Deserialize;

use crate::error::Error;

fn default_pgport() -> u16 {
    5432
}

fn default_pgpoolsize() -> u32 {
    1
}

#[derive(Deserialize)]
pub struct ConfigFlat {
    pub pghost: String,
    #[serde(default = "default_pgport")]
    pub pgport: u16,
    pub pgdatabase: String,
    pub pguser: String,
    pub pgpassword: String,
    #[serde(default = "default_pgpoolsize")]
    pub pgpoolsize: u32,
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
    let config_flat = envy::from_env::<ConfigFlat>()?;

    Ok(Config {
        host: config_flat.pghost,
        port: config_flat.pgport,
        database: config_flat.pgdatabase,
        user: config_flat.pguser,
        password: config_flat.pgpassword,
        pool_size: config_flat.pgpoolsize,
    })
}

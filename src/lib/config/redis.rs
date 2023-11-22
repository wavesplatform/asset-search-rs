use serde::Deserialize;

use crate::error::Error;

fn default_port() -> u16 {
    6379
}

// redis username is empty by default (legacy)
// and authenticated using password only
fn default_user() -> String {
    "".to_owned()
}

fn default_poolsize() -> u32 {
    1
}

#[derive(Deserialize)]
pub struct ConfigFlat {
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
    #[serde(default = "default_user")]
    pub user: String,
    pub password: String,
    #[serde(default = "default_poolsize")]
    pub poolsize: u32,
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
    let config_flat = envy::prefixed("REDIS__").from_env::<ConfigFlat>()?;

    Ok(Config {
        host: config_flat.host,
        port: config_flat.port,
        user: config_flat.user,
        password: config_flat.password,
        poolsize: config_flat.poolsize,
    })
}

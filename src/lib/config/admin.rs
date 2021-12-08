use serde::Deserialize;

use crate::error::Error;

#[derive(Deserialize)]
pub struct ConfigFlat {
    pub api_key: String,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub api_key: String,
}

pub fn load() -> Result<Config, Error> {
    let admin_config_flat = envy::prefixed("ADMIN__").from_env::<ConfigFlat>()?;

    Ok(Config {
        api_key: admin_config_flat.api_key,
    })
}

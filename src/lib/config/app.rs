use serde::Deserialize;

use crate::error::Error;

#[derive(Deserialize)]
pub struct ConfigFlat {
    pub waves_association_address: String,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub waves_association_address: String,
}

pub fn load() -> Result<Config, Error> {
    let app_config_flat = envy::from_env::<ConfigFlat>()?;

    Ok(Config {
        waves_association_address: app_config_flat.waves_association_address,
    })
}

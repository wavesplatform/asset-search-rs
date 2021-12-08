use serde::Deserialize;

use crate::error::Error;

#[derive(Deserialize)]
pub struct ConfigFlat {
    pub oracle_address: String,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub oracle_address: String,
}

pub fn load() -> Result<Config, Error> {
    let invalidate_config_flat = envy::from_env::<ConfigFlat>()?;

    Ok(Config {
        oracle_address: invalidate_config_flat.oracle_address,
    })
}

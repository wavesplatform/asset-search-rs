use serde::Deserialize;

use crate::error::Error;

fn default_invalidate_entire_cache() -> bool {
    false
}

#[derive(Deserialize)]
pub struct ConfigFlat {
    pub waves_association_address: String,
    #[serde(default = "default_invalidate_entire_cache")]
    pub invalidate_entire_cache: bool,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub waves_association_address: String,
    pub invalidate_entire_cache: bool,
}

pub fn load() -> Result<Config, Error> {
    let app_config_flat = envy::from_env::<ConfigFlat>()?;

    Ok(Config {
        waves_association_address: app_config_flat.waves_association_address,
        invalidate_entire_cache: app_config_flat.invalidate_entire_cache,
    })
}

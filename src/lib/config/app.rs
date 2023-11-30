use serde::Deserialize;

use crate::cache::InvalidateCacheMode;
use crate::error::Error;

fn default_invalidate_entire_cache() -> InvalidateCacheMode {
    InvalidateCacheMode::AllData
}

#[derive(Deserialize)]
struct ConfigFlat {
    asset_storage_address: String,
    #[serde(default = "default_invalidate_entire_cache")]
    invalidate_cache_mode: InvalidateCacheMode,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub asset_storage_address: String,
    pub invalidate_cache_mode: InvalidateCacheMode,
}

pub fn load() -> Result<Config, Error> {
    let app_config_flat = envy::from_env::<ConfigFlat>()?;

    Ok(Config {
        asset_storage_address: app_config_flat.asset_storage_address,
        invalidate_cache_mode: app_config_flat.invalidate_cache_mode,
    })
}

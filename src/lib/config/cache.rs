use serde::Deserialize;

use crate::error::Error;

fn default_key_prefix() -> String {
    "asset".to_owned()
}

#[derive(Deserialize)]
pub struct ConfigFlat {
    #[serde(default = "default_key_prefix")]
    pub key_prefix: String,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub key_prefix: String,
}

pub fn load() -> Result<Config, Error> {
    let cache_config_flat = envy::from_env::<ConfigFlat>()?;

    Ok(Config {
        key_prefix: cache_config_flat.key_prefix,
    })
}

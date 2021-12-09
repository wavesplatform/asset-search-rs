use serde::Deserialize;

use crate::error::Error;

#[derive(Deserialize)]
pub struct ConfigFlat {
    pub port: u16,
    pub image_service_url: String,
    pub image_service_bypass: bool,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub port: u16,
    pub image_service_url: String,
    pub image_service_bypass: bool,
}

pub fn load() -> Result<Config, Error> {
    let api_config_flat = envy::prefixed("API__").from_env::<ConfigFlat>()?;

    Ok(Config {
        port: api_config_flat.port,
        image_service_url: api_config_flat.image_service_url,
        image_service_bypass: api_config_flat.image_service_bypass,
    })
}

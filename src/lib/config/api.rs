use serde::Deserialize;

use crate::error::Error;

#[derive(Deserialize)]
pub struct ConfigFlat {
    pub port: u16,
    pub images_service_base_url: String,
    pub bypass_images_service: bool,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub port: u16,
    pub images_service_base_url: String,
    pub bypass_images_service: bool,
}

pub fn load() -> Result<Config, Error> {
    let api_config_flat = envy::prefixed("API__").from_env::<ConfigFlat>()?;

    Ok(Config {
        port: api_config_flat.port,
        images_service_base_url: api_config_flat.images_service_base_url,
        bypass_images_service: api_config_flat.bypass_images_service,
    })
}

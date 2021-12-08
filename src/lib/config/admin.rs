use serde::Deserialize;

use crate::error::Error;

#[derive(Deserialize)]
pub struct ConfigFlat {
    pub api_port: u16,
    pub api_key: String,
    pub images_service_base_url: String,
    pub bypass_images_service: bool,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub api_port: u16,
    pub api_key: String,
    pub images_service_base_url: String,
    pub bypass_images_service: bool,
}

pub fn load() -> Result<Config, Error> {
    let admin_config_flat = envy::from_env::<ConfigFlat>()?;

    Ok(Config {
        api_port: admin_config_flat.api_port,
        api_key: admin_config_flat.api_key,
        images_service_base_url: admin_config_flat.images_service_base_url,
        bypass_images_service: admin_config_flat.bypass_images_service,
    })
}

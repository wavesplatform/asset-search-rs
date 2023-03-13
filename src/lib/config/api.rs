use serde::Deserialize;

use crate::error::Error;

fn default_port() -> u16 {
    8080
}

fn default_metrics_port() -> u16 {
    9090
}

#[derive(Deserialize)]
struct ConfigFlat {
    #[serde(default = "default_port")]
    port: u16,
    #[serde(default = "default_metrics_port")]
    metrics_port: u16,
    #[serde(default)]
    image_service_url: String,
    #[serde(default)]
    image_service_bypass: bool,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub port: u16,
    pub metrics_port: u16,
    pub image_service_url: String,
    pub image_service_bypass: bool,
}

pub fn load() -> Result<Config, Error> {
    let api_config_flat = envy::prefixed("API__").from_env::<ConfigFlat>()?;

    Ok(Config {
        port: api_config_flat.port,
        metrics_port: api_config_flat.metrics_port,
        image_service_url: api_config_flat.image_service_url,
        image_service_bypass: api_config_flat.image_service_bypass,
    })
}

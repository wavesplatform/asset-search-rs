use serde::Deserialize;

use crate::error::Error;

fn default_updates_per_request() -> usize {
    256
}

fn default_max_wait_time_in_secs() -> u64 {
    5
}

#[derive(Deserialize)]
pub struct ConfigFlat {
    pub blockchain_updates_url: String,
    pub starting_height: u32,
    #[serde(default = "default_updates_per_request")]
    pub updates_per_request: usize,
    #[serde(default = "default_max_wait_time_in_secs")]
    pub max_wait_time_in_secs: u64,
    pub chain_id: u8,
    pub waves_association_address: String,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub blockchain_updates_url: String,
    pub starting_height: u32,
    pub updates_per_request: usize,
    pub max_wait_time_in_secs: u64,
    pub chain_id: u8,
    pub waves_association_address: String,
}

pub fn load() -> Result<Config, Error> {
    let config_flat = envy::from_env::<ConfigFlat>()?;

    Ok(Config {
        blockchain_updates_url: config_flat.blockchain_updates_url,
        starting_height: config_flat.starting_height,
        updates_per_request: config_flat.updates_per_request,
        max_wait_time_in_secs: config_flat.max_wait_time_in_secs,
        chain_id: config_flat.chain_id,
        waves_association_address: config_flat.waves_association_address,
    })
}

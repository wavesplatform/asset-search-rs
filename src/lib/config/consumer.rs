use serde::Deserialize;

use crate::error::Error;

fn default_updates_per_request() -> usize {
    256
}

fn default_max_wait_time_in_secs() -> u64 {
    5
}

fn default_metrics_port() -> u16 {
    9090
}

fn default_start_rollback_depth() -> u32 {
    1
}

#[derive(Deserialize)]
struct ConfigFlat {
    #[serde(default = "default_metrics_port")]
    metrics_port: u16,
    blockchain_updates_url: String,
    starting_height: u32,
    #[serde(default = "default_updates_per_request")]
    updates_per_request: usize,
    #[serde(default = "default_max_wait_time_in_secs")]
    max_wait_time_in_secs: u64,
    chain_id: u8,
    asset_storage_address: String,
    #[serde(default = "default_start_rollback_depth")]
    start_rollback_depth: u32,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub metrics_port: u16,
    pub blockchain_updates_url: String,
    pub starting_height: u32,
    pub updates_per_request: usize,
    pub max_wait_time_in_secs: u64,
    pub chain_id: u8,
    pub asset_storage_address: String,
    pub start_rollback_depth: u32,
}

pub fn load() -> Result<Config, Error> {
    let config_flat = envy::from_env::<ConfigFlat>()?;

    Ok(Config {
        metrics_port: config_flat.metrics_port,
        blockchain_updates_url: config_flat.blockchain_updates_url,
        starting_height: config_flat.starting_height,
        updates_per_request: config_flat.updates_per_request,
        max_wait_time_in_secs: config_flat.max_wait_time_in_secs,
        chain_id: config_flat.chain_id,
        asset_storage_address: config_flat.asset_storage_address,
        start_rollback_depth: config_flat.start_rollback_depth,
    })
}

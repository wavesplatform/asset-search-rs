use serde::Deserialize;

use crate::error::Error;

fn default_port() -> u16 {
    5432
}

#[derive(Deserialize)]
struct ConfigFlat {
    host: String,
    #[serde(default = "default_port")]
    port: u16,
    database: String,
    user: String,
    password: String,
}

#[derive(Clone, Debug)]
pub struct PostgresConfig {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub user: String,
    pub password: String,
}

#[derive(Clone, Debug)]
pub struct Config {
    pub postgres: PostgresConfig,
}

impl Config {
    pub fn database_url(&self) -> String {
        format!(
            "postgres://{}:{}@{}:{}/{}",
            self.postgres.user,
            self.postgres.password,
            self.postgres.host,
            self.postgres.port,
            self.postgres.database
        )
    }
}

pub fn load() -> Result<Config, Error> {
    let config_flat = envy::prefixed("PG").from_env::<ConfigFlat>()?;

    Ok(Config {
        postgres: PostgresConfig {
            host: config_flat.host,
            port: config_flat.port,
            database: config_flat.database,
            user: config_flat.user,
            password: config_flat.password,
        },
    })
}

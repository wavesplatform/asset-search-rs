pub mod enums;

use anyhow::{Error, Result};
use diesel::pg::PgConnection;
use diesel::r2d2::{ConnectionManager, Pool};
use diesel::Connection;
use std::time::Duration;

use crate::config::postgres::Config;
use crate::error::Error as AppError;

pub type PgPool = Pool<ConnectionManager<PgConnection>>;

pub fn pool(config: &Config) -> Result<PgPool, AppError> {
    let db_url = config.database_url();

    let manager = ConnectionManager::<PgConnection>::new(db_url);
    Ok(Pool::builder()
        .min_idle(Some(1))
        .max_size(config.pool_size as u32)
        .idle_timeout(Some(Duration::from_secs(5 * 60)))
        .connection_timeout(Duration::from_secs(5))
        .build(manager)?)
}

pub fn unpooled(config: &Config) -> Result<PgConnection> {
    let db_url = config.database_url();

    PgConnection::establish(&db_url).map_err(|err| Error::new(AppError::ConnectionError(err)))
}

pub mod server;

use serde::Deserialize;

use crate::cache::InvalidateCacheMode;

#[derive(Clone, Debug, Deserialize)]
pub struct InvalidateCacheQueryParams {
    pub mode: InvalidateCacheMode,
}

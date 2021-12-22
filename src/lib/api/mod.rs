pub mod dtos;
pub mod models;
pub mod server;

const ERROR_CODES_PREFIX: u16 = 95;
pub const DEFAULT_LIMIT: u32 = 100;
pub const DEFAULT_INCLUDE_METADATA: bool = true;
pub const DEFAULT_FORMAT: dtos::ResponseFormat = dtos::ResponseFormat::Full;

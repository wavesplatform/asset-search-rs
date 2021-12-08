use warp::reject::Reject;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("LoadConfigFailed: {0}")]
    LoadConfigFailed(#[from] envy::Error),
    #[error("HttpRequestError {0}")]
    HttpRequestError(#[from] reqwest::Error),
    #[error("InvalidMessage: {0}")]
    InvalidMessage(String),
    #[error("DbDieselError: {0}")]
    DbDieselError(#[from] diesel::result::Error),
    #[error("DbError: {0}")]
    DbError(String),
    #[error("CacheError: {0}")]
    CacheError(String),
    #[error("ConnectionPoolError: {0}")]
    ConnectionPoolError(#[from] r2d2::Error),
    #[error("ConnectionError: {0}")]
    ConnectionError(#[from] diesel::ConnectionError),
    #[error("ValidationError: {0}")]
    ValidationError(String, Option<std::collections::HashMap<String, String>>),
    #[error("StreamClosed: {0}")]
    StreamClosed(String),
    #[error("ConsistencyError: {0}")]
    ConsistencyError(String),
    #[error("UpstreamAPIBadResponse: {0}")]
    UpstreamAPIBadResponse(String),
    #[error("CursorSerializationError: {0}")]
    CursorSerializationError(#[from] serde_json::Error),
    #[error("CursorDecodeError: {0}")]
    CursorDecodeError(#[from] base64::DecodeError),
    #[error("DataEntryValueParseError: {0}")]
    DataEntryValueParseError(String),
    #[error("RedisError: {0}")]
    RedisError(#[from] redis::RedisError),
    #[error("InvalidDataEntryUpdate: {0}")]
    InvalidDataEntryUpdate(String),
    #[error("Unauthorized: {0}")]
    Unauthorized(String),
    #[error("InvalidVariant: {0}")]
    InvalidVariant(String),
}

impl Reject for Error {}

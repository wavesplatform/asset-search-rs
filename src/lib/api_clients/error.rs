use std::sync::Arc;

#[derive(Debug, Clone, thiserror::Error)]
pub enum Error {
    #[error("NotFoundError")]
    NotFoundError,
    #[error("HttpRequestError: {1} - {0}")]
    HttpRequestError(Arc<reqwest::Error>, String),
    #[error("InvalidStatus: {1}, status code: {0}")]
    InvalidStatus(reqwest::StatusCode, String),
    #[error("ParseResultError: {0}")]
    ParseResultError(String),
    #[error("DecodeResponseBytesError: {0}")]
    DecodeResponseBytesError(Arc<reqwest::Error>),
}

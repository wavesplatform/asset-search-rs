use async_trait::async_trait;
use bytes::Bytes;
use reqwest::StatusCode;
use std::sync::Arc;
use wavesexchange_log::trace;

use super::{ApiBaseUrl, Error, HttpClient};

#[async_trait]
pub trait Client: ApiBaseUrl {
    async fn svg(&self, asset_id: &str) -> Result<Bytes, Error>;
}

#[async_trait]
impl Client for HttpClient {
    async fn svg(&self, asset_id: &str) -> Result<Bytes, Error> {
        let endpoint_url = format!("{}{}.svg", &self.root_url, asset_id);

        trace!("Images service request: {}", endpoint_url,);

        let resp = self.client.get(&endpoint_url).send().await.map_err(|err| {
            Error::HttpRequestError(
                Arc::new(err),
                "Failed to the get result from the images service".to_string(),
            )
        })?;

        if resp.status() == StatusCode::OK {
            resp.bytes()
                .await
                .map_err(|err| Error::DecodeResponseBytesError(Arc::new(err)))
        } else if resp.status() == StatusCode::NOT_FOUND {
            Err(Error::NotFoundError)
        } else {
            Err(Error::InvalidStatus(
                resp.status(),
                format!(
                    "Failed to check whether images service has an image for the asset id {}",
                    asset_id
                ),
            ))
        }
    }
}

use async_trait::async_trait;
use reqwest::StatusCode;
use wavesexchange_log::{debug, trace};

use super::{ApiBaseUrl, Error, HttpClient};

#[async_trait]
pub trait Client: ApiBaseUrl {
    async fn has(&self, asset_id: &str) -> Result<bool, Error>;
}

#[async_trait]
impl Client for HttpClient {
    async fn has(&self, asset_id: &str) -> Result<bool, Error> {
        let endpoint_url = format!("{}/{}.svg", &self.root_url, asset_id);

        trace!("Images service request:\n\tURL: {}", endpoint_url);

        let req_start_time = chrono::Utc::now();

        let resp = self.client.get(&endpoint_url).send().await.map_err(|err| {
            Error::HttpRequestError(
                std::sync::Arc::new(err),
                "Failed to the get result from the images service".to_string(),
            )
        })?;

        let req_end_time = chrono::Utc::now();
        debug!(
            "Images service request has took {:?}ms",
            (req_end_time - req_start_time).num_milliseconds()
        );

        if resp.status() == StatusCode::OK {
            Ok(true)
        } else if resp.status() == StatusCode::NOT_FOUND {
            Ok(false)
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

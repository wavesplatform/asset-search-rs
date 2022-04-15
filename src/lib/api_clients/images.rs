use async_trait::async_trait;
use reqwest::StatusCode;
use serde_json::json;
use std::sync::Arc;
use wavesexchange_log::trace;

use super::{ApiBaseUrl, Error, HttpClient};

#[async_trait]
pub trait Client: ApiBaseUrl {
    async fn has_svg(&self, asset_id: &str) -> Result<bool, Error>;

    async fn has_svgs(&self, asset_ids: &[&str]) -> Result<Vec<bool>, Error>;
}

#[async_trait]
impl Client for HttpClient {
    async fn has_svg(&self, asset_id: &str) -> Result<bool, Error> {
        let endpoint_url = format!("{}{}.svg", &self.root_url, asset_id);

        trace!("Images service request: {}", endpoint_url,);

        let resp = self
            .client
            .head(&endpoint_url)
            .send()
            .await
            .map_err(|err| {
                Error::HttpRequestError(
                    Arc::new(err),
                    "Failed to the get result from the images service".to_string(),
                )
            })?;

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

    async fn has_svgs(&self, asset_ids: &[&str]) -> Result<Vec<bool>, Error> {
        let endpoint_url = format!("{}images_existence", &self.root_url);
        let body = json!({ "images": asset_ids.clone().into_iter().map(|asset_id| format!("{}.svg", asset_id)).collect::<Vec<_>>() });

        let resp = self
            .client
            .post(&endpoint_url)
            .json(&body)
            .send()
            .await
            .map_err(|err| {
                Error::HttpRequestError(
                    Arc::new(err),
                    "Failed to the get result from the images service".to_string(),
                )
            })?;

        if resp.status() == StatusCode::OK {
            resp.json::<Vec<bool>>().await.map_err(|err| {
                Error::HttpRequestError(
                    Arc::new(err),
                    "Failed to the get result from the images service".to_string(),
                )
            })
        } else {
            Err(Error::InvalidStatus(
                resp.status(),
                format!("Failed to check whether images service has an image for the asset ids",),
            ))
        }
    }
}

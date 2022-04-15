use wavesexchange_log::{debug, trace};

use super::Service;
use crate::api_clients::{images, Error as ApiClientError};
use crate::error::Error as AppError;

pub struct HttpService {
    images_api_client: Box<dyn images::Client + Send + Sync>,
}

impl HttpService {
    pub fn new(client: impl images::Client + Send + Sync + 'static) -> Self {
        Self {
            images_api_client: Box::new(client),
        }
    }
}

#[async_trait::async_trait]
impl Service for HttpService {
    async fn has_image(&self, id: &str) -> Result<bool, AppError> {
        trace!("has image"; "id" => format!("{:?}", id));
        match cache::has_image(&self.images_api_client, id).await {
            Ok(res) => Ok(res),
            Err(err) => Err(AppError::UpstreamAPIBadResponse(err.to_string())),
        }
    }

    async fn has_images(&self, ids: &[&str]) -> Result<Vec<bool>, AppError> {
        let start_time = tokio::time::Instant::now();

        let has_images = self
            .images_api_client
            .has_svgs(ids)
            .await
            .map_err(|e| AppError::UpstreamAPIBadResponse(e.to_string()))?;
        debug!(
            "has images: completed in {}ms ({} images)",
            start_time.elapsed().as_millis(),
            ids.len()
        );
        Ok(has_images)
    }
}

pub mod cache {
    use cached::proc_macro::cached;

    use super::*;

    #[cached(
        time = 60,
        key = "String",
        convert = r#"{ format!("{}", asset_id ) }"#,
        result = true
    )]
    pub async fn has_image(
        images_api_client: &Box<dyn images::Client + Send + Sync>,
        asset_id: &str,
    ) -> Result<bool, ApiClientError> {
        images_api_client.has_svg(asset_id).await
    }
}

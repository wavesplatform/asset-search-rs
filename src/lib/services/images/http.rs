use wavesexchange_log::trace;

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
            Ok(_) => Ok(true),
            Err(ApiClientError::NotFoundError) => Ok(false),
            Err(err) => Err(AppError::UpstreamAPIBadResponse(err.to_string())),
        }
    }

    async fn has_images(&self, ids: &[&str]) -> Result<Vec<bool>, AppError> {
        trace!("has images"; "ids" => format!("{:?}", ids));
        let fs = ids.iter().map(|id| self.has_image(id));
        let has_images = futures::future::join_all(fs)
            .await
            .into_iter()
            .collect::<Result<_, AppError>>()?;
        Ok(has_images)
    }
}

pub mod cache {
    use bytes::Bytes;
    use cached::proc_macro::cached;

    use super::*;

    #[cached(
        time = 60,
        key = "String",
        convert = r#"{ format!("{}", asset_id ) }"#,
        result = true,
        sync_writes = true
    )]
    pub async fn has_image(
        images_api_client: &Box<dyn images::Client + Send + Sync>,
        asset_id: &str,
    ) -> Result<Bytes, ApiClientError> {
        images_api_client.svg(asset_id).await
    }
}

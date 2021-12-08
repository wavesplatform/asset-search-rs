use wavesexchange_log::debug;

use super::Service;
use crate::api_clients::images;
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
        debug!("has image"; "id" => format!("{:?}", id));
        self.images_api_client
            .has(id)
            .await
            .map_err(|err| AppError::UpstreamAPIBadResponse(err.to_string()))
    }

    async fn has_images(&self, ids: &[&str]) -> Result<Vec<bool>, AppError> {
        debug!("has images"; "ids" => format!("{:?}", ids));
        let fs = ids.iter().map(|id| self.has_image(id));
        let has_images = futures::future::join_all(fs)
            .await
            .into_iter()
            .collect::<Result<_, AppError>>()?;
        Ok(has_images)
    }
}

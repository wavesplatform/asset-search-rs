use wavesexchange_log::trace;

use super::Service;
use crate::error::Error as AppError;

pub struct DummyService {}

impl DummyService {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait::async_trait]
impl Service for DummyService {
    async fn has_image(&self, id: &str) -> Result<bool, AppError> {
        trace!("has image"; "id" => format!("{:?}", id));
        Ok(false)
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

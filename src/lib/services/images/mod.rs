pub mod dummy;
pub mod http;

use crate::error::Error as AppError;

#[async_trait::async_trait]
pub trait Service {
    async fn has_image(&self, id: &str) -> Result<bool, AppError>;

    async fn has_images(&self, ids: &[&str]) -> Result<Vec<bool>, AppError>;
}

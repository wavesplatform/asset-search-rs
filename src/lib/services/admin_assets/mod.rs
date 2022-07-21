pub mod repo;

use std::collections::HashSet;
use std::sync::Arc;

use crate::cache::{AssetUserDefinedData, AsyncWriteCache};
use crate::error::Error as AppError;

#[async_trait::async_trait]
pub trait Service {
    async fn add_label(&self, id: &str, label: &str) -> Result<(), AppError>;

    async fn delete_label(&self, id: &str, label: &str) -> Result<(), AppError>;
}

pub struct AdminAssetsService {
    pub repo: Arc<dyn repo::Repo + Send + Sync>,
    pub user_defined_data_cache: Box<dyn AsyncWriteCache<AssetUserDefinedData> + Send + Sync>,
}

impl AdminAssetsService {
    pub fn new(
        repo: Arc<dyn repo::Repo + Send + Sync>,
        user_defined_data_cache: Box<dyn AsyncWriteCache<AssetUserDefinedData> + Send + Sync>,
    ) -> Self {
        Self {
            repo,
            user_defined_data_cache,
        }
    }
}

#[async_trait::async_trait]
impl Service for AdminAssetsService {
    async fn add_label(&self, id: &str, label: &str) -> Result<(), AppError> {
        if self
            .repo
            .add_label(id, label)
            .map_err(|err| AppError::DbError(err.to_string()))?
        {
            let asset_id = id.to_owned();
            let label = label.to_owned();

            let asset_user_defined_data = if let Some(cached_data) = self
                .user_defined_data_cache
                .get(id)
                .await
                .map_err(|e| AppError::CacheError(format!("{}", e)))?
            {
                let mut labels: HashSet<String> =
                    cached_data.labels.into_iter().collect::<HashSet<String>>();
                labels.insert(label);

                AssetUserDefinedData {
                    asset_id,
                    labels: labels.into_iter().collect::<Vec<_>>(),
                }
            } else {
                AssetUserDefinedData {
                    asset_id,
                    labels: vec![label],
                }
            };

            self.user_defined_data_cache
                .set(id.to_owned(), asset_user_defined_data)
                .await?;

            Ok(())
        } else {
            Err(AppError::ConsistencyError("Asset not found".to_owned()))
        }
    }

    async fn delete_label(&self, id: &str, label: &str) -> Result<(), AppError> {
        if self
            .repo
            .delete_label(id, label)
            .map_err(|err| AppError::DbError(err.to_string()))?
        {
            let asset_id = id.to_owned();
            let label = label.to_owned();

            let asset_user_defined_data = if let Some(cached_data) = self
                .user_defined_data_cache
                .get(id)
                .await
                .map_err(|e| AppError::CacheError(format!("{}", e)))?
            {
                let labels = cached_data
                    .labels
                    .into_iter()
                    .filter(|l| *l != label)
                    .collect::<Vec<_>>();

                AssetUserDefinedData { asset_id, labels }
            } else {
                AssetUserDefinedData {
                    asset_id,
                    labels: vec![],
                }
            };

            self.user_defined_data_cache
                .set(id.to_owned(), asset_user_defined_data)
                .await?;

            Ok(())
        } else {
            Err(AppError::ConsistencyError("Asset not found".to_owned()))
        }
    }
}

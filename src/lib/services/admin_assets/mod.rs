pub mod repo;

use std::collections::HashSet;
use std::convert::TryFrom;
use std::sync::Arc;

use crate::cache::{AssetUserDefinedData, SyncWriteCache};
use crate::db::enums::{AssetWxLabelValueType, VerificationStatusValueType};
use crate::error::Error as AppError;
use crate::models::{AssetLabel, VerificationStatus};

pub trait Service {
    fn update_verification_status(
        &self,
        id: &str,
        verification_status: &VerificationStatus,
    ) -> Result<(), AppError>;

    fn update_ticker(&self, id: &str, ticker: Option<&str>) -> Result<(), AppError>;

    fn add_label(&self, id: &str, label: &AssetLabel) -> Result<(), AppError>;

    fn delete_label(&self, id: &str, label: &AssetLabel) -> Result<(), AppError>;
}

pub struct AdminAssetsService {
    pub repo: Arc<dyn repo::Repo + Send + Sync>,
    pub user_defined_data_cache: Box<dyn SyncWriteCache<AssetUserDefinedData> + Send + Sync>,
}

impl AdminAssetsService {
    pub fn new(
        repo: Arc<dyn repo::Repo + Send + Sync>,
        user_defined_data_cache: Box<dyn SyncWriteCache<AssetUserDefinedData> + Send + Sync>,
    ) -> Self {
        Self {
            repo,
            user_defined_data_cache,
        }
    }
}

impl Service for AdminAssetsService {
    fn update_verification_status(
        &self,
        id: &str,
        verification_status: &VerificationStatus,
    ) -> Result<(), AppError> {
        let vs = VerificationStatusValueType::from(verification_status);
        if self
            .repo
            .set_verification_status(id, &vs)
            .map_err(|e| AppError::DbError(e.to_string()))?
        {
            let asset_user_defined_data = if let Some(cached_data) = self
                .user_defined_data_cache
                .get(id)
                .map_err(|e| AppError::CacheError(format!("{}", e)))?
            {
                AssetUserDefinedData {
                    asset_id: id.to_owned(),
                    verification_status: verification_status.to_owned(),
                    ticker: cached_data.ticker,
                    labels: cached_data.labels,
                }
            } else {
                AssetUserDefinedData {
                    asset_id: id.to_owned(),
                    verification_status: verification_status.to_owned(),
                    ticker: None,
                    labels: vec![],
                }
            };

            self.user_defined_data_cache
                .set(id, asset_user_defined_data)?;

            Ok(())
        } else {
            Err(AppError::ConsistencyError("Asset not found".to_owned()))
        }
    }

    fn update_ticker(&self, id: &str, ticker: Option<&str>) -> Result<(), AppError> {
        if self
            .repo
            .update_ticker(id, ticker)
            .map_err(|err| AppError::DbError(err.to_string()))?
        {
            let ticker = ticker.map(|s| s.to_owned());
            let asset_user_defined_data = if let Some(cached_data) = self
                .user_defined_data_cache
                .get(id)
                .map_err(|e| AppError::CacheError(format!("{}", e)))?
            {
                AssetUserDefinedData {
                    asset_id: id.to_owned(),
                    ticker,
                    verification_status: cached_data.verification_status,
                    labels: cached_data.labels,
                }
            } else {
                AssetUserDefinedData {
                    asset_id: id.to_owned(),
                    ticker,
                    verification_status: VerificationStatus::default(),
                    labels: vec![],
                }
            };

            self.user_defined_data_cache
                .set(id, asset_user_defined_data)?;

            Ok(())
        } else {
            Err(AppError::ConsistencyError("Asset not found".to_owned()))
        }
    }

    fn add_label(&self, id: &str, label: &AssetLabel) -> Result<(), AppError> {
        let l = AssetWxLabelValueType::try_from(label)?;

        if self
            .repo
            .add_label(id, &l)
            .map_err(|err| AppError::DbError(err.to_string()))?
        {
            let asset_id = id.to_owned();
            let label = label.to_owned();

            let asset_user_defined_data = if let Some(cached_data) = self
                .user_defined_data_cache
                .get(id)
                .map_err(|e| AppError::CacheError(format!("{}", e)))?
            {
                let mut labels: HashSet<AssetLabel> = cached_data
                    .labels
                    .into_iter()
                    .collect::<HashSet<AssetLabel>>();
                labels.insert(label);

                AssetUserDefinedData {
                    asset_id,
                    labels: labels.into_iter().collect::<Vec<_>>(),
                    ticker: cached_data.ticker,
                    verification_status: cached_data.verification_status,
                }
            } else {
                AssetUserDefinedData {
                    asset_id,
                    labels: vec![label],
                    ticker: None,
                    verification_status: VerificationStatus::default(),
                }
            };

            self.user_defined_data_cache
                .set(id, asset_user_defined_data)?;

            Ok(())
        } else {
            Err(AppError::ConsistencyError("Asset not found".to_owned()))
        }
    }

    fn delete_label(&self, id: &str, label: &AssetLabel) -> Result<(), AppError> {
        let l = AssetWxLabelValueType::try_from(label)?;

        if self
            .repo
            .delete_label(id, &l)
            .map_err(|err| AppError::DbError(err.to_string()))?
        {
            let asset_id = id.to_owned();
            let label = label.to_owned();

            let asset_user_defined_data = if let Some(cached_data) = self
                .user_defined_data_cache
                .get(id)
                .map_err(|e| AppError::CacheError(format!("{}", e)))?
            {
                let labels = cached_data
                    .labels
                    .into_iter()
                    .filter(|l| *l != label)
                    .collect::<Vec<_>>();

                AssetUserDefinedData {
                    asset_id,
                    labels,
                    ticker: cached_data.ticker,
                    verification_status: cached_data.verification_status,
                }
            } else {
                AssetUserDefinedData {
                    asset_id,
                    labels: vec![],
                    ticker: None,
                    verification_status: VerificationStatus::default(),
                }
            };

            self.user_defined_data_cache
                .set(id, asset_user_defined_data)?;

            Ok(())
        } else {
            Err(AppError::ConsistencyError("Asset not found".to_owned()))
        }
    }
}

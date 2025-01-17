pub mod async_redis_cache;
mod dtos;
pub mod invalidator;
pub mod sync_redis_cache;

pub use dtos::{AssetBlockchainData, AssetUserDefinedData, InvalidateCacheMode};

use crate::error::Error as AppError;

pub const KEY_SEPARATOR: &str = ":";
pub const ASSET_BLOCKCHAIN_DATA_KEY_PREFIX: &str = "asset";
pub const ASSET_USER_DEFINED_DATA_KEY_PREFIX: &str = "asset_user_defined_data";

pub trait CacheKeyFn {
    fn key_fn(&self, source_key: &str) -> String;
}

pub trait SyncReadCache<T>: CacheKeyFn {
    fn get(&self, key: &str) -> Result<Option<T>, AppError>;

    fn mget(&self, keys: &[&str]) -> Result<Vec<Option<T>>, AppError>;
}

pub trait SyncWriteCache<T>: SyncReadCache<T> {
    fn set(&self, key: &str, value: T) -> Result<(), AppError>;

    fn clear(&self) -> Result<(), AppError>;
}

#[async_trait::async_trait]
pub trait AsyncReadCache<T>: CacheKeyFn {
    async fn get(&self, key: &str) -> Result<Option<T>, AppError>;

    async fn mget(&self, keys: &[&str]) -> Result<Vec<Option<T>>, AppError>;
}

#[async_trait::async_trait]
pub trait AsyncWriteCache<T>: AsyncReadCache<T> {
    async fn set(&self, key: String, value: T) -> Result<(), AppError>;

    async fn clear(&self) -> Result<(), AppError>;
}

#[cfg(test)]
mod tests {
    use super::AssetUserDefinedData;

    #[test]
    fn should_add_label() {
        let udd = AssetUserDefinedData::new("asset_id");
        let udd_with_new_label = udd.add_label("WA_VERIFIED");
        assert_eq!(udd_with_new_label.labels, vec!["WA_VERIFIED"]);
    }

    #[test]
    fn should_add_label_exactly_once() {
        let udd = AssetUserDefinedData::new("asset_id");
        let udd_with_new_label = udd.add_label("WA_VERIFIED");
        let udd_with_new_label = udd_with_new_label.add_label("WA_VERIFIED");
        assert_eq!(udd_with_new_label.labels, vec!["WA_VERIFIED"]);
    }

    #[test]
    fn should_skip_empty_label_addition() {
        let udd = AssetUserDefinedData::new("asset_id");
        let udd_with_new_label = udd.add_label("");
        assert_eq!(udd_with_new_label.labels, [] as [&str; 0]);
    }

    #[test]
    fn should_delete_label() {
        let udd = AssetUserDefinedData::new("asset_id");
        let udd_with_new_label = udd.delete_label("WA_VERIFIED");
        assert_eq!(udd_with_new_label.labels, Vec::<String>::new());
    }

    #[test]
    fn should_delete_label_exactly_once() {
        let udd = AssetUserDefinedData::new("asset_id");
        let udd_with_new_label = udd.delete_label("WA_VERIFIED");
        // should not fail while deleting non-existing label
        udd_with_new_label.delete_label("WA_VERIFIED");
        assert_eq!(udd_with_new_label.labels, Vec::<String>::new());
    }
}

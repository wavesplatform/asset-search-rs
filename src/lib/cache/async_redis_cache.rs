use itertools::Itertools;
use redis::AsyncCommands;
use serde::{de::DeserializeOwned, Serialize};
use std::fmt::Debug;
use wavesexchange_log::trace;

use super::{AsyncReadCache, AsyncWriteCache, CacheKeyFn};
use crate::{async_redis::RedisPool, error::Error as AppError};
#[derive(Clone)]
pub struct AsyncRedisCache {
    redis_pool: RedisPool,
    key_prefix: String,
    key_separator: String,
}

pub fn new(
    redis_pool: RedisPool,
    key_prefix: impl AsRef<str>,
    key_separator: impl AsRef<str>,
) -> AsyncRedisCache {
    AsyncRedisCache {
        redis_pool,
        key_prefix: key_prefix.as_ref().to_string(),
        key_separator: key_separator.as_ref().to_string(),
    }
}

#[async_trait::async_trait]
impl<T> AsyncReadCache<T> for AsyncRedisCache
where
    T: DeserializeOwned + Clone + Debug,
{
    async fn get(&self, key: &str) -> Result<Option<T>, AppError> {
        let key = self.key_fn(key);

        trace!("get value from redis cache for key {}", key);

        let mut con = self
            .redis_pool
            .get()
            .await
            .map_err(|e| AppError::Bb8RunError(e.to_string()))?;
        let value: Option<String> = con.get(key).await?;

        match value {
            Some(s) => serde_json::from_str(&s)
                .map(|v| Some(v))
                .map_err(|e| AppError::from(e)),
            _ => Ok(None),
        }
    }

    async fn mget(&self, keys: &[&str]) -> Result<Vec<Option<T>>, AppError> {
        let keys = keys.into_iter().map(|k| self.key_fn(k)).collect::<Vec<_>>();

        trace!("mget values from redis cache for keys {:?}", keys);

        let mut con = self
            .redis_pool
            .get()
            .await
            .map_err(|e| AppError::Bb8RunError(e.to_string()))?;
        match keys.len() {
            0 => Ok(vec![]),
            1 => {
                con.get(keys)
                    .await
                    .map_err(|e| AppError::from(e))
                    .and_then(|m: Option<String>| match m {
                        Some(s) => {
                            let v = serde_json::from_str(&s)?;
                            Ok(vec![v])
                        }
                        _ => Ok(vec![None]),
                    })
            }
            _ => con.get(keys).await.map_err(|e| AppError::from(e)).and_then(
                |ms: Vec<Option<String>>| {
                    ms.into_iter()
                        .map(|m| match m {
                            Some(s) => serde_json::from_str(&s)
                                .map(|v| Some(v))
                                .map_err(|e| AppError::from(e)),
                            _ => Ok(None),
                        })
                        .try_collect()
                },
            ),
        }
    }
}

#[async_trait::async_trait]
impl<T> AsyncWriteCache<T> for AsyncRedisCache
where
    T: Serialize + DeserializeOwned + Clone + Debug + Send + 'static,
{
    async fn set(&self, key: String, value: T) -> Result<(), AppError> {
        let key = self.key_fn(&key);

        trace!("set redis cache value for key {}: {:?}", key, value);

        let mut con = self
            .redis_pool
            .get()
            .await
            .map_err(|e| AppError::Bb8RunError(e.to_string()))?;
        let value = serde_json::to_string(&value)?;

        con.set(key, value).await.map_err(|e| AppError::from(e))?;

        Ok(())
    }

    async fn clear(&self) -> Result<(), AppError> {
        trace!(
            "clear redis cache - deleting keys prefixed with '{}{}'",
            self.key_prefix,
            self.key_separator,
        );

        let mut con = self
            .redis_pool
            .get()
            .await
            .map_err(|e| AppError::Bb8RunError(e.to_string()))?;

        let keys_to_delete: Vec<String> = con
            .keys(format!("{}{}*", self.key_prefix, self.key_separator))
            .await
            .map_err(|e| AppError::from(e))?;

        if keys_to_delete.len() > 0 {
            con.del(keys_to_delete)
                .await
                .map_err(|e| AppError::from(e))?;
        }

        Ok(())
    }
}

impl CacheKeyFn for AsyncRedisCache {
    fn key_fn(&self, source_key: &str) -> String {
        format!("{}{}{}", self.key_prefix, self.key_separator, source_key)
    }
}

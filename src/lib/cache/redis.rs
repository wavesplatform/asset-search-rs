use itertools::Itertools;
use redis::Commands;
use serde::{de::DeserializeOwned, Serialize};
use std::fmt::Debug;
use wavesexchange_log::{debug, trace};

use super::{CacheKeyFn, SyncReadCache, SyncWriteCache};
use crate::{error::Error as AppError, redis::RedisPool};

#[derive(Clone)]
pub struct RedisCache {
    redis_pool: RedisPool,
    key_prefix: String,
}

pub fn new(redis_pool: RedisPool, key_prefix: String) -> RedisCache {
    RedisCache {
        redis_pool,
        key_prefix,
    }
}

impl<T> SyncReadCache<T> for RedisCache
where
    T: DeserializeOwned + Clone + Debug,
{
    fn get(&self, key: &str) -> Result<Option<T>, AppError> {
        let key = self.key_fn(key);

        trace!("get value from redis cache for key {}", key);

        let mut con = self.redis_pool.get()?;
        let value: Option<String> = con.get(key)?;
        debug!("value: {:?}", value);
        match value {
            Some(s) => serde_json::from_str(&s)
                .map(|v| Some(v))
                .map_err(|e| AppError::from(e)),
            _ => Ok(None),
        }
    }

    fn mget(&self, keys: &[&str]) -> Result<Vec<Option<T>>, AppError> {
        let keys = keys.into_iter().map(|k| self.key_fn(k)).collect::<Vec<_>>();

        trace!("mget values from redis cache for keys {:?}", keys);

        let mut con = self.redis_pool.get()?;
        match keys.len() {
            0 => Ok(vec![]),
            1 => {
                con.get(keys)
                    .map_err(|e| AppError::from(e))
                    .and_then(|m: Option<String>| match m {
                        Some(s) => {
                            let v = serde_json::from_str(&s)?;
                            Ok(vec![v])
                        }
                        _ => Ok(vec![None]),
                    })
            }
            _ => {
                con.get(keys)
                    .map_err(|e| AppError::from(e))
                    .and_then(|ms: Vec<Option<String>>| {
                        ms.into_iter()
                            .map(|m| match m {
                                Some(s) => serde_json::from_str(&s)
                                    .map(|v| Some(v))
                                    .map_err(|e| AppError::from(e)),
                                _ => Ok(None),
                            })
                            .try_collect()
                    })
            }
        }
    }
}

impl<T> SyncWriteCache<T> for RedisCache
where
    T: Serialize + DeserializeOwned + Clone + Debug,
{
    fn set(&self, key: &str, value: T) -> Result<(), AppError> {
        let key = self.key_fn(key);

        trace!("set redis cache value for key {}: {:?}", key, value);

        let mut con = self.redis_pool.get()?;
        let value = serde_json::to_string(&value)?;

        con.set(key, value).map_err(|e| AppError::from(e))?;

        Ok(())
    }

    fn clear(&self) -> Result<(), AppError> {
        trace!(
            "clear redis cache - keys prefixed with '{}'",
            self.key_prefix
        );

        let mut con = self.redis_pool.get()?;

        con.keys(format!("{}*", self.key_prefix))
            .and_then(|keys_to_delete: Vec<String>| con.del(keys_to_delete))
            .map_err(|e| AppError::from(e))?;

        Ok(())
    }
}

impl CacheKeyFn for RedisCache {
    fn key_fn(&self, source_key: &str) -> String {
        format!("{}:{}", self.key_prefix, source_key)
    }
}

mod error;
pub mod images;

use anyhow::{anyhow, Result};
use reqwest::Url;
use std::{str::FromStr, sync::Arc, time::Duration};

pub use error::Error; // reexport Error

#[derive(Clone)]
pub struct HttpClient {
    pub root_url: Url,
    pub client: Arc<reqwest::Client>,
    user_agent: Option<String>,
}

impl HttpClient {
    pub fn new(root_url: impl Into<String>) -> Result<Self> {
        let url = Url::from_str(&root_url.into()).map_err(|err| {
            anyhow!(
                "Couldn't parse root_url while initiating HttpClient: {}",
                err
            )
        })?;
        Ok(Self {
            root_url: url,
            client: Arc::new(
                reqwest::Client::builder()
                    .pool_max_idle_per_host(1)
                    .tcp_keepalive(Duration::from_secs(90))
                    .build()
                    .unwrap(),
            ),
            user_agent: None,
        })
    }

    pub fn with_user_agent(mut self, user_agent: impl Into<String>) -> Self {
        self.user_agent = Some(user_agent.into());
        self
    }
}

pub trait ApiBaseUrl {
    fn base_url(&self) -> String;
}

impl ApiBaseUrl for HttpClient {
    fn base_url(&self) -> String {
        self.root_url.to_string()
    }
}

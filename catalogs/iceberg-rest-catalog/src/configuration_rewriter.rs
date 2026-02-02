use crate::apis::configuration::Configuration;
use async_trait::async_trait;
use iceberg_rust::error::Error;

#[async_trait]
pub trait ConfigurationRewriter: std::fmt::Debug + Sync + Send {
    async fn rewrite_configuration(&self, conf: Configuration) -> Result<Configuration, Error>;
}

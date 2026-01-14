use crate::apis::configuration::Configuration;
use async_trait::async_trait;
use iceberg_rust::error::Error;

#[async_trait]
pub trait ConfigurationRewriter {
    async fn rewrite_configuration(&self, conf: Configuration) -> Result<Configuration, Error>;
}

impl std::fmt::Debug for dyn ConfigurationRewriter + Sync + Send {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

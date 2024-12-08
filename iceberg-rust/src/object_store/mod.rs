/*!
Defining the [Bucket] struct for specifying buckets for the ObjectStore.
*/

use std::{fmt::Display, ops::Deref, path::Path, str::FromStr, sync::Arc, time::SystemTime};

use async_trait::async_trait;
use aws_config::SdkConfig;
use aws_credential_types::{provider::ProvideCredentials, Credentials};
use futures::lock::Mutex;
use object_store::Error as ObjectStoreError;
use object_store::{
    aws::{AmazonS3Builder, AmazonS3ConfigKey, AwsCredential},
    gcp::{GoogleCloudStorageBuilder, GoogleConfigKey},
    local::LocalFileSystem,
    memory::InMemory,
    CredentialProvider, ObjectStore,
};

use crate::error::Error;

pub mod store;

/// Type for buckets for different cloud providers
#[derive(Debug)]
pub enum Bucket<'s> {
    /// Aws S3 bucket
    S3(&'s str),
    /// GCS bucket
    GCS(&'s str),
    /// No bucket
    Local,
}

impl Display for Bucket<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Bucket::S3(s) => write!(f, "s3://{}", s),
            Bucket::GCS(s) => write!(f, "gcs://{}", s),
            Bucket::Local => write!(f, ""),
        }
    }
}

impl Bucket<'_> {
    /// Get the bucket and coud provider from the location string
    pub fn from_path(path: &str) -> Result<Bucket, Error> {
        if path.starts_with("s3://") {
            path.trim_start_matches("s3://")
                .split('/')
                .next()
                .map(Bucket::S3)
                .ok_or(Error::NotFound(format!("Bucket in path {path}")))
        } else if path.starts_with("gcs://") {
            path.trim_start_matches("gcs://")
                .split('/')
                .next()
                .map(Bucket::GCS)
                .ok_or(Error::NotFound(format!("Bucket in path {path}")))
        } else {
            Ok(Bucket::Local)
        }
    }
}

/// A wrapper for ObjectStore builders that can be used as a template to generate an ObjectStore given a particular bucket.
#[derive(Debug, Clone)]
pub enum ObjectStoreBuilder {
    /// AWS s3 builder
    S3(AmazonS3Builder),
    /// Google Cloud Storage builder
    GCS(GoogleCloudStorageBuilder),
    /// Filesystem builder
    Filesystem(Arc<LocalFileSystem>),
    /// In memory builder
    Memory(Arc<InMemory>),
}

/// Configuration keys for [ObjectStoreBuilder]
pub enum ConfigKey {
    /// Configuration keys for AWS S3
    AWS(AmazonS3ConfigKey),
    /// Configuration keys for GCS
    GCS(GoogleConfigKey),
}

impl FromStr for ConfigKey {
    type Err = object_store::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Ok(x) = s.parse() {
            return Ok(ConfigKey::AWS(x));
        };
        if let Ok(x) = s.parse() {
            return Ok(ConfigKey::GCS(x));
        };
        Err(object_store::Error::UnknownConfigurationKey {
            store: "",
            key: s.to_string(),
        })
    }
}
impl ObjectStoreBuilder {
    /// Create new AWS S3 Object Store builder
    pub fn s3() -> Self {
        ObjectStoreBuilder::S3(AmazonS3Builder::from_env())
    }
    /// Create new AWS S3 Object Store builder
    pub fn gcs() -> Self {
        ObjectStoreBuilder::GCS(GoogleCloudStorageBuilder::from_env())
    }
    /// Create a new FileSystem ObjectStoreBuilder
    pub fn filesystem(prefix: impl AsRef<Path>) -> Self {
        ObjectStoreBuilder::Filesystem(Arc::new(LocalFileSystem::new_with_prefix(prefix).unwrap()))
    }
    /// Create a new InMemory ObjectStoreBuilder
    pub fn memory() -> Self {
        ObjectStoreBuilder::Memory(Arc::new(InMemory::new()))
    }
    /// Set config value for builder
    pub fn with_config(self, key: ConfigKey, value: impl Into<String>) -> Self {
        match (self, key) {
            (ObjectStoreBuilder::S3(aws), ConfigKey::AWS(key)) => {
                ObjectStoreBuilder::S3(aws.with_config(key, value))
            }
            (ObjectStoreBuilder::GCS(gcs), ConfigKey::GCS(key)) => {
                ObjectStoreBuilder::GCS(gcs.with_config(key, value))
            }
            (x, _) => x,
        }
    }
    /// Create objectstore from template
    pub fn build(&self, bucket: Bucket) -> Result<Arc<dyn ObjectStore>, Error> {
        match (bucket, self) {
            (Bucket::S3(bucket), Self::S3(builder)) => Ok::<_, Error>(Arc::new(
                builder
                    .clone()
                    .with_bucket_name(bucket)
                    .build()
                    .map_err(Error::from)?,
            )),
            (Bucket::GCS(bucket), Self::GCS(builder)) => Ok::<_, Error>(Arc::new(
                builder
                    .clone()
                    .with_bucket_name(bucket)
                    .build()
                    .map_err(Error::from)?,
            )),
            (Bucket::Local, Self::Filesystem(object_store)) => Ok(object_store.clone()),
            (Bucket::Local, Self::Memory(object_store)) => Ok(object_store.clone()),
            _ => Err(Error::NotSupported("Object store protocol".to_owned())),
        }
    }
}

/// AWS Sdk credential provider for object_store
#[derive(Debug)]
#[allow(clippy::type_complexity)]
pub struct AwsCredentialProvider {
    config: SdkConfig,
    cache: Arc<Mutex<Option<(Option<SystemTime>, Credentials)>>>,
}

#[async_trait]
impl CredentialProvider for AwsCredentialProvider {
    type Credential = AwsCredential;

    async fn get_credential(&self) -> Result<Arc<Self::Credential>, ObjectStoreError> {
        let mut guard = self.cache.lock().await;

        let is_valid = if let Some((Some(time), _)) = guard.deref() {
            *time >= SystemTime::now()
        } else {
            false
        };

        if !is_valid {
            let provider = self
                .config
                .credentials_provider()
                .ok_or(ObjectStoreError::NotImplemented)?;

            let credentials =
                provider
                    .provide_credentials()
                    .await
                    .map_err(|err| ObjectStoreError::Generic {
                        store: "s3",
                        source: Box::new(err),
                    })?;
            *guard = Some((credentials.expiry(), credentials));
        };

        let credentials = &guard.as_ref().unwrap().1;

        Ok(Arc::new(AwsCredential {
            key_id: credentials.access_key_id().to_string(),
            secret_key: credentials.secret_access_key().to_string(),
            token: credentials.session_token().map(ToString::to_string),
        }))
    }
}

impl AwsCredentialProvider {
    /// Create new credential provider
    pub fn new(config: &SdkConfig) -> Self {
        Self {
            config: config.clone(),
            cache: Arc::new(Mutex::new(None)),
        }
    }
}

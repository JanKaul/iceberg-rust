/*!
Defining the [Bucket] struct for specifying buckets for the ObjectStore.
*/

use std::{fmt::Display, path::Path, str::FromStr, sync::Arc};

use object_store::{
    aws::{AmazonS3Builder, AmazonS3ConfigKey, S3CopyIfNotExists},
    gcp::{GoogleCloudStorageBuilder, GoogleConfigKey},
    local::LocalFileSystem,
    memory::InMemory,
    ObjectStore,
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
            Bucket::GCS(s) => write!(f, "gs://{}", s),
            Bucket::Local => write!(f, ""),
        }
    }
}

impl Bucket<'_> {
    /// Get the bucket and coud provider from the location string
    pub fn from_path(path: &str) -> Result<Bucket, Error> {
        if path.starts_with("s3://") || path.starts_with("s3a://") {
            let prefix = if path.starts_with("s3://") { "s3://" } else { "s3a://" };
            path.trim_start_matches(prefix)
                .split('/')
                .next()
                .map(Bucket::S3)
                .ok_or(Error::NotFound(format!("Bucket in path {path}")))
        } else if path.starts_with("gcs://") || path.starts_with("gs://") {
            let prefix = if path.starts_with("gcs://") { "gcs://" } else { "gs://" };
            path.trim_start_matches(prefix)
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
                    .with_copy_if_not_exists(S3CopyIfNotExists::Multipart)
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

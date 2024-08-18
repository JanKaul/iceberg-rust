/*!
Defining the [Bucket] struct for specifying buckets for the ObjectStore.
*/

use std::{fmt::Display, sync::Arc};

use object_store::{
    aws::AmazonS3Builder, gcp::GoogleCloudStorageBuilder, local::LocalFileSystem, memory::InMemory,
    ObjectStore,
};

use crate::error::Error;

/// Type for buckets for different cloud providers
pub enum Bucket<'s> {
    /// Aws S3 bucket
    S3(&'s str),
    /// GCS bucket
    GCS(&'s str),
    /// No bucket
    Local,
}

impl<'s> Display for Bucket<'s> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Bucket::S3(s) => write!(f, "s3://{}", s),
            Bucket::GCS(s) => write!(f, "gcs://{}", s),
            Bucket::Local => write!(f, ""),
        }
    }
}

impl<'a> Bucket<'a> {
    /// Get the bucket and coud provider from the location string
    pub fn from_path(path: &str) -> Result<Bucket, Error> {
        if path.starts_with("s3://") {
            path.trim_start_matches("s3://")
                .split('/')
                .next()
                .map(Bucket::S3)
                .ok_or(Error::NotFound("Table".to_string(), "location".to_string()))
        } else if path.starts_with("gcs://") {
            path.trim_start_matches("gcs://")
                .split('/')
                .next()
                .map(Bucket::GCS)
                .ok_or(Error::NotFound("Table".to_string(), "location".to_string()))
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

impl ObjectStoreBuilder {
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

/*!
Defining the [Bucket] struct for specifying buckets for the ObjectStore.
*/

use std::sync::Arc;

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

/// Get the bucket and coud provider from the location string
pub fn parse_bucket(path: &str) -> Result<Bucket, Error> {
    if path.starts_with("s3://") {
        path.trim_start_matches("s3://")
            .split("/")
            .next()
            .map(Bucket::S3)
            .ok_or(Error::NotFound(format!("Table"), format!("location")))
    } else if path.starts_with("gcs://") {
        path.trim_start_matches("gcs://")
            .split("/")
            .next()
            .map(Bucket::GCS)
            .ok_or(Error::NotFound(format!("Table"), format!("location")))
    } else {
        Ok(Bucket::Local)
    }
}

/// A wrapper for ObjectStore builders that can be used as a template to generate an ObjectStore given a particular bucket.
#[derive(Debug)]
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

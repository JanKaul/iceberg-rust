/*!
Defining the [Bucket] struct for specifying buckets for the ObjectStore.
*/

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

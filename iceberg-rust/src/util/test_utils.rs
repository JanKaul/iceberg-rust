//! Test utilities for integration tests with LocalStack and S3

use crate::object_store::{Bucket, ObjectStoreBuilder};
use futures::StreamExt;
use std::time::Duration;

/// Wait for an S3 bucket to be ready in LocalStack by verifying it's accessible.
///
/// This function retries up to `max_retries` times with a 100ms delay between attempts.
/// It attempts to build an object store and list the bucket to verify readiness.
///
/// # Arguments
/// * `object_store` - The ObjectStoreBuilder configured for S3
/// * `bucket_path` - The S3 bucket path (e.g., "s3://warehouse")
/// * `max_retries` - Maximum number of retry attempts (default: 50)
///
/// # Panics
/// Panics if the bucket is not ready after max_retries attempts
pub async fn wait_for_s3_bucket(
    object_store: &ObjectStoreBuilder,
    bucket_path: &str,
    max_retries: Option<usize>,
) {
    let max_retries = max_retries.unwrap_or(50);
    let mut retries = 0;

    loop {
        match object_store.build(Bucket::from_path(bucket_path).unwrap()) {
            Ok(store) => {
                // Try to list the bucket to verify it's actually ready
                if store.list(None).next().await.is_some() || retries > 10 {
                    break;
                }
            }
            Err(_) if retries < max_retries => {
                tokio::time::sleep(Duration::from_millis(100)).await;
                retries += 1;
                continue;
            }
            Err(e) => panic!("Bucket not ready after {} retries: {:?}", max_retries, e),
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        retries += 1;
        if retries >= max_retries {
            break;
        }
    }
}

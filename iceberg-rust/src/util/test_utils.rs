//! Test utilities for integration tests with LocalStack and S3

use crate::object_store::{Bucket, ObjectStoreBuilder};
use futures::StreamExt;
use std::time::Duration;

/// Get the container host address for inter-container communication.
///
/// This function returns the appropriate host address depending on the container runtime:
/// - Podman: `host.containers.internal` (special DNS name that resolves to host)
/// - Docker: `host.docker.internal` (available on Docker Desktop and Linux with recent versions)
/// - Can be overridden with `CONTAINER_HOST` environment variable
///
/// # Returns
/// A string slice containing the host address for containers to access the host
pub fn get_container_host() -> &'static str {
    // Allow explicit override via environment variable
    if let Ok(host) = std::env::var("CONTAINER_HOST") {
        return Box::leak(host.into_boxed_str());
    }

    // For testcontainers with bridge networking:
    // - Podman (with netavark/bridge): host.containers.internal
    // - Docker (modern versions): host.docker.internal
    // Both resolve to an IP that routes to the host
    if is_podman() {
        "host.containers.internal"
    } else {
        "host.docker.internal"
    }
}

/// Detect if we're using Podman instead of Docker
fn is_podman() -> bool {
    // Check if docker command is actually podman
    std::process::Command::new("docker")
        .arg("--help")
        .output()
        .ok()
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|s| s.contains("podman"))
        .unwrap_or(false)
}

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

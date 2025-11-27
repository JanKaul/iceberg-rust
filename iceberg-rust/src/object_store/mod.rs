/*!
Defining the [Bucket] struct for specifying buckets for the ObjectStore.
*/

use std::{fmt::Display, path::Path, str::FromStr, sync::Arc};

use object_store::{
    aws::{AmazonS3Builder, AmazonS3ConfigKey, S3CopyIfNotExists},
    azure::{AzureConfigKey, MicrosoftAzureBuilder},
    gcp::{GoogleCloudStorageBuilder, GoogleConfigKey},
    local::LocalFileSystem,
    memory::InMemory,
    ObjectStore,
};

use crate::error::Error;

pub mod parse;
pub mod store;

/// Type for buckets for different cloud providers
#[derive(Debug)]
pub enum Bucket<'s> {
    /// Aws S3 bucket
    S3(&'s str),
    /// GCS bucket
    GCS(&'s str),
    /// Azure
    Azure {
        /// Account name
        account: &'s str,
        /// Container name
        container: &'s str,
    },
    /// No bucket
    Local,
}

impl Display for Bucket<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Bucket::S3(s) => write!(f, "s3://{s}"),
            Bucket::GCS(s) => write!(f, "gs://{s}"),
            Bucket::Azure { account, container } => {
                write!(f, "abfss://{container}@{account}.dfs.core.windows.net/")
            }
            Bucket::Local => write!(f, ""),
        }
    }
}

impl Bucket<'_> {
    /// Get the bucket and cloud provider from the location string
    pub fn from_path(path: &str) -> Result<Bucket<'_>, Error> {
        let extract_prefix = |path: &str| {
            path.split("://")
                .next()
                .map(|p| format!("{}://", p))
                .unwrap_or_default()
        };

        if path.starts_with("s3://") || path.starts_with("s3a://") {
            let prefix = extract_prefix(path);
            path.trim_start_matches(prefix.as_str())
                .split('/')
                .next()
                .map(Bucket::S3)
                .ok_or(Error::NotFound(format!("Bucket in path {path}")))
        } else if path.starts_with("gcs://") || path.starts_with("gs://") {
            let prefix = extract_prefix(path);
            path.trim_start_matches(prefix.as_str())
                .split('/')
                .next()
                .map(Bucket::GCS)
                .ok_or(Error::NotFound(format!("Bucket in path {path}")))
        } else if path.starts_with("az://")
            || path.starts_with("adl://")
            || path.starts_with("azure://")
        {
            // Format: az://container/path or adl://container/path or azure://container/path
            let prefix = extract_prefix(path);
            let container = path
                .trim_start_matches(prefix.as_str())
                .split('/')
                .next()
                .ok_or(Error::NotFound(format!("Container in path {path}")))?;
            Ok(Bucket::Azure {
                account: "",
                container,
            })
        } else if path.starts_with("abfs://") || path.starts_with("abfss://") {
            let prefix = extract_prefix(path);
            let remainder = path.trim_start_matches(prefix.as_str());

            if remainder.contains('@') {
                // Format: abfs[s]://file_system@account_name.dfs.core.windows.net/path
                let container = remainder
                    .split('@')
                    .next()
                    .ok_or(Error::NotFound(format!("Container in path {path}")))?;
                let account = remainder
                    .split('@')
                    .nth(1)
                    .and_then(|s| s.split('.').next())
                    .ok_or(Error::NotFound(format!("Account in path {path}")))?;
                Ok(Bucket::Azure { account, container })
            } else {
                // Format: abfs[s]://container/path
                let container = remainder
                    .split('/')
                    .next()
                    .ok_or(Error::NotFound(format!("Container in path {path}")))?;
                Ok(Bucket::Azure {
                    account: "",
                    container,
                })
            }
        } else if path.starts_with("https://")
            && (path.contains("dfs.core.windows.net")
                || path.contains("blob.core.windows.net")
                || path.contains("dfs.fabric.microsoft.com")
                || path.contains("blob.fabric.microsoft.com"))
        {
            // Format: https://account.dfs.core.windows.net/container/path
            let remainder = path.trim_start_matches("https://");
            let account = remainder
                .split('.')
                .next()
                .ok_or(Error::NotFound(format!("Account in path {path}")))?;
            let container = remainder.split('/').nth(1).unwrap_or("");
            Ok(Bucket::Azure { account, container })
        } else {
            Ok(Bucket::Local)
        }
    }
}

/// A wrapper for ObjectStore builders that can be used as a template to generate an ObjectStore given a particular bucket.
#[derive(Debug, Clone)]
pub enum ObjectStoreBuilder {
    /// Microsoft Azure builder
    Azure(Box<MicrosoftAzureBuilder>),
    /// AWS s3 builder
    S3(Box<AmazonS3Builder>),
    /// Google Cloud Storage builder
    GCS(Box<GoogleCloudStorageBuilder>),
    /// Filesystem builder
    Filesystem(Arc<LocalFileSystem>),
    /// In memory builder
    Memory(Arc<InMemory>),
}

/// Configuration keys for [ObjectStoreBuilder]
pub enum ConfigKey {
    /// Configuration keys for Microsoft Azure
    Azure(AzureConfigKey),
    /// Configuration keys for AWS S3
    AWS(AmazonS3ConfigKey),
    /// Configuration keys for GCS
    GCS(GoogleConfigKey),
}

impl FromStr for ConfigKey {
    type Err = object_store::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Ok(x) = s.parse() {
            return Ok(ConfigKey::Azure(x));
        };
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
    /// Create a new Microsoft Azure ObjectStoreBuilder
    pub fn azure() -> Self {
        ObjectStoreBuilder::Azure(Box::new(MicrosoftAzureBuilder::from_env()))
    }
    /// Create new AWS S3 Object Store builder
    pub fn s3() -> Self {
        ObjectStoreBuilder::S3(Box::new(AmazonS3Builder::from_env()))
    }
    /// Create new AWS S3 Object Store builder
    pub fn gcs() -> Self {
        ObjectStoreBuilder::GCS(Box::new(GoogleCloudStorageBuilder::from_env()))
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
    pub fn with_config(
        self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<Self, Error> {
        match self {
            ObjectStoreBuilder::Azure(azure) => {
                let key: AzureConfigKey = key.into().parse()?;
                Ok(ObjectStoreBuilder::Azure(Box::new(
                    azure.with_config(key, value),
                )))
            }
            ObjectStoreBuilder::S3(aws) => {
                let key: AmazonS3ConfigKey = key.into().parse()?;
                Ok(ObjectStoreBuilder::S3(Box::new(
                    aws.with_config(key, value),
                )))
            }
            ObjectStoreBuilder::GCS(gcs) => {
                let key: GoogleConfigKey = key.into().parse()?;
                Ok(ObjectStoreBuilder::GCS(Box::new(
                    gcs.with_config(key, value),
                )))
            }
            x => Ok(x),
        }
    }
    /// Create objectstore from template
    pub fn build(&self, bucket: Bucket) -> Result<Arc<dyn ObjectStore>, Error> {
        match (bucket, self) {
            (Bucket::Azure { account, container }, Self::Azure(builder)) => {
                Ok::<_, Error>(Arc::new(
                    (**builder)
                        .clone()
                        .with_account(account)
                        .with_container_name(container)
                        .build()
                        .map_err(Error::from)?,
                ))
            }
            (Bucket::S3(bucket), Self::S3(builder)) => Ok::<_, Error>(Arc::new(
                (**builder)
                    .clone()
                    .with_bucket_name(bucket)
                    .with_copy_if_not_exists(S3CopyIfNotExists::Multipart)
                    .build()
                    .map_err(Error::from)?,
            )),
            (Bucket::GCS(bucket), Self::GCS(builder)) => Ok::<_, Error>(Arc::new(
                (**builder)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_path_s3() {
        let bucket = Bucket::from_path("s3://my-bucket/path/to/file").unwrap();
        match bucket {
            Bucket::S3(name) => assert_eq!(name, "my-bucket"),
            _ => panic!("Expected S3 bucket"),
        }
    }

    #[test]
    fn test_from_path_s3a() {
        let bucket = Bucket::from_path("s3a://my-bucket/path/to/file").unwrap();
        match bucket {
            Bucket::S3(name) => assert_eq!(name, "my-bucket"),
            _ => panic!("Expected S3 bucket"),
        }
    }

    #[test]
    fn test_from_path_gcs() {
        let bucket = Bucket::from_path("gcs://my-bucket/path/to/file").unwrap();
        match bucket {
            Bucket::GCS(name) => assert_eq!(name, "my-bucket"),
            _ => panic!("Expected GCS bucket"),
        }
    }

    #[test]
    fn test_from_path_gs() {
        let bucket = Bucket::from_path("gs://my-bucket/path/to/file").unwrap();
        match bucket {
            Bucket::GCS(name) => assert_eq!(name, "my-bucket"),
            _ => panic!("Expected GCS bucket"),
        }
    }

    #[test]
    fn test_from_path_azure_abfs_simple() {
        let bucket = Bucket::from_path("abfs://container/path").unwrap();
        match bucket {
            Bucket::Azure { account, container } => {
                assert_eq!(account, "");
                assert_eq!(container, "container");
            }
            _ => panic!("Expected Azure bucket"),
        }
    }

    #[test]
    fn test_from_path_azure_abfss_simple() {
        let bucket = Bucket::from_path("abfss://container/path").unwrap();
        match bucket {
            Bucket::Azure { account, container } => {
                assert_eq!(account, "");
                assert_eq!(container, "container");
            }
            _ => panic!("Expected Azure bucket"),
        }
    }

    #[test]
    fn test_from_path_azure_abfs_with_account() {
        let bucket = Bucket::from_path(
            "abfs://myfilesystem@mystorageaccount.dfs.core.windows.net/path/to/file",
        )
        .unwrap();
        match bucket {
            Bucket::Azure { account, container } => {
                assert_eq!(account, "mystorageaccount");
                assert_eq!(container, "myfilesystem");
            }
            _ => panic!("Expected Azure bucket"),
        }
    }

    #[test]
    fn test_from_path_azure_abfss_with_account() {
        let bucket = Bucket::from_path(
            "abfss://myfilesystem@mystorageaccount.dfs.core.windows.net/path/to/file",
        )
        .unwrap();
        match bucket {
            Bucket::Azure { account, container } => {
                assert_eq!(account, "mystorageaccount");
                assert_eq!(container, "myfilesystem");
            }
            _ => panic!("Expected Azure bucket"),
        }
    }

    #[test]
    fn test_from_path_azure_abfs_fabric() {
        let bucket = Bucket::from_path(
            "abfs://myfilesystem@mystorageaccount.dfs.fabric.microsoft.com/path/to/file",
        )
        .unwrap();
        match bucket {
            Bucket::Azure { account, container } => {
                assert_eq!(account, "mystorageaccount");
                assert_eq!(container, "myfilesystem");
            }
            _ => panic!("Expected Azure bucket"),
        }
    }

    #[test]
    fn test_from_path_azure_abfss_fabric() {
        let bucket = Bucket::from_path(
            "abfss://myfilesystem@mystorageaccount.dfs.fabric.microsoft.com/path/to/file",
        )
        .unwrap();
        match bucket {
            Bucket::Azure { account, container } => {
                assert_eq!(account, "mystorageaccount");
                assert_eq!(container, "myfilesystem");
            }
            _ => panic!("Expected Azure bucket"),
        }
    }

    #[test]
    fn test_from_path_azure_az() {
        let bucket = Bucket::from_path("az://container/path/to/file").unwrap();
        match bucket {
            Bucket::Azure { account, container } => {
                assert_eq!(account, "");
                assert_eq!(container, "container");
            }
            _ => panic!("Expected Azure bucket"),
        }
    }

    #[test]
    fn test_from_path_azure_adl() {
        let bucket = Bucket::from_path("adl://container/path/to/file").unwrap();
        match bucket {
            Bucket::Azure { account, container } => {
                assert_eq!(account, "");
                assert_eq!(container, "container");
            }
            _ => panic!("Expected Azure bucket"),
        }
    }

    #[test]
    fn test_from_path_azure_azure_scheme() {
        let bucket = Bucket::from_path("azure://container/path/to/file").unwrap();
        match bucket {
            Bucket::Azure { account, container } => {
                assert_eq!(account, "");
                assert_eq!(container, "container");
            }
            _ => panic!("Expected Azure bucket"),
        }
    }

    #[test]
    fn test_from_path_azure_https_dfs_core() {
        let bucket = Bucket::from_path("https://mystorageaccount.dfs.core.windows.net").unwrap();
        match bucket {
            Bucket::Azure { account, container } => {
                assert_eq!(account, "mystorageaccount");
                assert_eq!(container, "");
            }
            _ => panic!("Expected Azure bucket"),
        }
    }

    #[test]
    fn test_from_path_azure_https_blob_core() {
        let bucket = Bucket::from_path("https://mystorageaccount.blob.core.windows.net").unwrap();
        match bucket {
            Bucket::Azure { account, container } => {
                assert_eq!(account, "mystorageaccount");
                assert_eq!(container, "");
            }
            _ => panic!("Expected Azure bucket"),
        }
    }

    #[test]
    fn test_from_path_azure_https_blob_core_with_container() {
        let bucket =
            Bucket::from_path("https://mystorageaccount.blob.core.windows.net/container").unwrap();
        match bucket {
            Bucket::Azure { account, container } => {
                assert_eq!(account, "mystorageaccount");
                assert_eq!(container, "container");
            }
            _ => panic!("Expected Azure bucket"),
        }
    }

    #[test]
    fn test_from_path_azure_https_dfs_fabric() {
        let bucket =
            Bucket::from_path("https://mystorageaccount.dfs.fabric.microsoft.com").unwrap();
        match bucket {
            Bucket::Azure { account, container } => {
                assert_eq!(account, "mystorageaccount");
                assert_eq!(container, "");
            }
            _ => panic!("Expected Azure bucket"),
        }
    }

    #[test]
    fn test_from_path_azure_https_dfs_fabric_with_container() {
        let bucket =
            Bucket::from_path("https://mystorageaccount.dfs.fabric.microsoft.com/container")
                .unwrap();
        match bucket {
            Bucket::Azure { account, container } => {
                assert_eq!(account, "mystorageaccount");
                assert_eq!(container, "container");
            }
            _ => panic!("Expected Azure bucket"),
        }
    }

    #[test]
    fn test_from_path_azure_https_blob_fabric() {
        let bucket =
            Bucket::from_path("https://mystorageaccount.blob.fabric.microsoft.com").unwrap();
        match bucket {
            Bucket::Azure { account, container } => {
                assert_eq!(account, "mystorageaccount");
                assert_eq!(container, "");
            }
            _ => panic!("Expected Azure bucket"),
        }
    }

    #[test]
    fn test_from_path_azure_https_blob_fabric_with_container() {
        let bucket =
            Bucket::from_path("https://mystorageaccount.blob.fabric.microsoft.com/container")
                .unwrap();
        match bucket {
            Bucket::Azure { account, container } => {
                assert_eq!(account, "mystorageaccount");
                assert_eq!(container, "container");
            }
            _ => panic!("Expected Azure bucket"),
        }
    }

    #[test]
    fn test_from_path_local() {
        let bucket = Bucket::from_path("/local/path/to/file").unwrap();
        match bucket {
            Bucket::Local => {}
            _ => panic!("Expected Local bucket"),
        }
    }

    #[test]
    fn test_from_path_https_non_azure() {
        let bucket = Bucket::from_path("https://example.com/path").unwrap();
        match bucket {
            Bucket::Local => {}
            _ => panic!("Expected Local bucket"),
        }
    }
}

/*! Helpers for Intarting with object storage
*/
use async_trait::async_trait;
use iceberg_rust_spec::{
    tabular::{TabularMetadata, TabularMetadataRef},
    util::strip_prefix,
};
use object_store::ObjectStore;

use crate::error::Error;

/// Simplify interaction with iceberg files
#[async_trait]
pub trait IcebergStore {
    /// Get metadata file from object_storage
    async fn get_metadata(&self, location: &str) -> Result<TabularMetadata, Error>;
    /// Write metadata file to object_storage
    async fn put_metadata(
        &self,
        location: &str,
        metadata: TabularMetadataRef<'_>,
    ) -> Result<(), Error>;
}

#[async_trait]
impl<T: ObjectStore> IcebergStore for T {
    async fn get_metadata(&self, location: &str) -> Result<TabularMetadata, Error> {
        let bytes = self
            .get(&strip_prefix(location).into())
            .await?
            .bytes()
            .await?;
        serde_json::from_slice(&bytes).map_err(Error::from)
    }

    async fn put_metadata(
        &self,
        location: &str,
        metadata: TabularMetadataRef<'_>,
    ) -> Result<(), Error> {
        self.put(
            &strip_prefix(location).into(),
            serde_json::to_vec(&metadata)?.into(),
        )
        .await?;
        Ok(())
    }
}

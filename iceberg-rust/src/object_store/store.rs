/*! Helpers for Intarting with object storage
*/
use async_trait::async_trait;
use iceberg_rust_spec::{
    tabular::{TabularMetadata, TabularMetadataRef},
    util::strip_prefix,
};
use object_store::{Attributes, ObjectStore, PutOptions, TagSet};

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
    /// Write version-hint file to object_storage
    async fn put_version_hint(&self, location: &str) -> Result<(), Error>;
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

    async fn put_version_hint(&self, location: &str) -> Result<(), Error> {
        self.put_opts(
            &version_hint_path(&strip_prefix(&location))
                .ok_or(Error::InvalidFormat(format!(
                    "Path for version-hint for {location}"
                )))?
                .into(),
            location.to_string().into(),
            PutOptions {
                mode: object_store::PutMode::Overwrite,
                tags: TagSet::default(),
                attributes: Attributes::default(),
            },
        )
        .await?;

        Ok(())
    }
}

fn version_hint_path(original: &str) -> Option<String> {
    Some(
        std::path::Path::new(original)
            .parent()?
            .join("version-hint.text")
            .to_str()?
            .to_string(),
    )
}

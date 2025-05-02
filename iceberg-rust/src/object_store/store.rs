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
            &version_hint_path(&strip_prefix(location))
                .ok_or(Error::InvalidFormat(format!(
                    "Path for version-hint for {location}"
                )))?
                .into(),
            location.to_string().into(),
            PutOptions {
                mode: object_store::PutMode::Overwrite,
                tags: TagSet::default(),
                attributes: Attributes::default(),
                extensions: Default::default(),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version_hint_path_normal_case() {
        let input = "/path/to/metadata/v1.metadata.json";
        let expected = "/path/to/metadata/version-hint.text";
        assert_eq!(version_hint_path(input), Some(expected.to_string()));
    }

    #[test]
    fn test_version_hint_path_relative() {
        let input = "path/to/metadata/v1.metadata.json";
        let expected = "path/to/metadata/version-hint.text";
        assert_eq!(version_hint_path(input), Some(expected.to_string()));
    }

    #[test]
    fn test_version_hint_path_single_file() {
        let input = "file.json";
        let expected = "version-hint.text";
        assert_eq!(version_hint_path(input), Some(expected.to_string()));
    }

    #[test]
    fn test_version_hint_path_empty_string() {
        let input = "";
        assert_eq!(version_hint_path(input), None);
    }

    #[test]
    fn test_version_hint_path_with_special_characters() {
        let input = "/path/with spaces/and#special@chars/file.json";
        let expected = "/path/with spaces/and#special@chars/version-hint.text";
        assert_eq!(version_hint_path(input), Some(expected.to_string()));
    }

    #[test]
    fn test_version_hint_path_with_multiple_extensions() {
        let input = "/path/to/file.with.multiple.extensions.json";
        let expected = "/path/to/version-hint.text";
        assert_eq!(version_hint_path(input), Some(expected.to_string()));
    }
}

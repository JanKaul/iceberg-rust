/*! Helpers for Intarting with object storage
*/
use async_trait::async_trait;
use iceberg_rust_spec::{
    tabular::{TabularMetadata, TabularMetadataRef},
    util::strip_prefix,
};
use object_store::{Attributes, ObjectStore, PutOptions, TagSet};

use crate::error::Error;
use flate2::read::GzDecoder;
use lazy_static::lazy_static;
use regex::Regex;
use std::io::Read;

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

        parse_metadata(location, &bytes)
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
            version_hint_content(location).into(),
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

lazy_static! {
    static ref SUPPORTED_METADATA_FILE_FORMATS: Vec<Regex> = vec![
        // The standard metastore format https://iceberg.apache.org/spec/#metastore-tables
        Regex::new(
            r"^(?<version>[0-9]{5}-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}).(?:gz.)?metadata.json$"
        )
        .unwrap(),
        // The legacy file-system format https://iceberg.apache.org/spec/#file-system-tables
        Regex::new(r"^v(?<version>[0-9]+).metadata.json$").unwrap(),
    ];
}

/// Given a full path to a metadata file, extract an appropriate version hint that other readers
/// without access to the catalog can parse.
pub fn version_hint_content(original: &str) -> String {
    original
        .split("/")
        .last()
        .and_then(|filename| {
            SUPPORTED_METADATA_FILE_FORMATS
                .iter()
                .filter_map(|regex| {
                    regex.captures(filename).and_then(|capture| {
                        capture
                            .name("version")
                            .and_then(|m| m.as_str().parse().ok())
                    })
                })
                .next()
        })
        .unwrap_or(original.to_string())
}

fn parse_metadata(location: &str, bytes: &[u8]) -> Result<TabularMetadata, Error> {
    if location.ends_with(".gz.metadata.json") {
        let mut decoder = GzDecoder::new(bytes);
        let mut decompressed_data = Vec::new();
        decoder
            .read_to_end(&mut decompressed_data)
            .map_err(|e| Error::Decompress(e.to_string()))?;
        serde_json::from_slice(&decompressed_data).map_err(Error::from)
    } else {
        serde_json::from_slice(bytes).map_err(Error::from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;
    use std::io::Write;

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

    #[rstest]
    #[case::file_format("/path/to/metadata/v2.metadata.json", "2")]
    #[case::metastore_format_no_gzip(
        "/path/to/metadata/00004-3f569e94-5601-48f3-9199-8d71df4ea7b0.metadata.json",
        "00004-3f569e94-5601-48f3-9199-8d71df4ea7b0"
    )]
    #[case::metastore_format_with_gzip(
        "/path/to/metadata/00004-3f569e94-5601-48f3-9199-8d71df4ea7b0.gz.metadata.json",
        "00004-3f569e94-5601-48f3-9199-8d71df4ea7b0"
    )]
    #[test]
    fn test_version_hint_content(#[case] input: &str, #[case] expected: &str) {
        assert_eq!(version_hint_content(input), expected);
    }

    #[test]
    fn test_parse_metadata_table_plain_json() {
        let location = "/path/to/metadata/v1.metadata.json";
        let json_data = r#"
            {
                "format-version" : 2,
                "table-uuid": "fb072c92-a02b-11e9-ae9c-1bb7bc9eca94",
                "location": "s3://b/wh/data.db/table",
                "last-sequence-number" : 1,
                "last-updated-ms": 1515100955770,
                "last-column-id": 1,
                "schemas": [
                    {
                        "schema-id" : 1,
                        "type" : "struct",
                        "fields" :[
                            {
                                "id": 1,
                                "name": "struct_name",
                                "required": true,
                                "type": "fixed[1]"
                            }
                        ]
                    }
                ],
                "current-schema-id" : 1,
                "partition-specs": [
                    {
                        "spec-id": 1,
                        "fields": [
                            {  
                                "source-id": 4,  
                                "field-id": 1000,  
                                "name": "ts_day",  
                                "transform": "day"
                            } 
                        ]
                    }
                ],
                "default-spec-id": 1,
                "last-partition-id": 1,
                "properties": {
                    "commit.retry.num-retries": "1"
                },
                "metadata-log": [
                    {  
                        "metadata-file": "s3://bucket/.../v1.json",  
                        "timestamp-ms": 1515100
                    }
                ],
                "sort-orders": [],
                "default-sort-order-id": 0
            }
        "#;
        let bytes = json_data.as_bytes();

        let result = parse_metadata(location, bytes);
        assert!(result.is_ok());
        let metadata = result.unwrap();
        if let TabularMetadata::Table(table_metadata) = metadata {
            // Add specific checks for `table_metadata` fields if needed
            assert_eq!(
                table_metadata.table_uuid.to_string(),
                "fb072c92-a02b-11e9-ae9c-1bb7bc9eca94"
            );
        } else {
            panic!("Expected TabularMetadata::Table variant");
        }
    }

    #[test]
    fn test_parse_metadata_table_gzipped_json() {
        let location = "/path/to/metadata/v1.gz.metadata.json";
        let json_data = r#"
            {
                "format-version" : 2,
                "table-uuid": "fb072c92-a02b-11e9-ae9c-1bb7bc9eca94",
                "location": "s3://b/wh/data.db/table",
                "last-sequence-number" : 1,
                "last-updated-ms": 1515100955770,
                "last-column-id": 1,
                "schemas": [
                    {
                        "schema-id" : 1,
                        "type" : "struct",
                        "fields" :[
                            {
                                "id": 1,
                                "name": "struct_name",
                                "required": true,
                                "type": "fixed[1]"
                            }
                        ]
                    }
                ],
                "current-schema-id" : 1,
                "partition-specs": [
                    {
                        "spec-id": 1,
                        "fields": [
                            {  
                                "source-id": 4,  
                                "field-id": 1000,  
                                "name": "ts_day",  
                                "transform": "day"
                            } 
                        ]
                    }
                ],
                "default-spec-id": 1,
                "last-partition-id": 1,
                "properties": {
                    "commit.retry.num-retries": "1"
                },
                "metadata-log": [
                    {  
                        "metadata-file": "s3://bucket/.../v1.json",  
                        "timestamp-ms": 1515100
                    }
                ],
                "sort-orders": [],
                "default-sort-order-id": 0
            }
        "#;

        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(json_data.as_bytes()).unwrap();
        let compressed_data = encoder.finish().unwrap();

        let result = parse_metadata(location, &compressed_data);
        assert!(result.is_ok());
        let metadata = result.unwrap();
        if let TabularMetadata::Table(table_metadata) = metadata {
            assert_eq!(
                table_metadata.table_uuid.to_string(),
                "fb072c92-a02b-11e9-ae9c-1bb7bc9eca94"
            );
        } else {
            panic!("Expected TabularMetadata::Table variant");
        }
    }

    #[test]
    fn test_parse_metadata_view_plain_json() {
        let location = "/path/to/metadata/v1.metadata.json";
        let json_data = r#"
        {
        "view-uuid": "fa6506c3-7681-40c8-86dc-e36561f83385",
        "format-version" : 1,
        "location" : "s3://bucket/warehouse/default.db/event_agg",
        "current-version-id" : 1,
        "properties" : {
            "comment" : "Daily event counts"
        },
        "versions" : [ {
            "version-id" : 1,
            "timestamp-ms" : 1573518431292,
            "schema-id" : 1,
            "default-catalog" : "prod",
            "default-namespace" : [ "default" ],
            "summary" : {
            "operation" : "create",
            "engine-name" : "Spark",
            "engineVersion" : "3.3.2"
            },
            "representations" : [ {
            "type" : "sql",
            "sql" : "SELECT\n    COUNT(1), CAST(event_ts AS DATE)\nFROM events\nGROUP BY 2",
            "dialect" : "spark"
            } ]
        } ],
        "schemas": [ {
            "schema-id": 1,
            "type" : "struct",
            "fields" : [ {
            "id" : 1,
            "name" : "event_count",
            "required" : false,
            "type" : "int",
            "doc" : "Count of events"
            }, {
            "id" : 2,
            "name" : "event_date",
            "required" : false,
            "type" : "date"
            } ]
        } ],
        "version-log" : [ {
            "timestamp-ms" : 1573518431292,
            "version-id" : 1
        } ]
        }
        "#;
        let bytes = json_data.as_bytes();

        let result = parse_metadata(location, bytes);
        assert!(result.is_ok());
        let metadata = result.unwrap();
        if let TabularMetadata::View(view_metadata) = metadata {
            assert_eq!(
                view_metadata.view_uuid.to_string(),
                "fa6506c3-7681-40c8-86dc-e36561f83385"
            );
        } else {
            panic!("Expected TabularMetadata::View variant");
        }
    }

    #[test]
    fn test_parse_metadata_view_gzipped_json() {
        let location = "/path/to/metadata/v1.gz.metadata.json";
        let json_data = r#"
        {
        "view-uuid": "fa6506c3-7681-40c8-86dc-e36561f83385",
        "format-version" : 1,
        "location" : "s3://bucket/warehouse/default.db/event_agg",
        "current-version-id" : 1,
        "properties" : {
            "comment" : "Daily event counts"
        },
        "versions" : [ {
            "version-id" : 1,
            "timestamp-ms" : 1573518431292,
            "schema-id" : 1,
            "default-catalog" : "prod",
            "default-namespace" : [ "default" ],
            "summary" : {
            "operation" : "create",
            "engine-name" : "Spark",
            "engineVersion" : "3.3.2"
            },
            "representations" : [ {
            "type" : "sql",
            "sql" : "SELECT\n    COUNT(1), CAST(event_ts AS DATE)\nFROM events\nGROUP BY 2",
            "dialect" : "spark"
            } ]
        } ],
        "schemas": [ {
            "schema-id": 1,
            "type" : "struct",
            "fields" : [ {
            "id" : 1,
            "name" : "event_count",
            "required" : false,
            "type" : "int",
            "doc" : "Count of events"
            }, {
            "id" : 2,
            "name" : "event_date",
            "required" : false,
            "type" : "date"
            } ]
        } ],
        "version-log" : [ {
            "timestamp-ms" : 1573518431292,
            "version-id" : 1
        } ]
        }
        "#;

        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(json_data.as_bytes()).unwrap();
        let compressed_data = encoder.finish().unwrap();

        let result = parse_metadata(location, &compressed_data);
        assert!(result.is_ok());
        let metadata = result.unwrap();
        if let TabularMetadata::View(view_metadata) = metadata {
            assert_eq!(
                view_metadata.view_uuid.to_string(),
                "fa6506c3-7681-40c8-86dc-e36561f83385"
            );
        } else {
            panic!("Expected TabularMetadata::View variant");
        }
    }

    #[test]
    fn test_parse_metadata_invalid_json() {
        let location = "/path/to/metadata/v1.metadata.json";
        let invalid_json_data = r#"{"key": "value""#;
        let bytes = invalid_json_data.as_bytes();

        let result = parse_metadata(location, bytes);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_metadata_invalid_gzipped_data() {
        let location = "/path/to/metadata/v1.gz.metadata.json";
        let invalid_gzipped_data = b"not a valid gzip";

        let result = parse_metadata(location, invalid_gzipped_data);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_metadata_empty_bytes() {
        let location = "/path/to/metadata/v1.metadata.json";
        let empty_bytes: &[u8] = &[];

        let result = parse_metadata(location, empty_bytes);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_metadata_gzipped_empty_bytes() {
        let location = "/path/to/metadata/v1.gz.metadata.json";
        let empty_gzipped_bytes: &[u8] = &[];

        let result = parse_metadata(location, empty_gzipped_bytes);
        assert!(result.is_err());
    }
}

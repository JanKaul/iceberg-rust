/*!
 * Helper for iterating over files in a table.
*/
use std::{io::Cursor, iter::repeat, sync::Arc};

use anyhow::Result;
use apache_avro::types::Value as AvroValue;
use futures::{stream, StreamExt, TryFutureExt, TryStreamExt};
use object_store::path::Path;

use crate::{
    model::{
        manifest::{ManifestEntry, ManifestEntryV1, ManifestEntryV2},
        manifest_list::ManifestFile,
        table_metadata::FormatVersion,
    },
    util,
};

use super::Table;

impl Table {
    /// Get the data_files associated to a table. The files are returned based on the list of manifest files associated to that table.
    /// The included manifest files can be filtered based on an filter vector. The filter vector has the length equal to the number of manifest files
    /// and contains a true entry everywhere the manifest file is to be included in the output.
    pub async fn files(&self, filter: Option<Vec<bool>>) -> Result<Vec<ManifestEntry>> {
        // filter manifest files according to filter vector
        let iter = match filter {
            Some(predicate) => {
                self.manifests()
                    .iter()
                    .zip(Box::new(predicate.into_iter())
                        as Box<dyn Iterator<Item = bool> + Send + Sync>)
                    .filter_map(
                        filter_manifest as fn((&ManifestFile, bool)) -> Option<&ManifestFile>,
                    )
            }
            None => self
                .manifests()
                .iter()
                .zip(Box::new(repeat(true)) as Box<dyn Iterator<Item = bool> + Send + Sync>)
                .filter_map(filter_manifest as fn((&ManifestFile, bool)) -> Option<&ManifestFile>),
        };
        // Collect a vector of data files by creating a stream over the manifst files, fetch their content and return a flatten stream over their entries.
        stream::iter(iter)
            .map(|file| async move {
                let object_store = Arc::clone(&self.object_store());
                let path: Path = util::strip_prefix(file.manifest_path()).into();
                let bytes = Cursor::new(Vec::from(
                    object_store
                        .get(&path)
                        .and_then(|file| file.bytes())
                        .await?,
                ));
                let reader = apache_avro::Reader::new(bytes)?;
                Ok(stream::iter(reader.map(|record| {
                    avro_value_to_manifest_entry(record, &self.metadata().format_version())
                })))
            })
            .flat_map(|reader| reader.try_flatten_stream())
            .try_collect()
            .await
    }
}

// Filter manifest files according to predicate. Returns Some(&ManifestFile) of the predicate is true and None if it is false.
fn filter_manifest((manifest, predicate): (&ManifestFile, bool)) -> Option<&ManifestFile> {
    if predicate {
        Some(manifest)
    } else {
        None
    }
}

// Convert avro value to ManifestEntry based on the format version of the table.
fn avro_value_to_manifest_entry(
    entry: Result<AvroValue, apache_avro::Error>,
    format_version: &FormatVersion,
) -> Result<ManifestEntry, anyhow::Error> {
    match format_version {
        FormatVersion::V1 => entry
            .and_then(|value| apache_avro::from_value::<ManifestEntryV1>(&value))
            .map(ManifestEntry::V1)
            .map_err(anyhow::Error::msg),
        FormatVersion::V2 => entry
            .and_then(|value| apache_avro::from_value::<ManifestEntryV2>(&value))
            .map(ManifestEntry::V2)
            .map_err(anyhow::Error::msg),
    }
}

#[cfg(test)]
mod tests {

    use object_store::{memory::InMemory, ObjectStore};
    use std::sync::Arc;

    use crate::{
        model::{
            data_types::{PrimitiveType, StructField, StructType, Type},
            schema::SchemaV2,
        },
        table::table_builder::TableBuilder,
    };

    #[tokio::test]
    async fn test_files_stream() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let schema = SchemaV2 {
            schema_id: 1,
            identifier_field_ids: Some(vec![1, 2]),
            fields: StructType {
                fields: vec![
                    StructField {
                        id: 1,
                        name: "one".to_string(),
                        required: false,
                        field_type: Type::Primitive(PrimitiveType::String),
                        doc: None,
                    },
                    StructField {
                        id: 2,
                        name: "two".to_string(),
                        required: false,
                        field_type: Type::Primitive(PrimitiveType::String),
                        doc: None,
                    },
                ],
            },
        };
        let mut table =
            TableBuilder::new_filesystem_table("test/append", schema, Arc::clone(&object_store))
                .unwrap()
                .commit()
                .await
                .unwrap();

        table
            .new_transaction()
            .fast_append(vec![
                "test/append/data/file1.parquet".to_string(),
                "test/append/data/file2.parquet".to_string(),
            ])
            .commit()
            .await
            .unwrap();
        table
            .new_transaction()
            .fast_append(vec![
                "test/append/data/file3.parquet".to_string(),
                "test/append/data/file4.parquet".to_string(),
            ])
            .commit()
            .await
            .unwrap();
        let mut files = table
            .files(None)
            .await
            .unwrap()
            .into_iter()
            .map(|manifest_entry| manifest_entry.file_path().to_string());
        assert_eq!(
            files.next().unwrap(),
            "test/append/data/file1.parquet".to_string()
        );
        assert_eq!(
            files.next().unwrap(),
            "test/append/data/file2.parquet".to_string()
        );
        assert_eq!(
            files.next().unwrap(),
            "test/append/data/file3.parquet".to_string()
        );
        assert_eq!(
            files.next().unwrap(),
            "test/append/data/file4.parquet".to_string()
        );
    }
}

/*!
 * Defines the different [Operation]s on a [Table].
*/

use anyhow::{anyhow, Result};
use object_store::path::Path;

use crate::{
    model::{
        manifest::{
            partition_value_schema, Content, DataFileV1, DataFileV2, FileFormat, ManifestEntry,
            ManifestEntryV1, ManifestEntryV2, Status,
        },
        manifest_list::{FieldSummary, ManifestFile, ManifestFileV1, ManifestFileV2},
        schema::SchemaV2,
        table_metadata::TableMetadata,
        values::Struct,
    },
    table::Table,
};

///Table operations
pub enum Operation {
    /// Update schema
    UpdateSchema(SchemaV2),
    /// Update spec
    UpdateSpec(i32),
    // /// Update table properties
    // UpdateProperties,
    // /// Replace the sort order
    // ReplaceSortOrder,
    // /// Update the table location
    // UpdateLocation,
    // /// Append new files to the table
    // NewAppend,
    /// Quickly append new files to the table
    NewFastAppend(Vec<String>),
    // /// Replace files in the table and commit
    // NewRewrite,
    // /// Replace manifests files and commit
    // RewriteManifests,
    // /// Replace files in the table by a filter expression
    // NewOverwrite,
    // /// Remove or replace rows in existing data files
    // NewRowDelta,
    // /// Delete files in the table and commit
    // NewDelete,
    // /// Expire snapshots in the table
    // ExpireSnapshots,
    // /// Manage snapshots in the table
    // ManageSnapshots,
    // /// Read and write table data and metadata files
    // IO,
}

impl Operation {
    pub async fn execute(self, table: &mut Table) -> Result<()> {
        match self {
            Operation::NewFastAppend(paths) => {
                let object_store = table.object_store();
                let table_metadata = table.metadata();
                let manifest_bytes = match table_metadata {
                    TableMetadata::V1(metadata) => {
                        let manifest_schema =
                            apache_avro::Schema::parse_str(&ManifestEntry::schema(
                                &partition_value_schema(
                                    table_metadata.default_spec(),
                                    table_metadata.current_schema(),
                                )?,
                                &table_metadata.format_version(),
                            ))?;
                        let mut manifest_writer =
                            apache_avro::Writer::new(&manifest_schema, Vec::new());
                        for path in paths {
                            let manifest_entry = ManifestEntryV1 {
                                status: Status::Added,
                                snapshot_id: metadata.current_snapshot_id.unwrap_or(1),
                                data_file: DataFileV1 {
                                    file_path: path,
                                    file_format: FileFormat::Parquet,
                                    partition: Struct::from_iter(
                                        table_metadata
                                            .default_spec()
                                            .iter()
                                            .map(|field| (field.name.to_owned(), None)),
                                    ),
                                    record_count: 4,
                                    file_size_in_bytes: 1200,
                                    block_size_in_bytes: 400,
                                    file_ordinal: None,
                                    sort_columns: None,
                                    column_sizes: None,
                                    value_counts: None,
                                    null_value_counts: None,
                                    nan_value_counts: None,
                                    distinct_counts: None,
                                    lower_bounds: None,
                                    upper_bounds: None,
                                    key_metadata: None,
                                    split_offsets: None,
                                    sort_order_id: None,
                                },
                            };
                            manifest_writer.append_ser(manifest_entry)?;
                        }
                        manifest_writer.into_inner()?
                    }
                    TableMetadata::V2(metadata) => {
                        let manifest_schema =
                            apache_avro::Schema::parse_str(&ManifestEntry::schema(
                                &partition_value_schema(
                                    table_metadata.default_spec(),
                                    table_metadata.current_schema(),
                                )?,
                                &table_metadata.format_version(),
                            ))?;
                        let mut manifest_writer =
                            apache_avro::Writer::new(&manifest_schema, Vec::new());
                        for path in paths {
                            let manifest_entry = ManifestEntry::V2(ManifestEntryV2 {
                                status: Status::Added,
                                snapshot_id: metadata.current_snapshot_id,
                                sequence_number: metadata
                                    .snapshots
                                    .as_ref()
                                    .map(|snapshots| snapshots.last().unwrap().sequence_number),
                                data_file: DataFileV2 {
                                    content: Content::Data,
                                    file_path: path,
                                    file_format: FileFormat::Parquet,
                                    partition: Struct::from_iter(
                                        table_metadata
                                            .default_spec()
                                            .iter()
                                            .map(|field| (field.name.to_owned(), None)),
                                    ),
                                    record_count: 4,
                                    file_size_in_bytes: 1200,
                                    column_sizes: None,
                                    value_counts: None,
                                    null_value_counts: None,
                                    nan_value_counts: None,
                                    distinct_counts: None,
                                    lower_bounds: None,
                                    upper_bounds: None,
                                    key_metadata: None,
                                    split_offsets: None,
                                    equality_ids: None,
                                    sort_order_id: None,
                                },
                            });
                            manifest_writer.append_ser(manifest_entry)?;
                        }
                        manifest_writer.into_inner()?
                    }
                };
                let manifest_list_location: Path = table_metadata
                    .manifest_list()
                    .ok_or_else(|| anyhow!("No manifest list in table metadata."))?
                    .into();
                let manifest_location: Path = (manifest_list_location
                    .to_string()
                    .trim_end_matches(".avro")
                    .to_owned()
                    + "-m0.avro")
                    .into();
                object_store
                    .put(&manifest_location, manifest_bytes.into())
                    .await?;
                let manifest_list_bytes = match table_metadata {
                    TableMetadata::V1(_) => {
                        let manifest_list_schema = apache_avro::Schema::parse_str(
                            &ManifestFile::schema(&table_metadata.format_version()),
                        )?;
                        let mut manifest_list_writer =
                            apache_avro::Writer::new(&manifest_list_schema, Vec::new());
                        let bytes: Vec<u8> = object_store
                            .get(&manifest_list_location)
                            .await?
                            .bytes()
                            .await?
                            .into();
                        if !bytes.is_empty() {
                            let reader = apache_avro::Reader::new(&*bytes)?;
                            manifest_list_writer.extend(reader.filter_map(Result::ok))?;
                        }
                        let manifest_file = ManifestFile::V1(ManifestFileV1 {
                            manifest_path: manifest_location.to_string(),
                            manifest_length: 1200,
                            partition_spec_id: 0,
                            added_snapshot_id: 39487483032,
                            added_files_count: Some(1),
                            existing_files_count: Some(2),
                            deleted_files_count: Some(0),
                            added_rows_count: Some(1000),
                            existing_rows_count: Some(8000),
                            deleted_rows_count: Some(0),
                            partitions: Some(vec![FieldSummary {
                                contains_null: true,
                                contains_nan: Some(false),
                                lower_bound: None,
                                upper_bound: None,
                            }]),
                            key_metadata: None,
                        });
                        manifest_list_writer.append_ser(manifest_file)?;

                        manifest_list_writer.into_inner()?
                    }
                    TableMetadata::V2(_) => {
                        let manifest_list_schema = apache_avro::Schema::parse_str(
                            &ManifestFile::schema(&table_metadata.format_version()),
                        )?;
                        let mut manifest_list_writer =
                            apache_avro::Writer::new(&manifest_list_schema, Vec::new());
                        let bytes: Vec<u8> = object_store
                            .get(&manifest_list_location)
                            .await?
                            .bytes()
                            .await?
                            .into();
                        if !bytes.is_empty() {
                            let reader = apache_avro::Reader::new(&*bytes)?;
                            manifest_list_writer.extend(reader.filter_map(Result::ok))?;
                        }
                        let manifest_file = ManifestFile::V2(ManifestFileV2 {
                            manifest_path: manifest_location.to_string(),
                            manifest_length: 1200,
                            partition_spec_id: 0,
                            content: Content::Data,
                            sequence_number: 566,
                            min_sequence_number: 0,
                            added_snapshot_id: 39487483032,
                            added_files_count: 1,
                            existing_files_count: 2,
                            deleted_files_count: 0,
                            added_rows_count: 1000,
                            existing_rows_count: 8000,
                            deleted_rows_count: 0,
                            partitions: Some(vec![FieldSummary {
                                contains_null: true,
                                contains_nan: Some(false),
                                lower_bound: None,
                                upper_bound: None,
                            }]),
                            key_metadata: None,
                        });
                        manifest_list_writer.append_ser(manifest_file)?;

                        manifest_list_writer.into_inner()?
                    }
                };
                object_store
                    .put(&manifest_list_location, manifest_list_bytes.into())
                    .await?;
                Ok(())
            }
            _ => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {

    use std::sync::Arc;

    use object_store::{memory::InMemory, ObjectStore};

    use crate::{
        model::{
            data_types::{PrimitiveType, StructField, StructType, Type},
            schema::SchemaV2,
        },
        table::table_builder::TableBuilder,
    };

    #[tokio::test]
    async fn test_append_files() {
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

        let metadata_location = table.metadata_location();
        assert_eq!(metadata_location, "test/append/metadata/v1.metadata.json");

        let transaction = table.new_transaction();
        transaction
            .fast_append(vec![
                "test/append/data/file1.parquet".to_string(),
                "test/append/data/file2.parquet".to_string(),
            ])
            .commit()
            .await
            .unwrap();
        let metadata_location = table.metadata_location();
        assert_eq!(metadata_location, "test/append/metadata/v2.metadata.json");
    }
}

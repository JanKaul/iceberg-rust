/*!
 * Helpers to deal with manifest files
*/

use std::sync::Arc;

use apache_avro::{Schema as AvroSchema, Writer as AvroWriter};
use iceberg_rust_spec::{
    manifest::ManifestEntry,
    manifest_list::{self, FieldSummary, ManifestListEntry},
    partition::PartitionField,
    schema::{SchemaV1, SchemaV2},
    table_metadata::{FormatVersion, TableMetadata},
    util::strip_prefix,
    values::{Struct, Value},
};
use object_store::ObjectStore;

use crate::error::Error;

/// A helper to write entries into a manifest
pub struct ManifestWriter<'schema, 'metadata> {
    table_metadata: &'metadata TableMetadata,
    manifest: ManifestListEntry,
    writer: AvroWriter<'schema, Vec<u8>>,
}

impl<'schema, 'metadata> ManifestWriter<'schema, 'metadata> {
    /// Create empty manifest writer
    pub fn new(
        manifest_location: &str,
        snapshot_id: i64,
        schema: &'schema AvroSchema,
        table_metadata: &'metadata TableMetadata,
        branch: Option<&str>,
    ) -> Result<Self, Error> {
        let mut writer = AvroWriter::new(schema, Vec::new());

        writer.add_user_metadata(
            "format-version".to_string(),
            match table_metadata.format_version {
                FormatVersion::V1 => "1".as_bytes(),
                FormatVersion::V2 => "2".as_bytes(),
            },
        )?;

        writer.add_user_metadata(
            "schema".to_string(),
            match table_metadata.format_version {
                FormatVersion::V1 => serde_json::to_string(&Into::<SchemaV1>::into(
                    table_metadata.current_schema(branch)?.clone(),
                ))?,
                FormatVersion::V2 => serde_json::to_string(&Into::<SchemaV2>::into(
                    table_metadata.current_schema(branch)?.clone(),
                ))?,
            },
        )?;

        writer.add_user_metadata(
            "schema-id".to_string(),
            serde_json::to_string(&table_metadata.current_schema(branch)?.schema_id())?,
        )?;

        writer.add_user_metadata(
            "partition-spec".to_string(),
            serde_json::to_string(&table_metadata.default_partition_spec()?.fields())?,
        )?;

        writer.add_user_metadata(
            "partition-spec-id".to_string(),
            serde_json::to_string(&table_metadata.default_partition_spec()?.spec_id())?,
        )?;

        writer.add_user_metadata("content".to_string(), "data")?;

        let manifest = ManifestListEntry {
            format_version: table_metadata.format_version,
            manifest_path: manifest_location.to_owned(),
            manifest_length: 0,
            partition_spec_id: table_metadata.default_spec_id,
            content: manifest_list::Content::Data,
            sequence_number: table_metadata.last_sequence_number,
            min_sequence_number: 0,
            added_snapshot_id: snapshot_id,
            added_files_count: Some(0),
            existing_files_count: Some(0),
            deleted_files_count: Some(0),
            added_rows_count: Some(0),
            existing_rows_count: Some(0),
            deleted_rows_count: Some(0),
            partitions: None,
            key_metadata: None,
        };

        Ok(ManifestWriter {
            manifest,
            writer,
            table_metadata,
        })
    }

    /// Create an manifest writer from an existing manifest
    pub fn from_existing(
        bytes: &[u8],
        manifest: ManifestListEntry,
        schema: &'schema AvroSchema,
        table_metadata: &'metadata TableMetadata,
        branch: Option<&str>,
    ) -> Result<Self, Error> {
        let manifest_reader = apache_avro::Reader::new(bytes)?;

        let mut writer = AvroWriter::new(schema, Vec::new());

        writer.add_user_metadata(
            "format-version".to_string(),
            match table_metadata.format_version {
                FormatVersion::V1 => "1".as_bytes(),
                FormatVersion::V2 => "2".as_bytes(),
            },
        )?;

        writer.add_user_metadata(
            "schema".to_string(),
            match table_metadata.format_version {
                FormatVersion::V1 => serde_json::to_string(&Into::<SchemaV1>::into(
                    table_metadata.current_schema(branch)?.clone(),
                ))?,
                FormatVersion::V2 => serde_json::to_string(&Into::<SchemaV2>::into(
                    table_metadata.current_schema(branch)?.clone(),
                ))?,
            },
        )?;

        writer.add_user_metadata(
            "schema-id".to_string(),
            serde_json::to_string(&table_metadata.current_schema(branch)?.schema_id())?,
        )?;

        writer.add_user_metadata(
            "partition-spec".to_string(),
            serde_json::to_string(&table_metadata.default_partition_spec()?.fields())?,
        )?;

        writer.add_user_metadata(
            "partition-spec-id".to_string(),
            serde_json::to_string(&table_metadata.default_partition_spec()?.spec_id())?,
        )?;

        writer.add_user_metadata("content".to_string(), "data")?;

        writer.extend(manifest_reader.filter_map(Result::ok))?;

        Ok(ManifestWriter {
            manifest,
            writer,
            table_metadata,
        })
    }

    /// Add an manifest entry to the manifest
    pub fn append(&mut self, manifest_entry: ManifestEntry) -> Result<(), Error> {
        let mut added_rows_count = 0;

        if self.manifest.partitions.is_none() {
            self.manifest.partitions = Some(
                self.table_metadata
                    .default_partition_spec()?
                    .fields()
                    .iter()
                    .map(|_| FieldSummary {
                        contains_null: false,
                        contains_nan: None,
                        lower_bound: None,
                        upper_bound: None,
                    })
                    .collect::<Vec<FieldSummary>>(),
            );
        }

        added_rows_count += manifest_entry.data_file().record_count();
        update_partitions(
            self.manifest.partitions.as_mut().unwrap(),
            manifest_entry.data_file().partition(),
            self.table_metadata.default_partition_spec()?.fields(),
        )?;

        self.writer.append_ser(manifest_entry)?;

        self.manifest.added_files_count = match self.manifest.added_files_count {
            Some(count) => Some(count + 1),
            None => Some(1),
        };
        self.manifest.added_rows_count = match self.manifest.added_rows_count {
            Some(count) => Some(count + added_rows_count),
            None => Some(added_rows_count),
        };

        Ok(())
    }

    /// Write the manifest to object storage and return the manifest-list-entry
    pub async fn write(
        mut self,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<ManifestListEntry, Error> {
        let manifest_bytes = self.writer.into_inner()?;

        let manifest_length: i64 = manifest_bytes.len() as i64;

        self.manifest.manifest_length += manifest_length;

        object_store
            .put(
                &strip_prefix(&self.manifest.manifest_path).as_str().into(),
                manifest_bytes.into(),
            )
            .await?;
        Ok(self.manifest)
    }
}

fn update_partitions(
    partitions: &mut [FieldSummary],
    partition_values: &Struct,
    partition_columns: &[PartitionField],
) -> Result<(), Error> {
    for (field, summary) in partition_columns.iter().zip(partitions.iter_mut()) {
        let value = partition_values.get(field.name()).and_then(|x| x.as_ref());
        if let Some(value) = value {
            if summary.lower_bound.is_none() {
                summary.lower_bound = Some(value.clone());
            } else if let Some(lower_bound) = &mut summary.lower_bound {
                match (value, lower_bound) {
                    (Value::Int(val), Value::Int(current)) => {
                        if *current > *val {
                            *current = *val
                        }
                    }
                    (Value::LongInt(val), Value::LongInt(current)) => {
                        if *current > *val {
                            *current = *val
                        }
                    }
                    (Value::Float(val), Value::Float(current)) => {
                        if *current > *val {
                            *current = *val
                        }
                    }
                    (Value::Double(val), Value::Double(current)) => {
                        if *current > *val {
                            *current = *val
                        }
                    }
                    (Value::Date(val), Value::Date(current)) => {
                        if *current > *val {
                            *current = *val
                        }
                    }
                    (Value::Time(val), Value::Time(current)) => {
                        if *current > *val {
                            *current = *val
                        }
                    }
                    (Value::Timestamp(val), Value::Timestamp(current)) => {
                        if *current > *val {
                            *current = *val
                        }
                    }
                    (Value::TimestampTZ(val), Value::TimestampTZ(current)) => {
                        if *current > *val {
                            *current = *val
                        }
                    }
                    _ => {}
                }
            }
            if summary.upper_bound.is_none() {
                summary.upper_bound = Some(value.clone());
            } else if let Some(upper_bound) = &mut summary.upper_bound {
                match (value, upper_bound) {
                    (Value::Int(val), Value::Int(current)) => {
                        if *current < *val {
                            *current = *val
                        }
                    }
                    (Value::LongInt(val), Value::LongInt(current)) => {
                        if *current < *val {
                            *current = *val
                        }
                    }
                    (Value::Float(val), Value::Float(current)) => {
                        if *current < *val {
                            *current = *val
                        }
                    }
                    (Value::Double(val), Value::Double(current)) => {
                        if *current < *val {
                            *current = *val
                        }
                    }
                    (Value::Date(val), Value::Date(current)) => {
                        if *current < *val {
                            *current = *val
                        }
                    }
                    (Value::Time(val), Value::Time(current)) => {
                        if *current < *val {
                            *current = *val
                        }
                    }
                    (Value::Timestamp(val), Value::Timestamp(current)) => {
                        if *current < *val {
                            *current = *val
                        }
                    }
                    (Value::TimestampTZ(val), Value::TimestampTZ(current)) => {
                        if *current < *val {
                            *current = *val
                        }
                    }
                    _ => {}
                }
            }
        }
    }
    Ok(())
}

/// TODO
#[cfg(test)]
mod tests {}

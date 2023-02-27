/*!
 * Defines the different [Operation]s on a [Table].
*/

use std::{cell::RefCell, collections::HashSet, rc::Rc, sync::Arc};

use anyhow::{anyhow, Result};
use apache_avro::from_value;
use futures::{lock::Mutex, stream, StreamExt, TryStreamExt};
use object_store::path::Path;
use serde_bytes::ByteBuf;

use crate::{
    file_format::parquet::{parquet_metadata, parquet_to_datafilev2},
    model::{
        bytes::bytes_to_any,
        data_types::{PrimitiveType, Type},
        manifest::{
            partition_value_schema, Content, DataFileV1, ManifestEntry, ManifestEntryV1,
            ManifestEntryV2, Status,
        },
        manifest_list::{FieldSummary, ManifestFile, ManifestFileV1, ManifestFileV2},
        partition::PartitionField,
        schema::SchemaV2,
        table_metadata::{FormatVersion, TableMetadata},
        values::{Struct, Value},
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
    /// Append new files to the table
    NewAppend {
        partinioned_paths: Vec<Vec<String>>,
        partition_values: Vec<Struct>,
    },
    /// Quickly append new files to the table
    NewFastAppend {
        paths: Vec<String>,
        partition_values: Vec<Struct>,
    },
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
            Operation::NewFastAppend {
                paths,
                partition_values,
            } => {
                let object_store = table.object_store();
                let table_metadata = table.metadata();
                let files_count = paths.len() as i32;

                let files_metadata = stream::iter(paths.iter())
                    .then(|x| {
                        let object_store = object_store.clone();
                        async move { parquet_metadata(x, object_store).await }
                    })
                    .try_collect::<Vec<_>>()
                    .await?;

                let mut added_rows_count = 0;
                let mut partitions = table_metadata
                    .default_spec()
                    .iter()
                    .map(|_| FieldSummary {
                        contains_null: false,
                        contains_nan: None,
                        lower_bound: None,
                        upper_bound: None,
                    })
                    .collect::<Vec<FieldSummary>>();

                let manifest_list_location: Path = table_metadata
                    .manifest_list()
                    .ok_or_else(|| anyhow!("No manifest list in table metadata."))?
                    .into();

                let manifest_list_bytes: Vec<u8> = object_store
                    .get(&manifest_list_location)
                    .await?
                    .bytes()
                    .await?
                    .into();

                let manifest_list_reader = apache_avro::Reader::new(&*manifest_list_bytes)?;

                let manifest_count = apache_avro::Reader::new(&*manifest_list_bytes)?.count();

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
                        for (file, partition_values) in
                            files_metadata.into_iter().zip(partition_values.into_iter())
                        {
                            let data_file = parquet_to_datafilev2(
                                file.0,
                                file.1,
                                file.2,
                                partition_values,
                                table_metadata.current_schema(),
                            )?;
                            added_rows_count += data_file.record_count;
                            update_partitions(
                                &mut partitions,
                                &data_file.partition,
                                table_metadata.default_spec(),
                            )?;
                            let manifest_entry = ManifestEntry::V1(ManifestEntryV1 {
                                status: Status::Added,
                                snapshot_id: metadata.current_snapshot_id.unwrap_or(1),
                                data_file: DataFileV1 {
                                    file_path: data_file.file_path,
                                    file_format: data_file.file_format,
                                    partition: data_file.partition,
                                    record_count: data_file.record_count,
                                    file_size_in_bytes: data_file.file_size_in_bytes,
                                    block_size_in_bytes: data_file.file_size_in_bytes,
                                    file_ordinal: None,
                                    sort_columns: None,
                                    column_sizes: data_file.column_sizes,
                                    value_counts: data_file.value_counts,
                                    null_value_counts: data_file.null_value_counts,
                                    nan_value_counts: data_file.nan_value_counts,
                                    distinct_counts: data_file.distinct_counts,
                                    lower_bounds: data_file.lower_bounds,
                                    upper_bounds: data_file.upper_bounds,
                                    key_metadata: data_file.key_metadata,
                                    split_offsets: data_file.split_offsets,
                                    sort_order_id: data_file.sort_order_id,
                                },
                            });

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
                        for (file, partition_values) in
                            files_metadata.into_iter().zip(partition_values.into_iter())
                        {
                            let data_file = parquet_to_datafilev2(
                                file.0,
                                file.1,
                                file.2,
                                partition_values,
                                table_metadata.current_schema(),
                            )?;
                            added_rows_count += data_file.record_count;
                            update_partitions(
                                &mut partitions,
                                &data_file.partition,
                                table_metadata.default_spec(),
                            )?;
                            let manifest_entry = ManifestEntry::V2(ManifestEntryV2 {
                                status: Status::Added,
                                snapshot_id: metadata.current_snapshot_id,
                                sequence_number: metadata
                                    .snapshots
                                    .as_ref()
                                    .map(|snapshots| snapshots.last().unwrap().sequence_number),
                                data_file,
                            });

                            manifest_writer.append_ser(manifest_entry)?;
                        }
                        manifest_writer.into_inner()?
                    }
                };
                let manifest_length: i64 = manifest_bytes.len() as i64;

                let manifest_location: Path = (manifest_list_location
                    .to_string()
                    .trim_end_matches(".avro")
                    .to_owned()
                    + "-m"
                    + &manifest_count.to_string()
                    + ".avro")
                    .into();
                object_store
                    .put(&manifest_location, manifest_bytes.into())
                    .await?;
                let manifest_list_bytes = match table_metadata {
                    TableMetadata::V1(table_metadata) => {
                        let manifest_list_schema = apache_avro::Schema::parse_str(
                            &ManifestFile::schema(&FormatVersion::V1),
                        )?;
                        let mut manifest_list_writer =
                            apache_avro::Writer::new(&manifest_list_schema, Vec::new());
                        if !manifest_list_bytes.is_empty() {
                            manifest_list_writer
                                .extend(manifest_list_reader.filter_map(Result::ok))?;
                        }
                        let manifest_file = ManifestFile::V1(ManifestFileV1 {
                            manifest_path: manifest_location.to_string(),
                            manifest_length,
                            partition_spec_id: table_metadata.default_spec_id.unwrap_or_default(),
                            added_snapshot_id: table_metadata.current_snapshot_id.unwrap(),
                            added_files_count: Some(files_count),
                            existing_files_count: Some(0),
                            deleted_files_count: Some(0),
                            added_rows_count: Some(added_rows_count),
                            existing_rows_count: Some(0),
                            deleted_rows_count: Some(0),
                            partitions: Some(partitions),
                            key_metadata: None,
                        });
                        manifest_list_writer.append_ser(manifest_file)?;

                        manifest_list_writer.into_inner()?
                    }
                    TableMetadata::V2(table_metadata) => {
                        let manifest_list_schema = apache_avro::Schema::parse_str(
                            &ManifestFile::schema(&FormatVersion::V2),
                        )?;
                        let mut manifest_list_writer =
                            apache_avro::Writer::new(&manifest_list_schema, Vec::new());

                        if !manifest_list_bytes.is_empty() {
                            manifest_list_writer
                                .extend(manifest_list_reader.filter_map(Result::ok))?;
                        }
                        let manifest_file = ManifestFile::V2(ManifestFileV2 {
                            manifest_path: manifest_location.to_string(),
                            manifest_length,
                            partition_spec_id: table_metadata.default_spec_id,
                            content: Content::Data,
                            sequence_number: table_metadata.last_sequence_number,
                            min_sequence_number: 0,
                            added_snapshot_id: table_metadata
                                .current_snapshot_id
                                .unwrap_or_default(),
                            added_files_count: files_count,
                            existing_files_count: 0,
                            deleted_files_count: 0,
                            added_rows_count,
                            existing_rows_count: 0,
                            deleted_rows_count: 0,
                            partitions: Some(partitions),
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
            Operation::NewAppend {
                partinioned_paths,
                partition_values,
            } => {
                let object_store = table.object_store();
                let table_metadata = table.metadata();

                let partition_values_value = partition_values
                    .iter()
                    .map(|values| {
                        table_metadata
                            .default_spec()
                            .iter()
                            .map(|field| {
                                values
                                    .lookup
                                    .get(&field.name)
                                    .ok_or_else(|| {
                                        anyhow!(
                                            "Partition values missing for column {}.",
                                            &field.name
                                        )
                                    })
                                    .map(|index| values[*index].clone())
                            })
                            .collect::<Result<Vec<_>>>()
                    })
                    .collect::<Result<Vec<_>>>()?;

                let manifest_list_location: Path = table_metadata
                    .manifest_list()
                    .ok_or_else(|| anyhow!("No manifest list in table metadata."))?
                    .into();

                let manifest_list_bytes: Vec<u8> = object_store
                    .get(&manifest_list_location)
                    .await?
                    .bytes()
                    .await?
                    .into();

                let manifest_list_reader = apache_avro::Reader::new(&*manifest_list_bytes)?;

                let manifest_count = apache_avro::Reader::new(&*manifest_list_bytes)?.count();

                let existing_manifests = Rc::new(RefCell::new(HashSet::new()));

                let manifest_list_schema = apache_avro::Schema::parse_str(&ManifestFile::schema(
                    &table_metadata.format_version(),
                ))?;

                let manifest_list_writer = Arc::new(Mutex::new(apache_avro::Writer::new(
                    &manifest_list_schema,
                    Vec::new(),
                )));

                let existing_manifest_iter = manifest_list_reader.filter_map(|manifest| {
                    let manifest = manifest
                        .and_then(|value| from_value::<ManifestFile>(&value))
                        .unwrap();
                    if let Some(summary) = manifest.partitions() {
                        if let Some(key) =
                            partition_values_in_bounds(summary, &partition_values_value)
                        {
                            let files: Vec<(Vec<String>, Struct)> = partition_values_value
                                .iter()
                                .enumerate()
                                .filter_map(|(i, values)| {
                                    if key == values
                                        && !existing_manifests
                                            .borrow()
                                            .contains(&partinioned_paths[i])
                                    {
                                        existing_manifests
                                            .borrow_mut()
                                            .insert(partinioned_paths[i].clone());
                                        Some((
                                            partinioned_paths[i].clone(),
                                            partition_values[i].clone(),
                                        ))
                                    } else {
                                        None
                                    }
                                })
                                .collect();
                            Some((Ok(manifest), files))
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                });

                let new_manifest_iter = partinioned_paths
                    .iter()
                    .zip(partition_values.iter())
                    .filter_map(|(paths, values)| {
                        if !existing_manifests.borrow().contains(paths) {
                            let manifest_location = manifest_list_location
                                .to_string()
                                .trim_end_matches(".avro")
                                .to_owned()
                                + "-m"
                                + &manifest_count.to_string()
                                + ".avro";
                            let manifest = match &table_metadata {
                                TableMetadata::V1(table_metadata) => {
                                    ManifestFile::V1(ManifestFileV1 {
                                        manifest_path: manifest_location,
                                        manifest_length: 0,
                                        partition_spec_id: table_metadata
                                            .default_spec_id
                                            .unwrap_or_default(),
                                        added_snapshot_id: table_metadata
                                            .current_snapshot_id
                                            .unwrap(),
                                        added_files_count: Some(0),
                                        existing_files_count: Some(0),
                                        deleted_files_count: Some(0),
                                        added_rows_count: Some(0),
                                        existing_rows_count: Some(0),
                                        deleted_rows_count: Some(0),
                                        partitions: None,
                                        key_metadata: None,
                                    })
                                }
                                TableMetadata::V2(table_metadata) => {
                                    ManifestFile::V2(ManifestFileV2 {
                                        manifest_path: manifest_location,
                                        manifest_length: 0,
                                        partition_spec_id: table_metadata.default_spec_id,
                                        content: Content::Data,
                                        sequence_number: table_metadata.last_sequence_number,
                                        min_sequence_number: 0,
                                        added_snapshot_id: table_metadata
                                            .current_snapshot_id
                                            .unwrap_or_default(),
                                        added_files_count: 0,
                                        existing_files_count: 0,
                                        deleted_files_count: 0,
                                        added_rows_count: 0,
                                        existing_rows_count: 0,
                                        deleted_rows_count: 0,
                                        partitions: None,
                                        key_metadata: None,
                                    })
                                }
                            };
                            Some((Err(manifest), vec![(paths.clone(), values.clone())]))
                        } else {
                            None
                        }
                    });

                let manifest_iter = existing_manifest_iter.chain(new_manifest_iter);

                stream::iter(manifest_iter)
                    .then(|(manifest, files)| {
                        let object_store = object_store.clone();
                        async move {
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

                            let mut manifest = match manifest {
                                Ok(manifest) => {
                                    let manifest_bytes: Vec<u8> = object_store
                                        .get(&manifest.manifest_path().into())
                                        .await?
                                        .bytes()
                                        .await?
                                        .into();

                                    let manifest_reader =
                                        apache_avro::Reader::new(&*manifest_bytes)?;
                                    manifest_writer
                                        .extend(manifest_reader.filter_map(Result::ok))?;
                                    manifest
                                }
                                Err(manifest) => manifest,
                            };
                            for (paths, partition_value) in files {
                                let files_count = manifest.added_files_count().unwrap_or_default()
                                    + paths.len() as i32;

                                let files_metadata = stream::iter(paths.iter())
                                    .then(|x| {
                                        let object_store = object_store.clone();
                                        async move { parquet_metadata(x, object_store).await }
                                    })
                                    .try_collect::<Vec<_>>()
                                    .await?;

                                let mut added_rows_count = 0;
                                let mut partitions = match manifest.partitions() {
                                    Some(partitions) => partitions.clone(),
                                    None => table_metadata
                                        .default_spec()
                                        .iter()
                                        .map(|_| FieldSummary {
                                            contains_null: false,
                                            contains_nan: None,
                                            lower_bound: None,
                                            upper_bound: None,
                                        })
                                        .collect::<Vec<FieldSummary>>(),
                                };

                                match table_metadata {
                                    TableMetadata::V1(metadata) => {
                                        for file in files_metadata {
                                            let data_file = parquet_to_datafilev2(
                                                file.0,
                                                file.1,
                                                file.2,
                                                partition_value.clone(),
                                                table_metadata.current_schema(),
                                            )?;
                                            added_rows_count += data_file.record_count;
                                            update_partitions(
                                                &mut partitions,
                                                &data_file.partition,
                                                table_metadata.default_spec(),
                                            )?;
                                            let manifest_entry =
                                                ManifestEntry::V1(ManifestEntryV1 {
                                                    status: Status::Added,
                                                    snapshot_id: metadata
                                                        .current_snapshot_id
                                                        .unwrap_or(1),
                                                    data_file: DataFileV1 {
                                                        file_path: data_file.file_path,
                                                        file_format: data_file.file_format,
                                                        partition: data_file.partition,
                                                        record_count: data_file.record_count,
                                                        file_size_in_bytes: data_file
                                                            .file_size_in_bytes,
                                                        block_size_in_bytes: data_file
                                                            .file_size_in_bytes,
                                                        file_ordinal: None,
                                                        sort_columns: None,
                                                        column_sizes: data_file.column_sizes,
                                                        value_counts: data_file.value_counts,
                                                        null_value_counts: data_file
                                                            .null_value_counts,
                                                        nan_value_counts: data_file
                                                            .nan_value_counts,
                                                        distinct_counts: data_file.distinct_counts,
                                                        lower_bounds: data_file.lower_bounds,
                                                        upper_bounds: data_file.upper_bounds,
                                                        key_metadata: data_file.key_metadata,
                                                        split_offsets: data_file.split_offsets,
                                                        sort_order_id: data_file.sort_order_id,
                                                    },
                                                });

                                            manifest_writer.append_ser(manifest_entry)?;
                                        }
                                    }
                                    TableMetadata::V2(metadata) => {
                                        for file in files_metadata {
                                            let data_file = parquet_to_datafilev2(
                                                file.0,
                                                file.1,
                                                file.2,
                                                partition_value.clone(),
                                                table_metadata.current_schema(),
                                            )?;
                                            added_rows_count += data_file.record_count;
                                            update_partitions(
                                                &mut partitions,
                                                &data_file.partition,
                                                table_metadata.default_spec(),
                                            )?;
                                            let manifest_entry =
                                                ManifestEntry::V2(ManifestEntryV2 {
                                                    status: Status::Added,
                                                    snapshot_id: metadata.current_snapshot_id,
                                                    sequence_number: metadata
                                                        .snapshots
                                                        .as_ref()
                                                        .map(|snapshots| {
                                                            snapshots
                                                                .last()
                                                                .unwrap()
                                                                .sequence_number
                                                        }),
                                                    data_file,
                                                });

                                            manifest_writer.append_ser(manifest_entry)?;
                                        }
                                    }
                                };

                                match &mut manifest {
                                    ManifestFile::V1(manifest) => {
                                        manifest.added_files_count =
                                            match manifest.added_files_count {
                                                Some(count) => Some(count + files_count),
                                                None => Some(files_count),
                                            };
                                        manifest.added_rows_count = match manifest.added_rows_count
                                        {
                                            Some(count) => Some(count + added_rows_count),
                                            None => Some(added_rows_count),
                                        };
                                        manifest.partitions = Some(partitions)
                                    }
                                    ManifestFile::V2(manifest) => {
                                        manifest.added_rows_count += added_rows_count;
                                        manifest.partitions = Some(partitions)
                                    }
                                }
                            }

                            let manifest_bytes = manifest_writer.into_inner()?;

                            let manifest_length: i64 = manifest_bytes.len() as i64;

                            match &mut manifest {
                                ManifestFile::V1(manifest) => {
                                    manifest.manifest_length += manifest_length;
                                }
                                ManifestFile::V2(manifest) => {
                                    manifest.manifest_length += manifest_length;
                                }
                            }

                            object_store
                                .put(&manifest.manifest_path().into(), manifest_bytes.into())
                                .await?;

                            Ok::<_, anyhow::Error>(manifest)
                        }
                    })
                    .for_each_concurrent(None, |manifest| {
                        let manifest_list_writer = manifest_list_writer.clone();
                        async move {
                            manifest_list_writer
                                .lock()
                                .await
                                .append_ser(manifest.unwrap())
                                .unwrap();
                        }
                    })
                    .await;
                Ok(())
            }
            _ => Ok(()),
        }
    }
}

fn update_partitions(
    partitions: &mut Vec<FieldSummary>,
    partition_values: &Struct,
    partition_spec: &[PartitionField],
) -> Result<()> {
    for (field, summary) in partition_spec.iter().zip(partitions.iter_mut()) {
        let value = &partition_values.fields[*partition_values
            .lookup
            .get(&field.name)
            .ok_or_else(|| anyhow!("Couldn't find column {} in partition values", &field.name))?];
        if let Some(value) = value {
            if let Some(lower_bound) = &mut summary.lower_bound {
                match value {
                    Value::Int(val) => {
                        let current =
                            bytes_to_any(lower_bound, &Type::Primitive(PrimitiveType::Int))?
                                .downcast::<i32>()
                                .unwrap();
                        if *current > *val {
                            *lower_bound = <Value as Into<ByteBuf>>::into(value.clone())
                        }
                    }
                    Value::LongInt(val) => {
                        let current =
                            bytes_to_any(lower_bound, &Type::Primitive(PrimitiveType::Long))?
                                .downcast::<i64>()
                                .unwrap();
                        if *current > *val {
                            *lower_bound = <Value as Into<ByteBuf>>::into(value.clone())
                        }
                    }
                    Value::Float(val) => {
                        let current =
                            bytes_to_any(lower_bound, &Type::Primitive(PrimitiveType::Float))?
                                .downcast::<f32>()
                                .unwrap();
                        if *current > *val {
                            *lower_bound = <Value as Into<ByteBuf>>::into(value.clone())
                        }
                    }
                    Value::Double(val) => {
                        let current =
                            bytes_to_any(lower_bound, &Type::Primitive(PrimitiveType::Double))?
                                .downcast::<f64>()
                                .unwrap();
                        if *current > *val {
                            *lower_bound = <Value as Into<ByteBuf>>::into(value.clone())
                        }
                    }
                    Value::Date(val) => {
                        let current =
                            bytes_to_any(lower_bound, &Type::Primitive(PrimitiveType::Date))?
                                .downcast::<i32>()
                                .unwrap();
                        if *current > *val {
                            *lower_bound = <Value as Into<ByteBuf>>::into(value.clone())
                        }
                    }
                    Value::Time(val) => {
                        let current =
                            bytes_to_any(lower_bound, &Type::Primitive(PrimitiveType::Time))?
                                .downcast::<i64>()
                                .unwrap();
                        if *current > *val {
                            *lower_bound = <Value as Into<ByteBuf>>::into(value.clone())
                        }
                    }
                    Value::Timestamp(val) => {
                        let current =
                            bytes_to_any(lower_bound, &Type::Primitive(PrimitiveType::Timestamp))?
                                .downcast::<i64>()
                                .unwrap();
                        if *current > *val {
                            *lower_bound = <Value as Into<ByteBuf>>::into(value.clone())
                        }
                    }
                    Value::TimestampTZ(val) => {
                        let current =
                            bytes_to_any(lower_bound, &Type::Primitive(PrimitiveType::Timestampz))?
                                .downcast::<i64>()
                                .unwrap();
                        if *current > *val {
                            *lower_bound = <Value as Into<ByteBuf>>::into(value.clone())
                        }
                    }
                    _ => {}
                }
            }
            if let Some(upper_bound) = &mut summary.upper_bound {
                match value {
                    Value::Int(val) => {
                        let current =
                            bytes_to_any(upper_bound, &Type::Primitive(PrimitiveType::Int))?
                                .downcast::<i32>()
                                .unwrap();
                        if *current < *val {
                            *upper_bound = <Value as Into<ByteBuf>>::into(value.clone())
                        }
                    }
                    Value::LongInt(val) => {
                        let current =
                            bytes_to_any(upper_bound, &Type::Primitive(PrimitiveType::Long))?
                                .downcast::<i64>()
                                .unwrap();
                        if *current < *val {
                            *upper_bound = <Value as Into<ByteBuf>>::into(value.clone())
                        }
                    }
                    Value::Float(val) => {
                        let current =
                            bytes_to_any(upper_bound, &Type::Primitive(PrimitiveType::Float))?
                                .downcast::<f32>()
                                .unwrap();
                        if *current < *val {
                            *upper_bound = <Value as Into<ByteBuf>>::into(value.clone())
                        }
                    }
                    Value::Double(val) => {
                        let current =
                            bytes_to_any(upper_bound, &Type::Primitive(PrimitiveType::Double))?
                                .downcast::<f64>()
                                .unwrap();
                        if *current < *val {
                            *upper_bound = <Value as Into<ByteBuf>>::into(value.clone())
                        }
                    }
                    Value::Date(val) => {
                        let current =
                            bytes_to_any(upper_bound, &Type::Primitive(PrimitiveType::Date))?
                                .downcast::<i32>()
                                .unwrap();
                        if *current < *val {
                            *upper_bound = <Value as Into<ByteBuf>>::into(value.clone())
                        }
                    }
                    Value::Time(val) => {
                        let current =
                            bytes_to_any(upper_bound, &Type::Primitive(PrimitiveType::Time))?
                                .downcast::<i64>()
                                .unwrap();
                        if *current < *val {
                            *upper_bound = <Value as Into<ByteBuf>>::into(value.clone())
                        }
                    }
                    Value::Timestamp(val) => {
                        let current =
                            bytes_to_any(upper_bound, &Type::Primitive(PrimitiveType::Timestamp))?
                                .downcast::<i64>()
                                .unwrap();
                        if *current < *val {
                            *upper_bound = <Value as Into<ByteBuf>>::into(value.clone())
                        }
                    }
                    Value::TimestampTZ(val) => {
                        let current =
                            bytes_to_any(upper_bound, &Type::Primitive(PrimitiveType::Timestampz))?
                                .downcast::<i64>()
                                .unwrap();
                        if *current < *val {
                            *upper_bound = <Value as Into<ByteBuf>>::into(value.clone())
                        }
                    }
                    _ => {}
                }
            }
        }
    }
    Ok(())
}

/// checks if partition values lie in the bounds of the field summary
fn partition_values_in_bounds<'values>(
    partitions: &Vec<FieldSummary>,
    partition_values: &'values Vec<Vec<Option<Value>>>,
) -> Option<&'values Vec<Option<Value>>> {
    partition_values.iter().find(|values| {
        values
            .iter()
            .zip(partitions.iter())
            .all(|(value, summary)| {
                if let Some(value) = value {
                    if let (Some(lower_bound), Some(upper_bound)) =
                        (&summary.lower_bound, &summary.upper_bound)
                    {
                        match value {
                            Value::Int(val) => {
                                let lower_bound =
                                    bytes_to_any(lower_bound, &Type::Primitive(PrimitiveType::Int))
                                        .and_then(|x| {
                                            x.downcast::<i32>().map_err(|_| {
                                                anyhow!("Failed to downcast bytes to i32.")
                                            })
                                        })
                                        .unwrap();
                                let upper_bound =
                                    bytes_to_any(upper_bound, &Type::Primitive(PrimitiveType::Int))
                                        .and_then(|x| {
                                            x.downcast::<i32>().map_err(|_| {
                                                anyhow!("Failed to downcast bytes to i32.")
                                            })
                                        })
                                        .unwrap();
                                *lower_bound <= *val && *upper_bound >= *val
                            }
                            Value::LongInt(val) => {
                                let lower_bound = bytes_to_any(
                                    lower_bound,
                                    &Type::Primitive(PrimitiveType::Long),
                                )
                                .and_then(|x| {
                                    x.downcast::<i64>()
                                        .map_err(|_| anyhow!("Failed to downcast bytes to i64."))
                                })
                                .unwrap();
                                let upper_bound = bytes_to_any(
                                    upper_bound,
                                    &Type::Primitive(PrimitiveType::Long),
                                )
                                .and_then(|x| {
                                    x.downcast::<i64>()
                                        .map_err(|_| anyhow!("Failed to downcast bytes to i64."))
                                })
                                .unwrap();
                                *lower_bound <= *val && *upper_bound >= *val
                            }
                            Value::Float(val) => {
                                let lower_bound = bytes_to_any(
                                    lower_bound,
                                    &Type::Primitive(PrimitiveType::Float),
                                )
                                .and_then(|x| {
                                    x.downcast::<f32>()
                                        .map_err(|_| anyhow!("Failed to downcast bytes to f32."))
                                })
                                .unwrap();
                                let upper_bound = bytes_to_any(
                                    upper_bound,
                                    &Type::Primitive(PrimitiveType::Float),
                                )
                                .and_then(|x| {
                                    x.downcast::<f32>()
                                        .map_err(|_| anyhow!("Failed to downcast bytes to f32."))
                                })
                                .unwrap();
                                *lower_bound <= *val && *upper_bound >= *val
                            }
                            Value::Double(val) => {
                                let lower_bound = bytes_to_any(
                                    lower_bound,
                                    &Type::Primitive(PrimitiveType::Double),
                                )
                                .and_then(|x| {
                                    x.downcast::<f64>()
                                        .map_err(|_| anyhow!("Failed to downcast bytes to f64."))
                                })
                                .unwrap();
                                let upper_bound = bytes_to_any(
                                    upper_bound,
                                    &Type::Primitive(PrimitiveType::Double),
                                )
                                .and_then(|x| {
                                    x.downcast::<f64>()
                                        .map_err(|_| anyhow!("Failed to downcast bytes to f64."))
                                })
                                .unwrap();
                                *lower_bound <= *val && *upper_bound >= *val
                            }
                            Value::Date(val) => {
                                let lower_bound = bytes_to_any(
                                    lower_bound,
                                    &Type::Primitive(PrimitiveType::Date),
                                )
                                .and_then(|x| {
                                    x.downcast::<i32>()
                                        .map_err(|_| anyhow!("Failed to downcast bytes to i32."))
                                })
                                .unwrap();
                                let upper_bound = bytes_to_any(
                                    upper_bound,
                                    &Type::Primitive(PrimitiveType::Date),
                                )
                                .and_then(|x| {
                                    x.downcast::<i32>()
                                        .map_err(|_| anyhow!("Failed to downcast bytes to i32."))
                                })
                                .unwrap();
                                *lower_bound <= *val && *upper_bound >= *val
                            }
                            Value::Time(val) => {
                                let lower_bound = bytes_to_any(
                                    lower_bound,
                                    &Type::Primitive(PrimitiveType::Time),
                                )
                                .and_then(|x| {
                                    x.downcast::<i64>()
                                        .map_err(|_| anyhow!("Failed to downcast bytes to i64."))
                                })
                                .unwrap();
                                let upper_bound = bytes_to_any(
                                    upper_bound,
                                    &Type::Primitive(PrimitiveType::Time),
                                )
                                .and_then(|x| {
                                    x.downcast::<i64>()
                                        .map_err(|_| anyhow!("Failed to downcast bytes to i64."))
                                })
                                .unwrap();
                                *lower_bound <= *val && *upper_bound >= *val
                            }
                            Value::Timestamp(val) => {
                                let lower_bound = bytes_to_any(
                                    lower_bound,
                                    &Type::Primitive(PrimitiveType::Timestamp),
                                )
                                .and_then(|x| {
                                    x.downcast::<i64>()
                                        .map_err(|_| anyhow!("Failed to downcast bytes to i64."))
                                })
                                .unwrap();
                                let upper_bound = bytes_to_any(
                                    upper_bound,
                                    &Type::Primitive(PrimitiveType::Timestamp),
                                )
                                .and_then(|x| {
                                    x.downcast::<i64>()
                                        .map_err(|_| anyhow!("Failed to downcast bytes to i64."))
                                })
                                .unwrap();
                                *lower_bound <= *val && *upper_bound >= *val
                            }
                            Value::TimestampTZ(val) => {
                                let lower_bound = bytes_to_any(
                                    lower_bound,
                                    &Type::Primitive(PrimitiveType::Timestampz),
                                )
                                .and_then(|x| {
                                    x.downcast::<i64>()
                                        .map_err(|_| anyhow!("Failed to downcast bytes to i64."))
                                })
                                .unwrap();
                                let upper_bound = bytes_to_any(
                                    upper_bound,
                                    &Type::Primitive(PrimitiveType::Timestampz),
                                )
                                .and_then(|x| {
                                    x.downcast::<i64>()
                                        .map_err(|_| anyhow!("Failed to downcast bytes to i64."))
                                })
                                .unwrap();
                                *lower_bound <= *val && *upper_bound >= *val
                            }
                            _ => false,
                        }
                    } else {
                        false
                    }
                } else {
                    summary.contains_null
                }
            })
    })
}

#[cfg(test)]
mod tests {

    use std::sync::Arc;

    use object_store::{memory::InMemory, ObjectStore};

    use crate::{
        model::{
            data_types::{PrimitiveType, StructField, StructType, Type},
            schema::SchemaV2,
            values::Struct,
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
        let mut table = TableBuilder::new_filesystem_table(
            "test/append",
            schema.clone(),
            Arc::clone(&object_store),
        )
        .unwrap()
        .commit()
        .await
        .unwrap();

        let metadata_location = table.metadata_location();
        assert_eq!(metadata_location, "test/append/metadata/v1.metadata.json");

        let transaction = table.new_transaction();
        transaction
            .fast_append(
                vec![
                    "test/append/data/file1.parquet".to_string(),
                    "test/append/data/file2.parquet".to_string(),
                ],
                vec![Struct::from_iter([]), Struct::from_iter([])],
            )
            .commit()
            .await
            .unwrap();
        let metadata_location = table.metadata_location();
        assert_eq!(metadata_location, "test/append/metadata/v2.metadata.json");
    }
}

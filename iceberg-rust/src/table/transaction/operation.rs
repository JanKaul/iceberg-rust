/*!
 * Defines the different [Operation]s on a [Table].
*/

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use apache_avro::from_value;
use futures::{lock::Mutex, stream, StreamExt, TryStreamExt};

use crate::{
    error::Error,
    file_format::{parquet::parquet_to_datafile, DatafileMetadata},
    spec::{
        manifest::{
            partition_value_schema, Content, DataFile, ManifestEntry, ManifestWriter, Status,
        },
        manifest_list::{FieldSummary, ManifestListEntry, ManifestListEntryEnum},
        partition::PartitionField,
        schema::{Schema, SchemaV2},
        types::StructField,
        values::{Struct, Value},
    },
    table::Table,
    util::strip_prefix,
};

///Table operations
pub enum Operation {
    /// Update schema
    UpdateSchema(SchemaV2),
    /// Update spec
    UpdateSpec(i32),
    // /// Update table properties
    UpdateProperties(Vec<(String, String)>),
    /// Replace the sort order
    // ReplaceSortOrder,
    // /// Update the table location
    // UpdateLocation,
    /// Append new files to the table
    NewAppend {
        paths: Vec<(String, DatafileMetadata)>,
    },
    // /// Quickly append new files to the table
    // NewFastAppend {
    //     paths: Vec<String>,
    //     partition_values: Vec<Struct>,
    // },
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
    pub async fn execute(self, table: &mut Table) -> Result<(), Error> {
        match self {
            Operation::NewAppend { paths } => {
                let object_store = table.object_store();
                let table_metadata = table.metadata();
                let partition_spec = table_metadata.default_partition_spec()?;
                let schema = table_metadata.current_schema()?;

                let datafiles = Arc::new(
                    stream::iter(paths.iter())
                        .then(|(path, file_metadata)| {
                            let object_store = object_store.clone();
                            async move {
                                let size = object_store.head(&path.as_str().into()).await?.size;
                                let datafile = match file_metadata {
                                    DatafileMetadata::Parquet(file_metadata) => {
                                        parquet_to_datafile(
                                            path,
                                            size,
                                            file_metadata,
                                            schema,
                                            &partition_spec.fields,
                                        )?
                                    }
                                };

                                Ok::<_, Error>(datafile)
                            }
                        })
                        .try_fold(
                            HashMap::<Struct, Vec<DataFile>>::new(),
                            |mut acc, x| async move {
                                let partition_value = x.partition.clone();
                                acc.entry(partition_value).or_default().push(x);
                                Ok(acc)
                            },
                        )
                        .await?,
                );

                let manifest_list_schema =
                    ManifestListEntry::schema(&table_metadata.format_version)?;

                let manifest_list_writer = Arc::new(Mutex::new(apache_avro::Writer::new(
                    &manifest_list_schema,
                    Vec::new(),
                )));

                let existing_partitions = Arc::new(Mutex::new(HashSet::new()));

                let manifest_list_location = &table_metadata
                    .current_snapshot()?
                    .ok_or_else(|| Error::InvalidFormat("manifest list in metadata".to_string()))?
                    .manifest_list;

                let manifest_list_bytes: Vec<u8> = object_store
                    .get(&strip_prefix(manifest_list_location).as_str().into())
                    .await?
                    .bytes()
                    .await?
                    .into();

                // Check if file has content "null", if so manifest_list file is empty
                let existing_manifest_iter = if manifest_list_bytes == vec![110, 117, 108, 108] {
                    None
                } else {
                    let manifest_list_reader = apache_avro::Reader::new(&*manifest_list_bytes)?;

                    Some(stream::iter(manifest_list_reader).filter_map(|manifest| {
                        let datafiles = datafiles.clone();
                        let existing_partitions = existing_partitions.clone();
                        async move {
                            let manifest = manifest
                                .map_err(Into::into)
                                .and_then(|value| {
                                    ManifestListEntry::try_from_enum(
                                        from_value::<ManifestListEntryEnum>(&value)?,
                                        table_metadata,
                                    )
                                })
                                .unwrap();

                            if let Some(summary) = &manifest.partitions {
                                let partition_values = partition_values_in_bounds(
                                    summary,
                                    datafiles.keys(),
                                    &partition_spec.fields,
                                    schema,
                                );
                                if !partition_values.is_empty() {
                                    for file in &partition_values {
                                        existing_partitions.lock().await.insert(file.clone());
                                    }
                                    Some((
                                        Ok::<ManifestListEntry, ManifestListEntry>(manifest),
                                        partition_values,
                                    ))
                                } else {
                                    None
                                }
                            } else {
                                None
                            }
                        }
                    }))
                };

                let manifest_count = if existing_manifest_iter.is_some() {
                    apache_avro::Reader::new(&*manifest_list_bytes)?.count()
                } else {
                    0
                };

                let new_manifest_iter = stream::iter(datafiles.iter().enumerate()).filter_map(
                    |(i, (partition_value, _))| {
                        let existing_partitions = existing_partitions.clone();
                        async move {
                            if !existing_partitions.lock().await.contains(partition_value) {
                                let manifest_location = manifest_list_location
                                    .to_string()
                                    .trim_end_matches(".avro")
                                    .to_owned()
                                    + "-m"
                                    + &(manifest_count + i).to_string()
                                    + ".avro";
                                let manifest = ManifestListEntry {
                                    format_version: table_metadata.format_version.clone(),
                                    manifest_path: manifest_location,
                                    manifest_length: 0,
                                    partition_spec_id: table_metadata.default_spec_id,
                                    content: Content::Data,
                                    sequence_number: table_metadata.last_sequence_number,
                                    min_sequence_number: 0,
                                    added_snapshot_id: table_metadata.current_snapshot_id.unwrap(),
                                    added_files_count: Some(0),
                                    existing_files_count: Some(0),
                                    deleted_files_count: Some(0),
                                    added_rows_count: Some(0),
                                    existing_rows_count: Some(0),
                                    deleted_rows_count: Some(0),
                                    partitions: None,
                                    key_metadata: None,
                                };
                                Some((
                                    Err::<ManifestListEntry, ManifestListEntry>(manifest),
                                    vec![partition_value.clone()],
                                ))
                            } else {
                                None
                            }
                        }
                    },
                );

                let partition_columns = Arc::new(
                    partition_spec
                        .fields
                        .iter()
                        .map(|x| schema.fields.get(x.source_id as usize))
                        .collect::<Option<Vec<_>>>()
                        .ok_or(Error::InvalidFormat(
                            "Partition column in schema".to_string(),
                        ))?,
                );

                let write_manifest_closure = |(manifest, files): (
                    Result<ManifestListEntry, ManifestListEntry>,
                    Vec<Struct>,
                )| {
                    let object_store = object_store.clone();
                    let datafiles = datafiles.clone();
                    let partition_columns = partition_columns.clone();
                    async move {
                        let manifest_schema = ManifestEntry::schema(
                            &partition_value_schema(
                                &table_metadata.default_partition_spec()?.fields,
                                schema,
                            )?,
                            &table_metadata.format_version,
                        )?;

                        let mut manifest_writer =
                            ManifestWriter::new(&manifest_schema, &table_metadata, Vec::new())?;

                        let mut manifest = match manifest {
                            Ok(manifest) => {
                                let manifest_bytes: Vec<u8> = object_store
                                    .get(&strip_prefix(&manifest.manifest_path).as_str().into())
                                    .await?
                                    .bytes()
                                    .await?
                                    .into();

                                let manifest_reader = apache_avro::Reader::new(&*manifest_bytes)?;
                                manifest_writer.extend(manifest_reader.filter_map(Result::ok))?;
                                manifest
                            }
                            Err(manifest) => manifest,
                        };
                        let files_count =
                            manifest.added_files_count.unwrap_or_default() + files.len() as i32;
                        for path in files {
                            for datafile in datafiles.get(&path).ok_or(Error::InvalidFormat(
                                "Datafiles for partition value".to_string(),
                            ))? {
                                let mut added_rows_count = 0;

                                let mut partitions = match &manifest.partitions {
                                    Some(partitions) => partitions.clone(),
                                    None => table_metadata
                                        .default_partition_spec()?
                                        .fields
                                        .iter()
                                        .map(|_| FieldSummary {
                                            contains_null: false,
                                            contains_nan: None,
                                            lower_bound: None,
                                            upper_bound: None,
                                        })
                                        .collect::<Vec<FieldSummary>>(),
                                };

                                added_rows_count += datafile.record_count;
                                update_partitions(
                                    &mut partitions,
                                    &datafile.partition,
                                    &partition_columns,
                                )?;

                                let manifest_entry = ManifestEntry {
                                    format_version: table_metadata.format_version.clone(),
                                    status: Status::Added,
                                    snapshot_id: table_metadata.current_snapshot_id,
                                    sequence_number: table_metadata
                                        .current_snapshot()?
                                        .map(|x| x.sequence_number),
                                    data_file: datafile.clone(),
                                };

                                manifest_writer.append_ser(manifest_entry)?;

                                manifest.added_files_count = match manifest.added_files_count {
                                    Some(count) => Some(count + files_count),
                                    None => Some(files_count),
                                };
                                manifest.added_rows_count = match manifest.added_rows_count {
                                    Some(count) => Some(count + added_rows_count),
                                    None => Some(added_rows_count),
                                };
                                manifest.partitions = Some(partitions);
                            }
                        }

                        let manifest_bytes = manifest_writer.into_inner()?;

                        let manifest_length: i64 = manifest_bytes.len() as i64;

                        manifest.manifest_length += manifest_length;

                        object_store
                            .put(
                                &strip_prefix(&manifest.manifest_path).as_str().into(),
                                manifest_bytes.into(),
                            )
                            .await?;

                        Ok::<_, Error>(manifest)
                    }
                };

                match existing_manifest_iter {
                    Some(existing_manifest_iter) => {
                        let manifest_iter =
                            Box::new(existing_manifest_iter.chain(new_manifest_iter));

                        manifest_iter
                            .then(write_manifest_closure)
                            .try_for_each_concurrent(None, |manifest| {
                                let manifest_list_writer = manifest_list_writer.clone();
                                async move {
                                    manifest_list_writer.lock().await.append_ser(manifest)?;
                                    Ok(())
                                }
                            })
                            .await?;
                    }
                    None => {
                        new_manifest_iter
                            .then(write_manifest_closure)
                            .try_for_each_concurrent(None, |manifest| {
                                let manifest_list_writer = manifest_list_writer.clone();
                                async move {
                                    manifest_list_writer.lock().await.append_ser(manifest)?;
                                    Ok(())
                                }
                            })
                            .await?;
                    }
                }

                let manifest_list_bytes = Arc::into_inner(manifest_list_writer)
                    .unwrap()
                    .into_inner()
                    .into_inner()?;

                object_store
                    .put(
                        &strip_prefix(manifest_list_location).as_str().into(),
                        manifest_list_bytes.into(),
                    )
                    .await?;

                Ok(())
            }
            Operation::UpdateProperties(entries) => {
                let properties = &mut table.metadata.properties;
                if properties.is_none() {
                    *properties = Some(HashMap::new());
                }
                match properties {
                    Some(properties) => {
                        entries.into_iter().for_each(|(key, value)| {
                            properties.insert(key, value);
                        });
                    }
                    None => (),
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }
}

fn update_partitions(
    partitions: &mut [FieldSummary],
    partition_values: &Struct,
    partition_columns: &[&StructField],
) -> Result<(), Error> {
    for (field, summary) in partition_columns.iter().zip(partitions.iter_mut()) {
        let value = &partition_values.fields[*partition_values
            .lookup
            .get(&field.name)
            .ok_or_else(|| Error::InvalidFormat("partition value in schema".to_string()))?];
        if let Some(value) = value {
            if let Some(lower_bound) = &mut summary.lower_bound {
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
            if let Some(upper_bound) = &mut summary.upper_bound {
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

/// checks if partition values lie in the bounds of the field summary
fn partition_values_in_bounds<'a>(
    partitions: &[FieldSummary],
    partition_values: impl Iterator<Item = &'a Struct>,
    partition_spec: &[PartitionField],
    schema: &Schema,
) -> Vec<Struct> {
    partition_values
        .filter(|value| {
            partition_spec
                .iter()
                .map(|field| {
                    let name = &schema
                        .fields
                        .get(field.source_id.try_into().unwrap())
                        .ok_or_else(|| {
                            Error::InvalidFormat("partition values in schema".to_string())
                        })
                        .unwrap()
                        .name;
                    value
                        .get(name)
                        .ok_or_else(|| {
                            Error::InvalidFormat("partition values in schema".to_string())
                        })
                        .unwrap()
                })
                .zip(partitions.iter())
                .all(|(value, summary)| {
                    if let Some(value) = value {
                        if let (Some(lower_bound), Some(upper_bound)) =
                            (&summary.lower_bound, &summary.upper_bound)
                        {
                            match (value, lower_bound, upper_bound) {
                                (
                                    Value::Int(val),
                                    Value::Int(lower_bound),
                                    Value::Int(upper_bound),
                                ) => *lower_bound <= *val && *upper_bound >= *val,
                                (
                                    Value::LongInt(val),
                                    Value::LongInt(lower_bound),
                                    Value::LongInt(upper_bound),
                                ) => *lower_bound <= *val && *upper_bound >= *val,
                                (
                                    Value::Float(val),
                                    Value::Float(lower_bound),
                                    Value::Float(upper_bound),
                                ) => *lower_bound <= *val && *upper_bound >= *val,
                                (
                                    Value::Double(val),
                                    Value::Double(lower_bound),
                                    Value::Double(upper_bound),
                                ) => *lower_bound <= *val && *upper_bound >= *val,
                                (
                                    Value::Date(val),
                                    Value::Date(lower_bound),
                                    Value::Date(upper_bound),
                                ) => *lower_bound <= *val && *upper_bound >= *val,
                                (
                                    Value::Time(val),
                                    Value::Time(lower_bound),
                                    Value::Time(upper_bound),
                                ) => *lower_bound <= *val && *upper_bound >= *val,
                                (
                                    Value::Timestamp(val),
                                    Value::Timestamp(lower_bound),
                                    Value::Timestamp(upper_bound),
                                ) => *lower_bound <= *val && *upper_bound >= *val,
                                (
                                    Value::TimestampTZ(val),
                                    Value::TimestampTZ(lower_bound),
                                    Value::TimestampTZ(upper_bound),
                                ) => *lower_bound <= *val && *upper_bound >= *val,
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
        .map(Clone::clone)
        .collect()
}

/*!
 * Defines the different [Operation]s on a [Table].
*/

use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::{collections::HashMap, sync::Arc};

use bytes::Bytes;
use futures::future;
use iceberg_rust_spec::manifest_list::{
    manifest_list_schema_v1, manifest_list_schema_v2, Content as ManifestListContent,
    ManifestListEntry,
};
use iceberg_rust_spec::snapshot::{Operation as SnapshotOperation, Snapshot};
use iceberg_rust_spec::spec::manifest::Content;
use iceberg_rust_spec::spec::table_metadata::TableMetadata;
use iceberg_rust_spec::spec::{
    manifest::{partition_value_schema, DataFile, ManifestEntry, Status},
    schema::Schema,
    snapshot::{
        generate_snapshot_id, SnapshotBuilder, SnapshotReference, SnapshotRetention, Summary,
    },
};
use iceberg_rust_spec::table_metadata::FormatVersion;
use iceberg_rust_spec::util::strip_prefix;
use itertools::Either;
use object_store::ObjectStore;
use smallvec::SmallVec;
use tokio::task::JoinHandle;
use tracing::{debug, instrument};

use crate::table::manifest::ManifestWriter;
use crate::table::manifest_list::ManifestListWriter;
use crate::table::transaction::append::append_summary;
use crate::{
    catalog::commit::{TableRequirement, TableUpdate},
    error::Error,
    util::{partition_struct_to_vec, Rectangle},
};

use super::append::split_datafiles;

/// The target number of datafiles per manifest is dynamic, but we don't want to go below this number.
static MIN_DATAFILES_PER_MANIFEST: usize = 4;

#[derive(Debug, Clone)]
/// Group of writes sharing a Data Sequence Number
pub struct SequenceGroup {
    /// Delete files. These apply to insert files from previous Sequence Groups
    pub delete_files: Vec<DataFile>,
    /// Insert files
    pub data_files: Vec<DataFile>,
}

#[derive(Debug)]
///Table operations
pub enum Operation {
    /// Update schema
    AddSchema(Schema),
    /// Update spec
    SetDefaultSpec(i32),
    /// Update table properties
    UpdateProperties(Vec<(String, String)>),
    /// Set Ref
    SetSnapshotRef((String, SnapshotReference)),
    /// Replace the sort order
    // ReplaceSortOrder,
    // /// Update the table location
    // UpdateLocation,
    /// Append new files to the table
    Append {
        branch: Option<String>,
        data_files: Vec<DataFile>,
        delete_files: Vec<DataFile>,
        additional_summary: Option<HashMap<String, String>>,
    },
    /// Append new change groups to the table
    AppendSequenceGroups {
        branch: Option<String>,
        sequence_groups: Vec<SequenceGroup>,
    },
    // /// Quickly append new files to the table
    // NewFastAppend {
    //     paths: Vec<String>,
    //     partition_values: Vec<Struct>,
    // },
    // /// Replace files in the table and commit
    Replace {
        branch: Option<String>,
        files: Vec<DataFile>,
        additional_summary: Option<HashMap<String, String>>,
    },
    // /// Replace manifests files and commit
    // RewriteManifests,
    Overwrite {
        branch: Option<String>,
        data_files: Vec<DataFile>,
        files_to_overwrite: HashMap<String, Vec<String>>,
        additional_summary: Option<HashMap<String, String>>,
    }, // /// Remove or replace rows in existing data files
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
    #[instrument(
        name = "iceberg_rust::table::transaction::operation::execute",
        level = "debug",
        skip(object_store)
    )]
    pub async fn execute(
        self,
        table_metadata: &TableMetadata,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<(Option<TableRequirement>, Vec<TableUpdate>), Error> {
        match self {
            Operation::AppendSequenceGroups {
                branch,
                sequence_groups,
            } => {
                let old_snapshot = table_metadata.current_snapshot(branch.as_deref())?;

                let manifest_list_schema = match table_metadata.format_version {
                    FormatVersion::V1 => manifest_list_schema_v1(),
                    FormatVersion::V2 => manifest_list_schema_v2(),
                };

                let n_data_files = sequence_groups.iter().map(|d| d.data_files.len()).sum();
                let n_delete_files = sequence_groups.iter().map(|d| d.delete_files.len()).sum();
                if n_data_files + n_delete_files == 0 {
                    return Ok((None, vec![]));
                };

                let all_files: Vec<DataFile> = sequence_groups
                    .iter()
                    .flat_map(|d| d.delete_files.iter().chain(d.data_files.iter()))
                    .cloned()
                    .collect();
                let additional_summary = append_summary(&all_files);

                let mut manifest_list_writer = if let Some(manifest_list_bytes) =
                    prefetch_manifest_list(old_snapshot, &object_store)
                {
                    let bytes = manifest_list_bytes.await??;
                    ManifestListWriter::from_existing(
                        &bytes,
                        all_files.iter(),
                        manifest_list_schema,
                        table_metadata,
                        branch.as_deref(),
                    )?
                } else {
                    ManifestListWriter::new(
                        all_files.iter(),
                        manifest_list_schema,
                        table_metadata,
                        branch.as_deref(),
                    )?
                };

                let snapshot_id = generate_snapshot_id();

                let mut dsn_offset: i64 = 0;
                for SequenceGroup {
                    data_files,
                    delete_files,
                } in sequence_groups.into_iter()
                {
                    dsn_offset += 1;
                    let n_data_files_in_group = data_files.len();
                    let n_delete_files_in_group = delete_files.len();

                    let new_datafile_iter = data_files.into_iter().map(|data_file| {
                        ManifestEntry::builder()
                            .with_format_version(table_metadata.format_version)
                            .with_status(Status::Added)
                            .with_data_file(data_file)
                            .with_sequence_number(table_metadata.last_sequence_number + dsn_offset)
                            .build()
                            .map_err(Error::from)
                    });

                    let new_deletefile_iter = delete_files.into_iter().map(|data_file| {
                        ManifestEntry::builder()
                            .with_format_version(table_metadata.format_version)
                            .with_status(Status::Added)
                            .with_data_file(data_file)
                            .with_sequence_number(table_metadata.last_sequence_number + dsn_offset)
                            .build()
                            .map_err(Error::from)
                    });

                    // Write manifest files
                    // Split manifest file if limit is exceeded
                    for (content, files, n_files) in [
                        (
                            ManifestListContent::Data,
                            Either::Left(new_datafile_iter),
                            n_data_files_in_group,
                        ),
                        (
                            ManifestListContent::Deletes,
                            Either::Right(new_deletefile_iter),
                            n_delete_files_in_group,
                        ),
                    ] {
                        if n_files != 0 {
                            manifest_list_writer
                                .append(files, snapshot_id, object_store.clone(), content)
                                .await?;
                        }
                    }
                }

                let new_manifest_list_location = manifest_list_writer
                    .finish(snapshot_id, object_store)
                    .await?;

                let snapshot_operation = match (n_data_files, n_delete_files) {
                    (0, 0) => return Ok((None, Vec::new())),
                    (_, 0) => Ok::<_, Error>(SnapshotOperation::Append),
                    (0, _) => Ok(SnapshotOperation::Delete),
                    (_, _) => Ok(SnapshotOperation::Overwrite),
                }?;

                // Compute updated snapshot summary
                // Separate data files from delete files using iterators
                let data_files_iter = all_files.iter().filter(|f| *f.content() == Content::Data);
                let delete_files_iter = all_files.iter().filter(|f| *f.content() != Content::Data);

                let mut summary_fields = update_snapshot_summary(
                    old_snapshot.map(|s| s.summary()),
                    data_files_iter,
                    delete_files_iter,
                );

                // Merge with any additional summary fields
                summary_fields.extend(additional_summary.unwrap_or_default());

                let mut snapshot_builder = SnapshotBuilder::default();
                snapshot_builder
                    .with_snapshot_id(snapshot_id)
                    .with_manifest_list(new_manifest_list_location)
                    .with_sequence_number(table_metadata.last_sequence_number + dsn_offset)
                    .with_summary(Summary {
                        operation: snapshot_operation,
                        other: summary_fields,
                    })
                    .with_schema_id(
                        *table_metadata
                            .current_schema(branch.as_deref())?
                            .schema_id(),
                    );
                if let Some(snapshot) = old_snapshot {
                    snapshot_builder.with_parent_snapshot_id(*snapshot.snapshot_id());
                }
                let snapshot = snapshot_builder.build()?;

                Ok((
                    old_snapshot.map(|x| TableRequirement::AssertRefSnapshotId {
                        r#ref: branch.clone().unwrap_or("main".to_owned()),
                        snapshot_id: *x.snapshot_id(),
                    }),
                    vec![
                        TableUpdate::AddSnapshot { snapshot },
                        TableUpdate::SetSnapshotRef {
                            ref_name: branch.unwrap_or("main".to_owned()),
                            snapshot_reference: SnapshotReference {
                                snapshot_id,
                                retention: SnapshotRetention::default(),
                            },
                        },
                    ],
                ))
            }
            Operation::Append {
                branch,
                data_files,
                delete_files,
                additional_summary,
            } => {
                debug!(
                    "Executing Append operation: branch={:?}, data_files={}, delete_files={}, additional_summary={:?}",
                    branch, data_files.len(), delete_files.len(), additional_summary
                );
                let old_snapshot = table_metadata.current_snapshot(branch.as_deref())?;

                let manifest_list_schema = match table_metadata.format_version {
                    FormatVersion::V1 => manifest_list_schema_v1(),
                    FormatVersion::V2 => manifest_list_schema_v2(),
                };

                let n_data_files = data_files.len();
                let n_delete_files = delete_files.len();

                if n_data_files + n_delete_files == 0 {
                    return Ok((None, Vec::new()));
                }

                // Compute summary before moving data_files and delete_files
                let summary_fields = update_snapshot_summary(
                    old_snapshot.map(|s| s.summary()),
                    data_files.iter(),
                    delete_files.iter(),
                );

                let data_files_iter = delete_files.iter().chain(data_files.iter());

                let mut manifest_list_writer = if let Some(manifest_list_bytes) =
                    prefetch_manifest_list(old_snapshot, &object_store)
                {
                    let bytes = manifest_list_bytes.await??;
                    ManifestListWriter::from_existing(
                        &bytes,
                        data_files_iter,
                        manifest_list_schema,
                        table_metadata,
                        branch.as_deref(),
                    )?
                } else {
                    ManifestListWriter::new(
                        data_files_iter,
                        manifest_list_schema,
                        table_metadata,
                        branch.as_deref(),
                    )?
                };

                let new_datafile_iter = data_files.into_iter().map(|data_file| {
                    ManifestEntry::builder()
                        .with_format_version(table_metadata.format_version)
                        .with_status(Status::Added)
                        .with_data_file(data_file)
                        .build()
                        .map_err(Error::from)
                });

                let new_deletefile_iter = delete_files.into_iter().map(|data_file| {
                    ManifestEntry::builder()
                        .with_format_version(table_metadata.format_version)
                        .with_status(Status::Added)
                        .with_data_file(data_file)
                        .build()
                        .map_err(Error::from)
                });

                let snapshot_id = generate_snapshot_id();

                // Write manifest files
                // Split manifest file if limit is exceeded
                #[allow(clippy::type_complexity)]
                let mut futures: Vec<
                    Pin<Box<dyn Future<Output = Result<(), Error>> + Send>>,
                > = Vec::new();
                for (content, files, n_files) in [
                    (
                        ManifestListContent::Data,
                        Either::Left(new_datafile_iter),
                        n_data_files,
                    ),
                    (
                        ManifestListContent::Deletes,
                        Either::Right(new_deletefile_iter),
                        n_delete_files,
                    ),
                ] {
                    if n_files != 0 {
                        let n_splits = manifest_list_writer.n_splits(n_files, content);

                        if n_splits == 0 {
                            let future = manifest_list_writer
                                .append_concurrently(
                                    files,
                                    snapshot_id,
                                    object_store.clone(),
                                    content,
                                )
                                .await?;
                            futures.push(Box::pin(future));
                        } else {
                            let future = manifest_list_writer
                                .append_multiple_concurrently(
                                    files,
                                    snapshot_id,
                                    n_splits,
                                    object_store.clone(),
                                    content,
                                )
                                .await?;
                            futures.push(Box::pin(future));
                        }
                    }
                }

                let manifest_future = future::try_join_all(futures);

                let (_, new_manifest_list_location) = future::try_join(
                    manifest_future,
                    manifest_list_writer.finish(snapshot_id, object_store),
                )
                .await?;

                let snapshot_operation = match (n_data_files, n_delete_files) {
                    (0, 0) => return Ok((None, Vec::new())),
                    (_, 0) => Ok::<_, Error>(SnapshotOperation::Append),
                    (0, _) => Ok(SnapshotOperation::Delete),
                    (_, _) => Ok(SnapshotOperation::Overwrite),
                }?;

                // Merge with any additional summary fields provided by the caller
                let mut summary_fields = summary_fields;
                if let Some(additional) = additional_summary {
                    summary_fields.extend(additional);
                }

                let mut snapshot_builder = SnapshotBuilder::default();
                snapshot_builder
                    .with_snapshot_id(snapshot_id)
                    .with_manifest_list(new_manifest_list_location)
                    .with_sequence_number(table_metadata.last_sequence_number + 1)
                    .with_summary(Summary {
                        operation: snapshot_operation,
                        other: summary_fields,
                    })
                    .with_schema_id(
                        *table_metadata
                            .current_schema(branch.as_deref())?
                            .schema_id(),
                    );
                if let Some(snapshot) = old_snapshot {
                    snapshot_builder.with_parent_snapshot_id(*snapshot.snapshot_id());
                }
                let snapshot = snapshot_builder.build()?;

                Ok((
                    old_snapshot.map(|x| TableRequirement::AssertRefSnapshotId {
                        r#ref: branch.clone().unwrap_or("main".to_owned()),
                        snapshot_id: *x.snapshot_id(),
                    }),
                    vec![
                        TableUpdate::AddSnapshot { snapshot },
                        TableUpdate::SetSnapshotRef {
                            ref_name: branch.unwrap_or("main".to_owned()),
                            snapshot_reference: SnapshotReference {
                                snapshot_id,
                                retention: SnapshotRetention::default(),
                            },
                        },
                    ],
                ))
            }
            Operation::Replace {
                branch,
                files,
                additional_summary,
            } => {
                debug!(
                    "Executing Replace operation: branch={:?}, files={}, additional_summary={:?}",
                    branch,
                    files.len(),
                    additional_summary
                );
                let partition_fields =
                    table_metadata.current_partition_fields(branch.as_deref())?;
                let old_snapshot = table_metadata.current_snapshot(branch.as_deref())?;
                let schema = table_metadata.current_schema(branch.as_deref())?.clone();

                let partition_column_names = partition_fields
                    .iter()
                    .map(|x| x.name())
                    .collect::<SmallVec<[_; 4]>>();

                let manifest_list_schema = match table_metadata.format_version {
                    FormatVersion::V1 => manifest_list_schema_v1(),
                    FormatVersion::V2 => manifest_list_schema_v2(),
                };

                let mut manifest_list_writer =
                    apache_avro::Writer::new(manifest_list_schema, Vec::new());

                let n_splits = compute_n_splits(0, files.len(), 0);

                let snapshot_id = generate_snapshot_id();
                let sequence_number = table_metadata.last_sequence_number + 1;

                let new_datafile_iter = files.iter().map(|data_file| {
                    ManifestEntry::builder()
                        .with_format_version(table_metadata.format_version)
                        .with_status(Status::Added)
                        .with_snapshot_id(snapshot_id)
                        .with_sequence_number(sequence_number)
                        .with_data_file(data_file.clone())
                        .build()
                        .map_err(Error::from)
                });

                let manifest_schema = ManifestEntry::schema(
                    &partition_value_schema(&partition_fields)?,
                    &table_metadata.format_version,
                )?;

                let snapshot_uuid = &uuid::Uuid::new_v4().to_string();
                let new_manifest_list_location = new_manifest_list_location(
                    &table_metadata.location,
                    snapshot_id,
                    0,
                    snapshot_uuid,
                );

                // Write manifest files
                // Split manifest file if limit is exceeded
                if n_splits == 0 {
                    // If manifest doesn't need to be split

                    let manifest_location =
                        new_manifest_location(&table_metadata.location, snapshot_uuid, 0);
                    let mut manifest_writer = ManifestWriter::new(
                        &manifest_location,
                        snapshot_id,
                        &manifest_schema,
                        table_metadata,
                        ManifestListContent::Data,
                        branch.as_deref(),
                    )?;

                    for manifest_entry in new_datafile_iter {
                        manifest_writer.append(manifest_entry?)?;
                    }

                    let manifest = manifest_writer.finish(object_store.clone()).await?;

                    manifest_list_writer.append_ser(manifest)?;
                } else {
                    let bounding_partition_values =
                        bounding_partition_values(files.iter(), &partition_column_names)?;

                    // Split datafiles
                    let splits = split_datafiles(
                        new_datafile_iter,
                        bounding_partition_values,
                        &partition_column_names,
                        n_splits,
                    )?;

                    for (i, entries) in splits.into_iter().enumerate() {
                        let manifest_location =
                            new_manifest_location(&table_metadata.location, snapshot_uuid, i);

                        let mut manifest_writer = ManifestWriter::new(
                            &manifest_location,
                            snapshot_id,
                            &manifest_schema,
                            table_metadata,
                            ManifestListContent::Data,
                            branch.as_deref(),
                        )?;

                        for manifest_entry in entries {
                            manifest_writer.append(manifest_entry)?;
                        }

                        let manifest = manifest_writer.finish(object_store.clone()).await?;

                        manifest_list_writer.append_ser(manifest)?;
                    }
                };

                let manifest_list_bytes = manifest_list_writer.into_inner()?;

                object_store
                    .put(
                        &strip_prefix(&new_manifest_list_location).into(),
                        manifest_list_bytes.into(),
                    )
                    .await?;

                // For Replace operation, we're replacing all data with new files
                // Compute summary for the new state (not incremental)
                // We set old_summary to None so totals equal the new files only
                let data_files_iter = files.iter().filter(|f| *f.content() == Content::Data);
                let delete_files_iter = files.iter().filter(|f| *f.content() != Content::Data);

                let mut summary_fields = update_snapshot_summary(
                    None, // No old summary - this is a full replacement
                    data_files_iter,
                    delete_files_iter,
                );

                // Merge with any additional summary fields
                if let Some(additional) = additional_summary {
                    summary_fields.extend(additional);
                }

                let mut snapshot_builder = SnapshotBuilder::default();
                snapshot_builder
                    .with_snapshot_id(snapshot_id)
                    .with_sequence_number(sequence_number)
                    .with_schema_id(*schema.schema_id())
                    .with_manifest_list(new_manifest_list_location)
                    .with_summary(Summary {
                        operation: iceberg_rust_spec::spec::snapshot::Operation::Overwrite,
                        other: summary_fields,
                    });
                let snapshot = snapshot_builder.build()?;

                Ok((
                    old_snapshot.map(|x| TableRequirement::AssertRefSnapshotId {
                        r#ref: branch.clone().unwrap_or("main".to_owned()),
                        snapshot_id: *x.snapshot_id(),
                    }),
                    vec![
                        TableUpdate::AddSnapshot { snapshot },
                        TableUpdate::SetSnapshotRef {
                            ref_name: branch.unwrap_or("main".to_owned()),
                            snapshot_reference: SnapshotReference {
                                snapshot_id,
                                retention: SnapshotRetention::default(),
                            },
                        },
                    ],
                ))
            }
            Operation::Overwrite {
                branch,
                data_files,
                files_to_overwrite,
                additional_summary,
            } => {
                debug!(
                    "Executing Overwrite operation: branch={:?}, data_files={}, files_to_overwrite={:?}, additional_summary={:?}",
                    branch, data_files.len(), files_to_overwrite, additional_summary
                );
                let old_snapshot = table_metadata
                    .current_snapshot(branch.as_deref())?
                    .ok_or(Error::InvalidFormat("Snapshot to overwrite".to_owned()))?;

                let manifest_list_schema = match table_metadata.format_version {
                    FormatVersion::V1 => manifest_list_schema_v1(),
                    FormatVersion::V2 => manifest_list_schema_v2(),
                };

                let n_data_files = data_files.len();

                if n_data_files == 0 {
                    return Ok((None, Vec::new()));
                }

                // Compute summary before moving data_files
                let summary_fields = update_snapshot_summary(
                    Some(old_snapshot.summary()),
                    data_files.iter(),
                    std::iter::empty::<&DataFile>(), // No separate delete files in this operation
                );

                let data_files_iter = data_files.iter();

                let manifests_to_overwrite: HashSet<String> =
                    files_to_overwrite.keys().map(ToOwned::to_owned).collect();

                let bytes = prefetch_manifest_list(Some(old_snapshot), &object_store)
                    .unwrap()
                    .await??;

                let (mut manifest_list_writer, manifests_to_overwrite) =
                    ManifestListWriter::from_existing_without_overwrites(
                        &bytes,
                        data_files_iter,
                        &manifests_to_overwrite,
                        manifest_list_schema,
                        table_metadata,
                        branch.as_deref(),
                    )?;

                manifest_list_writer
                    .append_and_filter(
                        manifests_to_overwrite,
                        &files_to_overwrite,
                        object_store.clone(),
                    )
                    .await?;

                let n_splits =
                    manifest_list_writer.n_splits(n_data_files, ManifestListContent::Data);

                let new_datafile_iter = data_files.into_iter().map(|data_file| {
                    ManifestEntry::builder()
                        .with_format_version(table_metadata.format_version)
                        .with_status(Status::Added)
                        .with_data_file(data_file)
                        .build()
                        .map_err(Error::from)
                });

                let snapshot_id = generate_snapshot_id();

                let selected_manifest_location = manifest_list_writer
                    .selected_data_manifest()
                    .map(|x| x.manifest_path.clone())
                    .ok_or(Error::NotFound("Selected manifest".to_owned()))?;

                let files_to_filter = files_to_overwrite.get(&selected_manifest_location);

                let filter = if let Some(filter_files) = files_to_filter {
                    let filter_files: HashSet<String> =
                        filter_files.iter().map(ToOwned::to_owned).collect();
                    Some(move |file: &Result<ManifestEntry, Error>| {
                        let Ok(file) = file else { return true };

                        !filter_files.contains(file.data_file().file_path())
                    })
                } else {
                    None
                };

                // Write manifest files
                // Split manifest file if limit is exceeded
                let new_manifest_list_location = if n_splits == 0 {
                    manifest_list_writer
                        .append_filtered(
                            new_datafile_iter,
                            snapshot_id,
                            filter,
                            object_store.clone(),
                            ManifestListContent::Data,
                        )
                        .await?;
                    manifest_list_writer
                        .finish(snapshot_id, object_store)
                        .await?
                } else {
                    manifest_list_writer
                        .append_multiple_filtered(
                            new_datafile_iter,
                            snapshot_id,
                            n_splits,
                            filter,
                            object_store.clone(),
                            ManifestListContent::Data,
                        )
                        .await?;
                    manifest_list_writer
                        .finish(snapshot_id, object_store)
                        .await?
                };

                let snapshot_operation = SnapshotOperation::Overwrite;

                // Merge with any additional summary fields
                let mut summary_fields = summary_fields;
                if let Some(additional) = additional_summary {
                    summary_fields.extend(additional);
                }

                let mut snapshot_builder = SnapshotBuilder::default();
                snapshot_builder
                    .with_snapshot_id(snapshot_id)
                    .with_manifest_list(new_manifest_list_location)
                    .with_sequence_number(table_metadata.last_sequence_number + 1)
                    .with_summary(Summary {
                        operation: snapshot_operation,
                        other: summary_fields,
                    })
                    .with_schema_id(
                        *table_metadata
                            .current_schema(branch.as_deref())?
                            .schema_id(),
                    );
                snapshot_builder.with_parent_snapshot_id(*old_snapshot.snapshot_id());
                let snapshot = snapshot_builder.build()?;

                Ok((
                    Some(TableRequirement::AssertRefSnapshotId {
                        r#ref: branch.clone().unwrap_or("main".to_owned()),
                        snapshot_id: *old_snapshot.snapshot_id(),
                    }),
                    vec![
                        TableUpdate::AddSnapshot { snapshot },
                        TableUpdate::SetSnapshotRef {
                            ref_name: branch.unwrap_or("main".to_owned()),
                            snapshot_reference: SnapshotReference {
                                snapshot_id,
                                retention: SnapshotRetention::default(),
                            },
                        },
                    ],
                ))
            }
            Operation::UpdateProperties(entries) => {
                debug!(
                    "Executing UpdateProperties operation: entries={:?}",
                    entries
                );
                Ok((
                    None,
                    vec![TableUpdate::SetProperties {
                        updates: HashMap::from_iter(entries),
                    }],
                ))
            }
            Operation::SetSnapshotRef((key, value)) => {
                debug!(
                    "Executing SetSnapshotRef operation: key={}, value={:?}",
                    key, value
                );
                Ok((
                    table_metadata
                        .refs
                        .get(&key)
                        .map(|x| TableRequirement::AssertRefSnapshotId {
                            r#ref: key.clone(),
                            snapshot_id: x.snapshot_id,
                        }),
                    vec![TableUpdate::SetSnapshotRef {
                        ref_name: key,
                        snapshot_reference: value,
                    }],
                ))
            }
            Operation::AddSchema(schema) => {
                debug!(
                    "Executing AddSchema operation: schema_id={:?}",
                    schema.schema_id()
                );
                let last_column_id = schema.fields().iter().map(|x| x.id).max();
                Ok((
                    None,
                    vec![TableUpdate::AddSchema {
                        schema,
                        last_column_id,
                    }],
                ))
            }
            Operation::SetDefaultSpec(spec_id) => {
                debug!("Executing SetDefaultSpec operation: spec_id={}", spec_id);
                Ok((None, vec![TableUpdate::SetDefaultSpec { spec_id }]))
            }
        }
    }
}

pub fn bounding_partition_values<'a>(
    mut iter: impl Iterator<Item = &'a DataFile>,
    partition_column_names: &SmallVec<[&str; 4]>,
) -> Result<Rectangle, Error> {
    iter.try_fold(None, |acc, x| {
        let node = partition_struct_to_vec(x.partition(), partition_column_names)?;
        let Some(mut acc) = acc else {
            return Ok::<_, Error>(Some(Rectangle::new(node.clone(), node)));
        };
        acc.expand_with_node(node);
        Ok(Some(acc))
    })?
    .ok_or(Error::NotFound("Bounding partition values".to_owned()))
}

pub(crate) fn prefetch_manifest(
    selected_manifest_opt: &Option<ManifestListEntry>,
    object_store: &Arc<dyn ObjectStore>,
) -> Option<JoinHandle<Result<Bytes, object_store::Error>>> {
    selected_manifest_opt.as_ref().map(|selected_manifest| {
        tokio::task::spawn({
            let object_store = object_store.clone();
            let path = selected_manifest.manifest_path.clone();
            async move {
                object_store
                    .get(&strip_prefix(&path).as_str().into())
                    .await?
                    .bytes()
                    .await
            }
        })
    })
}

fn prefetch_manifest_list(
    old_snapshot: Option<&Snapshot>,
    object_store: &Arc<dyn ObjectStore>,
) -> Option<JoinHandle<Result<Bytes, object_store::Error>>> {
    old_snapshot
        .map(|x| x.manifest_list())
        .cloned()
        .as_ref()
        .map(|old_manifest_list_location| {
            tokio::task::spawn({
                let object_store = object_store.clone();
                let old_manifest_list_location = old_manifest_list_location.clone();
                async move {
                    object_store
                        .get(&strip_prefix(&old_manifest_list_location).as_str().into())
                        .await?
                        .bytes()
                        .await
                }
            })
        })
}

/// Updates snapshot summary fields incrementally based on operation changes.
///
/// This function computes snapshot summary metrics by:
/// 1. Parsing existing totals from the old snapshot summary
/// 2. Computing deltas from the new data/delete files being added
/// 3. Adding deltas to existing totals
///
/// # Arguments
/// * `old_summary` - The summary from the parent snapshot (if it exists)
/// * `data_files` - Iterator over new data files being added in this operation
/// * `delete_files` - Iterator over new delete files being added in this operation
///
/// # Returns
/// A HashMap with updated summary fields including:
/// - `total-records`: Total record count across all data files
/// - `total-data-files`: Total number of data files
/// - `total-delete-files`: Total number of delete files
/// - `total-file-size-bytes`: Total size of all files in bytes
/// - `added-records`: Records added in this operation
/// - `added-data-files`: Data files added in this operation
/// - `added-files-size-bytes`: Size of files added in this operation
pub fn update_snapshot_summary<'files>(
    old_summary: Option<&Summary>,
    data_files: impl Iterator<Item = &'files DataFile>,
    delete_files: impl Iterator<Item = &'files DataFile>,
) -> HashMap<String, String> {
    // Parse existing values from old summary
    let old_other = old_summary.map(|s| &s.other);

    let parse_i64 = |key: &str| -> i64 {
        old_other
            .and_then(|m| m.get(key))
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(0)
    };

    let old_total_records = parse_i64("total-records");
    let old_total_data_files = parse_i64("total-data-files");
    let old_total_delete_files = parse_i64("total-delete-files");
    let old_total_file_size = parse_i64("total-file-size-bytes");

    // Compute deltas from new files - we need to iterate to count and sum
    let mut added_data_files = 0i64;
    let mut added_records = 0i64;
    let mut added_data_files_size = 0i64;

    for file in data_files {
        added_data_files += 1;
        if *file.content() == Content::Data {
            added_records += file.record_count();
        }
        added_data_files_size += file.file_size_in_bytes();
    }

    let mut added_delete_files = 0i64;
    let mut added_delete_files_size = 0i64;

    for file in delete_files {
        added_delete_files += 1;
        added_delete_files_size += file.file_size_in_bytes();
    }

    let added_files_size = added_data_files_size + added_delete_files_size;

    // Compute new totals
    let total_records = old_total_records + added_records;
    let total_data_files = old_total_data_files + added_data_files;
    let total_delete_files = old_total_delete_files + added_delete_files;
    let total_file_size = old_total_file_size + added_files_size;

    // Build result map
    let mut result = HashMap::new();
    result.insert("total-records".to_string(), total_records.to_string());
    result.insert("total-data-files".to_string(), total_data_files.to_string());
    result.insert(
        "total-delete-files".to_string(),
        total_delete_files.to_string(),
    );
    result.insert(
        "total-file-size-bytes".to_string(),
        total_file_size.to_string(),
    );
    result.insert("added-records".to_string(), added_records.to_string());
    result.insert("added-data-files".to_string(), added_data_files.to_string());
    result.insert(
        "added-files-size-bytes".to_string(),
        added_files_size.to_string(),
    );

    result
}

pub(crate) fn new_manifest_location(
    table_metadata_location: &str,
    commit_uuid: &str,
    i: usize,
) -> String {
    format!("{table_metadata_location}/metadata/{commit_uuid}-m{i}.avro")
}

pub(crate) fn new_manifest_list_location(
    table_metadata_location: &str,
    snapshot_id: i64,
    attempt: i64,
    commit_uuid: &str,
) -> String {
    format!("{table_metadata_location}/metadata/snap-{snapshot_id}-{attempt}-{commit_uuid}.avro")
}

/// To achieve fast lookups of the datafiles, the manifest tree should be somewhat balanced, meaning that manifest files should contain a similar number of datafiles.
/// This means that manifest files might need to be split up when they get too large. Since the number of datafiles being added by a append operation might be really large,
/// it might even be required to split the manifest file multiple times. *n_splits* stores how many times a manifest file needs to be split to give at most *limit* datafiles per manifest.
pub fn compute_n_splits(
    existing_file_count: usize,
    new_file_count: usize,
    selected_manifest_file_count: usize,
) -> u32 {
    // We want:
    //   nb manifests per manifest list ~= nb data files per manifest
    // Since:
    //   total number of data files = nb manifests per manifest list * nb data files per manifest
    // We shall have:
    //   limit = sqrt(total number of data files)
    let limit = MIN_DATAFILES_PER_MANIFEST
        + ((existing_file_count + new_file_count) as f64).sqrt() as usize;
    let new_manifest_file_count = selected_manifest_file_count + new_file_count;
    match new_manifest_file_count / limit {
        0 => 0,
        x => x.ilog2() + 1,
    }
}

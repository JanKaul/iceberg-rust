/*!
 * Defines the [MaterializedView] struct that represents an iceberg materialized view.
*/

use std::{collections::HashMap, sync::Arc};

use futures::{lock::Mutex, stream, StreamExt, TryStreamExt};
use iceberg_rust_spec::{
    spec::{
        manifest::{Content, DataFile},
        manifest_list::ManifestListEntry,
        materialized_view_metadata::{MaterializedViewMetadata, VersionId},
        schema::Schema,
        snapshot::{generate_snapshot_id, Lineage, SnapshotBuilder, SourceTable},
        table_metadata::{new_metadata_location, TableMetadataBuilder},
        values::Struct,
    },
    util::strip_prefix,
};
use object_store::ObjectStore;

use crate::{
    catalog::{bucket::parse_bucket, identifier::Identifier, tabular::TabularMetadata, Catalog},
    error::Error,
    table::{
        delete_files,
        transaction::operation::{write_manifest, ManifestStatus},
    },
};

use self::{storage_table::StorageTable, transaction::Transaction as MaterializedViewTransaction};

pub mod materialized_view_builder;
mod storage_table;
pub mod transaction;

#[derive(Debug)]
/// An iceberg materialized view
pub struct MaterializedView {
    /// Type of the View, either filesystem or metastore.
    identifier: Identifier,
    /// Metadata for the iceberg view according to the iceberg view spec
    metadata: MaterializedViewMetadata,
    /// Catalog of the table
    catalog: Arc<dyn Catalog>,
}

/// Storage table states
#[derive(Debug)]
pub enum StorageTableState {
    /// Data in storage table is fresh
    Fresh,
    /// Data in storage table is outdated
    Outdated(i64),
    /// Data in storage table is invalid
    Invalid,
}

/// Public interface of the table.
impl MaterializedView {
    /// Create a new metastore view
    pub async fn new(
        identifier: Identifier,
        catalog: Arc<dyn Catalog>,
        metadata: MaterializedViewMetadata,
    ) -> Result<Self, Error> {
        Ok(MaterializedView {
            identifier,
            metadata,
            catalog,
        })
    }
    /// Get the table identifier in the catalog. Returns None of it is a filesystem view.
    pub fn identifier(&self) -> &Identifier {
        &self.identifier
    }
    /// Get the catalog associated to the view. Returns None if the view is a filesystem view
    pub fn catalog(&self) -> Arc<dyn Catalog> {
        self.catalog.clone()
    }
    /// Get the object_store associated to the view
    pub fn object_store(&self) -> Arc<dyn ObjectStore> {
        self.catalog
            .object_store(parse_bucket(&self.metadata.location).unwrap())
    }
    /// Get the schema of the view
    pub fn current_schema(&self, branch: Option<&str>) -> Result<&Schema, Error> {
        self.metadata.current_schema(branch).map_err(Error::from)
    }
    /// Get the metadata of the view
    pub fn metadata(&self) -> &MaterializedViewMetadata {
        &self.metadata
    }
    /// Create a new transaction for this view
    pub fn new_transaction(&mut self, branch: Option<&str>) -> MaterializedViewTransaction {
        MaterializedViewTransaction::new(self, branch)
    }
    /// Get the storage table of the materialized view
    pub async fn storage_table(&self) -> Result<StorageTable, Error> {
        let storage_table_location = &self.metadata.materialization;
        let bucket = parse_bucket(&storage_table_location)?;
        if let TabularMetadata::Table(metadata) = serde_json::from_str(std::str::from_utf8(
            &self
                .catalog()
                .object_store(bucket)
                .get(&strip_prefix(&storage_table_location).into())
                .await?
                .bytes()
                .await?,
        )?)? {
            Ok(StorageTable {
                table_metadata: metadata,
            })
        } else {
            Err(Error::InvalidFormat("storage table".to_string()))
        }
    }

    /// Replace the entire storage table with new datafiles
    pub async fn full_refresh(
        &mut self,
        files: Vec<DataFile>,
        version_id: VersionId,
        base_tables: Vec<SourceTable>,
        branch: Option<String>,
    ) -> Result<(), Error> {
        let object_store = self.object_store();

        let old_storage_table_metadata = self.storage_table().await?.table_metadata;
        let schema = old_storage_table_metadata
            .current_schema(branch.as_deref())?
            .clone();

        // Split datafils by partition
        let datafiles = Arc::new(files.into_iter().map(Ok::<_, Error>).try_fold(
            HashMap::<Struct, Vec<DataFile>>::new(),
            |mut acc, x| {
                let x = x?;
                let partition_value = x.partition().clone();
                acc.entry(partition_value).or_default().push(x);
                Ok::<_, Error>(acc)
            },
        )?);

        let snapshot_id = generate_snapshot_id();
        let manifest_list_location = old_storage_table_metadata.location.to_string()
            + "/metadata/snap-"
            + &snapshot_id.to_string()
            + "-"
            + &uuid::Uuid::new_v4().to_string()
            + ".avro";

        let manifest_iter = datafiles.keys().enumerate().map(|(i, partition_value)| {
            let manifest_location = manifest_list_location
                .to_string()
                .trim_end_matches(".avro")
                .to_owned()
                + "-m"
                + &(i).to_string()
                + ".avro";
            let manifest = ManifestListEntry {
                format_version: old_storage_table_metadata.format_version.clone(),
                manifest_path: manifest_location,
                manifest_length: 0,
                partition_spec_id: old_storage_table_metadata.default_spec_id,
                content: Content::Data,
                sequence_number: old_storage_table_metadata.last_sequence_number,
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
            (ManifestStatus::New(manifest), vec![partition_value.clone()])
        });

        let partition_columns = Arc::new(
            old_storage_table_metadata
                .default_partition_spec()?
                .fields()
                .iter()
                .map(|x| schema.fields().get(*x.source_id() as usize))
                .collect::<Option<Vec<_>>>()
                .ok_or(Error::InvalidFormat(
                    "Partition column in schema".to_string(),
                ))?,
        );

        let manifest_list_schema =
            ManifestListEntry::schema(&old_storage_table_metadata.format_version)?;

        let manifest_list_writer = Arc::new(Mutex::new(apache_avro::Writer::new(
            &manifest_list_schema,
            Vec::new(),
        )));

        stream::iter(manifest_iter)
            .then(|(manifest, files): (ManifestStatus, Vec<Struct>)| {
                let object_store = object_store.clone();
                let datafiles = datafiles.clone();
                let partition_columns = partition_columns.clone();
                let branch = branch.clone();
                let schema = &schema;
                let old_storage_table_metadata = &old_storage_table_metadata;
                async move {
                    write_manifest(
                        old_storage_table_metadata,
                        manifest,
                        files,
                        datafiles,
                        schema,
                        &partition_columns,
                        object_store,
                        branch,
                    )
                    .await
                }
            })
            .try_for_each_concurrent(None, |manifest| {
                let manifest_list_writer = manifest_list_writer.clone();
                async move {
                    manifest_list_writer.lock().await.append_ser(manifest)?;
                    Ok(())
                }
            })
            .await?;

        let manifest_list_bytes = Arc::into_inner(manifest_list_writer)
            .unwrap()
            .into_inner()
            .into_inner()?;

        object_store
            .put(
                &strip_prefix(&manifest_list_location).into(),
                manifest_list_bytes.into(),
            )
            .await?;

        let snapshot = SnapshotBuilder::default()
            .with_snapshot_id(snapshot_id)
            .with_sequence_number(0)
            .with_schema_id(*schema.schema_id())
            .with_manifest_list(manifest_list_location)
            .with_lineage(Lineage::new(version_id, base_tables))
            .build()
            .map_err(iceberg_rust_spec::error::Error::from)?;

        let table_metadata = TableMetadataBuilder::default()
            .format_version(old_storage_table_metadata.format_version.clone())
            .location(old_storage_table_metadata.location.clone())
            .schemas(old_storage_table_metadata.schemas.clone())
            .current_schema_id(old_storage_table_metadata.current_schema_id)
            .partition_specs(old_storage_table_metadata.partition_specs.clone())
            .default_spec_id(old_storage_table_metadata.default_spec_id)
            .snapshots(HashMap::from_iter(vec![(snapshot_id, snapshot)]))
            .current_snapshot_id(Some(snapshot_id))
            .build()?;
        let storage_table_location = new_metadata_location(&table_metadata)?;

        let bytes = serde_json::to_vec(&table_metadata)?;

        object_store
            .put(
                &strip_prefix(&storage_table_location.clone()).into(),
                bytes.into(),
            )
            .await?;

        self.new_transaction(branch.as_deref())
            .update_materialization(&storage_table_location)
            .commit()
            .await?;

        delete_files(&old_storage_table_metadata, object_store).await?;

        Ok(())
    }
}

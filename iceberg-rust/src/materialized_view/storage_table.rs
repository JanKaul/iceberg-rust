use std::{
    iter::repeat,
    ops::{Deref, DerefMut},
};

use futures::{stream, StreamExt, TryStreamExt};
use itertools::Itertools;

use crate::{
    catalog::{identifier::Identifier, tabular::Tabular},
    error::Error,
    spec::{
        manifest::DataFile,
        materialized_view_metadata::{BaseTable, VersionId},
        table_metadata::TableMetadataBuilder,
    },
    sql::find_relations,
    table::Table,
};

pub struct StorageTable(pub Table);

impl Deref for StorageTable {
    type Target = Table;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for StorageTable {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[derive(Debug)]
pub enum StorageTableState {
    Fresh,
    Outdated(i64),
    Empty,
}

impl StorageTable {
    #[inline]
    /// Returns the version id of the last refresh.
    pub fn version_id(&self, branch: Option<String>) -> Result<Option<i64>, Error> {
        self.0
            .metadata()
            .current_snapshot(branch.as_deref())?
            .and_then(|snapshot| snapshot.summary.other.get("version-id"))
            .map(|json| Ok(serde_json::from_str::<VersionId>(json)?))
            .transpose()
    }

    /// Return base tables and the optional snapshot ids of the last refresh. If the the optional value is None, the table is fresh. If the optional value is Some(None) the table requires a full refresh.
    pub async fn base_tables(
        &self,
        sql: Option<&str>,
        branch: Option<String>,
    ) -> Result<Vec<(Table, StorageTableState)>, Error> {
        let base_tables = if let Some(sql) = sql {
            find_relations(sql)?
                .into_iter()
                .map(|ident| {
                    Itertools::intersperse(ident.split(".").skip(1), ".").collect::<String>()
                })
                .zip(repeat(None))
                .collect::<Vec<_>>()
        } else {
            self.metadata()
                .current_snapshot(branch.as_deref())?
                .and_then(|snapshot| snapshot.summary.other.get("base-tables"))
                .ok_or(Error::NotFound(
                    "Snapshot summary field".to_string(),
                    "base-tables".to_string(),
                ))
                .and_then(|json| {
                    Ok(serde_json::from_str::<Vec<BaseTable>>(json)?
                        .into_iter()
                        .map(|x| (x.identifier, Some(x.snapshot_id)))
                        .collect())
                })?
        };

        let catalog = self.catalog().clone();

        stream::iter(base_tables.iter())
            .then(|(pointer, snapshot_id)| {
                let catalog = catalog.clone();
                let branch = branch.clone();
                async move {
                    // if !pointer.starts_with("identifier:") {
                    //     return Err(anyhow!("Only identifiers supported as base table pointers"));
                    // }
                    let base_table = match catalog
                        .load_table(&Identifier::parse(
                            &pointer.trim_start_matches("identifier:"),
                        )?)
                        .await?
                    {
                        Tabular::Table(table) => table,
                        Tabular::MaterializedView(mv) => {
                            mv.storage_table(branch.as_deref()).await?.0
                        }
                        _ => return Err(Error::InvalidFormat("storage table".to_string())),
                    };

                    let snapshot_id = if let Some(snapshot_id) = snapshot_id {
                        if base_table
                            .metadata()
                            .current_snapshot(branch.as_deref())?
                            .unwrap()
                            .snapshot_id
                            == *snapshot_id
                        {
                            StorageTableState::Fresh
                        } else {
                            if *snapshot_id == -1 {
                                StorageTableState::Empty
                            } else {
                                StorageTableState::Outdated(*snapshot_id)
                            }
                        }
                    } else {
                        StorageTableState::Empty
                    };
                    Ok((base_table, snapshot_id))
                }
            })
            .try_collect()
            .await
    }
    /// Replace the entire storage table with new datafiles
    pub async fn full_refresh(
        &mut self,
        files: Vec<DataFile>,
        version_id: VersionId,
        base_tables: Vec<BaseTable>,
        branch: Option<String>,
    ) -> Result<(), Error> {
        let object_store = self.object_store();

        let table_identifier = self.identifier().clone();
        let table_catalog = self.catalog().clone();
        let table_metadata_location = self.metadata_location();
        let table_metadata = self.metadata();
        let table_metadata = TableMetadataBuilder::default()
            .format_version(table_metadata.format_version.clone())
            .location(table_metadata.location.clone())
            .schemas(table_metadata.schemas.clone())
            .current_schema_id(table_metadata.current_schema_id)
            .partition_specs(table_metadata.partition_specs.clone())
            .default_spec_id(table_metadata.default_spec_id)
            .build()?;
        let metadata_location = self.new_metadata_location()?;

        let bytes = serde_json::to_vec(&table_metadata)?;

        object_store
            .put(&metadata_location.clone().into(), bytes.into())
            .await?;
        let mut table = if let Tabular::Table(table) = table_catalog
            .update_table(
                table_identifier,
                &metadata_location,
                table_metadata_location,
            )
            .await?
        {
            Ok(table)
        } else {
            Err(Error::InvalidFormat(
                "Entity returnedfrom catalog".to_string(),
            ))
        }?;

        table
            .new_transaction(branch.as_ref().map(String::as_str))
            .append(files)
            .update_snapshot_summary(vec![
                (
                    "version-id".to_string(),
                    serde_json::to_string(&version_id)?,
                ),
                (
                    "base-tables".to_string(),
                    serde_json::to_string(&base_tables)?,
                ),
            ])
            .commit()
            .await?;
        let old = std::mem::replace(self, StorageTable(table));

        old.0.drop().await?;
        Ok(())
    }
}

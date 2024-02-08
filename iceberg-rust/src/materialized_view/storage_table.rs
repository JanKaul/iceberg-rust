use std::ops::{Deref, DerefMut};

use iceberg_rust_spec::spec::{snapshot::SourceTable, table_metadata::TableMetadata};

use crate::error::Error;

pub struct StorageTable {
    pub table_metadata: TableMetadata,
}

impl Deref for StorageTable {
    type Target = TableMetadata;
    fn deref(&self) -> &Self::Target {
        &self.table_metadata
    }
}

impl DerefMut for StorageTable {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.table_metadata
    }
}

impl StorageTable {
    #[inline]
    /// Returns the version id of the last refresh.
    pub fn version_id(&self, branch: Option<String>) -> Result<Option<i64>, Error> {
        Ok(self
            .table_metadata
            .current_snapshot(branch.as_deref())?
            .and_then(|snapshot| snapshot.lineage().as_ref())
            .map(|x| *x.refresh_version_id()))
    }

    pub async fn source_tables(
        &self,
        branch: Option<String>,
    ) -> Result<Option<&Vec<SourceTable>>, Error> {
        Ok(self
            .current_snapshot(branch.as_deref())?
            .and_then(|snapshot| snapshot.lineage().as_ref())
            .map(|x| x.source_tables()))
    }
}

use std::ops::{Deref, DerefMut};

use iceberg_rust_spec::spec::{
    materialized_view_metadata::{depends_on_tables_from_string, SourceTable},
    snapshot::DEPENDS_ON_TABLES,
    table_metadata::TableMetadata,
};

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

    pub async fn source_tables(
        &self,
        branch: Option<String>,
    ) -> Result<Option<Vec<SourceTable>>, Error> {
        self.current_snapshot(branch.as_deref())?
            .and_then(|snapshot| snapshot.summary().other.get(DEPENDS_ON_TABLES))
            .map(|x| depends_on_tables_from_string(x))
            .transpose()
            .map_err(Error::from)
    }
}

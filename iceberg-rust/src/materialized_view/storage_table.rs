use std::ops::{Deref, DerefMut};

use iceberg_rust_spec::spec::{
    materialized_view_metadata::{depends_on_tables_from_string, SourceTable},
    snapshot::DEPENDS_ON_TABLES,
};

use crate::{error::Error, table::Table};

pub struct StorageTable(Table);

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

impl StorageTable {
    pub fn new(table: Table) -> Self {
        Self(table)
    }

    #[inline]
    pub async fn source_tables(
        &self,
        branch: Option<String>,
    ) -> Result<Option<Vec<SourceTable>>, Error> {
        self.metadata()
            .current_snapshot(branch.as_deref())?
            .and_then(|snapshot| snapshot.summary().other.get(DEPENDS_ON_TABLES))
            .map(|x| depends_on_tables_from_string(x))
            .transpose()
            .map_err(Error::from)
    }
}

use std::ops::{Deref, DerefMut};

use iceberg_rust_spec::spec::{
    materialized_view_metadata::{BaseTable, VersionId},
    table_metadata::TableMetadata,
};

use crate::error::Error;

pub(crate) static VERSION_KEY: &str = "version-id";
pub(crate) static BASE_TABLES_KEY: &str = "base-tables";

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
        self.table_metadata
            .current_snapshot(branch.as_deref())?
            .and_then(|snapshot| snapshot.summary.other.get(VERSION_KEY))
            .map(|json| Ok(serde_json::from_str::<VersionId>(json)?))
            .transpose()
    }
    pub async fn base_tables(
        &self,
        branch: Option<String>,
    ) -> Result<Option<Vec<BaseTable>>, Error> {
        self.current_snapshot(branch.as_deref())?
            .and_then(|snapshot| snapshot.summary.other.get(BASE_TABLES_KEY))
            .map(|json| serde_json::from_str::<Vec<BaseTable>>(json))
            .transpose()
            .map_err(Error::from)
    }
}

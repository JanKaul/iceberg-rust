use std::{
    collections::HashMap,
    ops::{Deref, DerefMut},
};

use iceberg_rust_spec::{
    snapshot::REFRESH_VERSION_ID,
    spec::{materialized_view_metadata::RefreshTable, snapshot::REFRESH_TABLES},
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
    pub async fn refresh_tables(
        &self,
        version_id: i64,
        branch: Option<String>,
    ) -> Result<Option<HashMap<i64, i64>>, Error> {
        let current_snapshot = self.metadata().current_snapshot(branch.as_deref())?;
        if Some(version_id)
            == current_snapshot
                .and_then(|snapshot| snapshot.summary().other.get(REFRESH_VERSION_ID))
                .map(|x| str::parse::<i64>(x))
                .transpose()?
        {
            current_snapshot
                .and_then(|snapshot| snapshot.summary().other.get(REFRESH_TABLES))
                .map(|x| {
                    serde_json::from_str::<Vec<RefreshTable>>(x).map(|x| {
                        HashMap::from_iter(x.into_iter().map(|y| (y.sequence_id, y.revision_id)))
                    })
                })
                .transpose()
                .map_err(Error::from)
        } else {
            Ok(None)
        }
    }
}

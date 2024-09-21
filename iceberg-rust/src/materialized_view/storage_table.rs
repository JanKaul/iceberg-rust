use std::ops::{Deref, DerefMut};

use iceberg_rust_spec::materialized_view_metadata::{RefreshState, REFRESH_STATE};

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
    pub async fn refresh_state(
        &self,
        version_id: i64,
        branch: Option<String>,
    ) -> Result<Option<RefreshState>, Error> {
        let current_snapshot = self.metadata().current_snapshot(branch.as_deref())?;
        let refresh_state = current_snapshot
            .and_then(|snapshot| snapshot.summary().other.get(REFRESH_STATE))
            .map(|x| serde_json::from_str::<RefreshState>(x))
            .transpose()?;
        let Some(refresh_state) = refresh_state else {
            return Ok(None);
        };
        if version_id == refresh_state.refresh_version_id {
            Ok(Some(refresh_state))
        } else {
            Ok(None)
        }
    }
}

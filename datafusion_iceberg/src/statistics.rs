use std::ops::Deref;

use datafusion::physical_plan::{ColumnStatistics, Statistics};
use iceberg_rust::{catalog::tabular::Tabular, table::Table};

use crate::error::Error;

use super::table::DataFusionTable;

impl DataFusionTable {
    pub(crate) async fn statistics(&self) -> Result<Statistics, Error> {
        match self.tabular.read().await.deref() {
            Tabular::Table(table) => table_statistics(table, &self.snapshot_range).await,
            Tabular::View(_) => Err(Error::NotSupported("Statistics for views".to_string())),
            Tabular::MaterializedView(mv) => {
                let table = mv.storage_table(None).await.map_err(Error::from)?;
                table_statistics(&table, &self.snapshot_range).await
            }
        }
    }
}

async fn table_statistics(
    table: &Table,
    snapshot_range: &(Option<i64>, Option<i64>),
) -> Result<Statistics, Error> {
    let schema = snapshot_range
        .1
        .and_then(|snapshot_id| table.metadata().schema(snapshot_id).ok().cloned())
        .unwrap_or_else(|| table.current_schema(None).unwrap().clone());
    let manifests = table.manifests(snapshot_range.0, snapshot_range.1).await?;
    let datafiles = table.datafiles(&manifests, None).await?;
    Ok(datafiles.iter().fold(
        Statistics {
            num_rows: Some(0),
            total_byte_size: None,
            column_statistics: Some(vec![
                ColumnStatistics {
                    null_count: None,
                    max_value: None,
                    min_value: None,
                    distinct_count: None
                };
                schema.fields.fields.len()
            ]),
            is_exact: true,
        },
        |acc, _| acc,
    ))
}

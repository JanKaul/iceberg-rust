use std::ops::Deref;

use datafusion::physical_plan::{ColumnStatistics, Statistics};
use iceberg_rust::catalog::tabular::Tabular;

use crate::error::Error;

use super::table::DataFusionTable;

impl DataFusionTable {
    pub(crate) async fn statistics(&self) -> Result<Statistics, Error> {
        match self.tabular.read().await.deref() {
            Tabular::Table(table) => table
                .manifests(self.snapshot_range.0, self.snapshot_range.1)
                .await?
                .iter()
                .try_fold(
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
                            table.schema()?.fields.fields.len()
                        ]),
                        is_exact: true,
                    },
                    |acc, x| {
                        Ok(Statistics {
                            num_rows: acc.num_rows.zip(x.added_files_count).map(
                                |(num_rows, added_files_count)| {
                                    num_rows + added_files_count as usize
                                },
                            ),
                            total_byte_size: None,
                            column_statistics: Some(vec![
                                ColumnStatistics {
                                    null_count: None,
                                    max_value: None,
                                    min_value: None,
                                    distinct_count: None
                                };
                                table.schema()?.fields.fields.len()
                            ]),
                            is_exact: true,
                        })
                    },
                ),
            Tabular::View(_) => Err(Error::NotSupported("Statistics for views".to_string())),
            Tabular::MaterializedView(mv) => {
                let table = mv.storage_table(None).await.map_err(Error::from)?;
                table
                    .manifests(self.snapshot_range.0, self.snapshot_range.1)
                    .await?
                    .iter()
                    .try_fold(
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
                                table.schema()?.fields.fields.len()
                            ]),
                            is_exact: true,
                        },
                        |acc, x| {
                            Ok(Statistics {
                                num_rows: acc.num_rows.zip(x.added_files_count).map(
                                    |(num_rows, added_files_count)| {
                                        num_rows + added_files_count as usize
                                    },
                                ),
                                total_byte_size: None,
                                column_statistics: Some(vec![
                                    ColumnStatistics {
                                        null_count: None,
                                        max_value: None,
                                        min_value: None,
                                        distinct_count: None
                                    };
                                    table.schema()?.fields.fields.len()
                                ]),
                                is_exact: true,
                            })
                        },
                    )
            }
        }
    }
}

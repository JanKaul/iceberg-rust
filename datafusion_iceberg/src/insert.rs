use anyhow::anyhow;
use async_trait::async_trait;
use datafusion::{
    error::DataFusionError,
    execution::TaskContext,
    physical_plan::{insert::DataSink, DisplayAs, DisplayFormatType, SendableRecordBatchStream},
};
use futures::{stream, StreamExt, TryStreamExt};
use iceberg_rust::{arrow::write::write_parquet_partitioned, catalog::relation::Relation};
use std::{fmt, ops::DerefMut, sync::Arc};

use crate::DataFusionTable;

impl DisplayAs for DataFusionTable {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "IcebergTable")
            }
        }
    }
}

#[async_trait]
impl DataSink for DataFusionTable {
    async fn write_all(
        &self,
        data: Vec<SendableRecordBatchStream>,
        _context: &Arc<TaskContext>,
    ) -> Result<u64, DataFusionError> {
        let batches = stream::iter(data.into_iter()).flatten().map_err(Into::into);
        let mut lock = self.tabular.write().await;
        let table = if let Relation::Table(table) = lock.deref_mut() {
            Ok(table)
        } else {
            Err(anyhow!("Can only insert into a table."))
        }
        .map_err(|err| DataFusionError::Internal(format!("{}", err)))?;

        let object_store = table.object_store();
        let schema = table
            .schema()
            .map_err(|err| DataFusionError::Internal(format!("{}", err)))?;
        let location = &table.metadata().location;
        let partition_spec = table
            .metadata()
            .default_partition_spec()
            .map_err(|err| DataFusionError::Internal(format!("{}", err)))?;
        let metadata_files =
            write_parquet_partitioned(location, schema, partition_spec, batches, object_store)
                .await?;

        table
            .new_transaction()
            .append(metadata_files)
            .commit()
            .await
            .map_err(|err| DataFusionError::Internal(format!("{}", err)))?;

        Ok(0)
    }
}

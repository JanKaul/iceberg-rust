use std::sync::Arc;

use datafusion::{
    arrow::error::ArrowError,
    datasource::{empty::EmptyTable, TableProvider},
    prelude::SessionContext,
};
use futures::TryStreamExt;
use iceberg_rust::{
    arrow::write::write_parquet_partitioned, materialized_view::MaterializedView,
    spec::materialized_view_metadata::MaterializedViewRepresentation,
};

use crate::{error::Error, DataFusionTable};

pub async fn refresh_materialized_view(matview: &mut MaterializedView) -> Result<(), Error> {
    let metadata = matview.metadata();
    let ctx = SessionContext::new();

    let base_tables = matview.base_tables().await?;

    // Full refresh

    base_tables
        .into_iter()
        .map(|(base_table, _)| {
            let identifier = base_table.identifier().to_string();
            let table = Arc::new(DataFusionTable::new_table(base_table, None, None))
                as Arc<dyn TableProvider>;
            let schema = table.schema().clone();
            vec![
                (identifier.clone(), table),
                (
                    identifier + "__delta__",
                    Arc::new(EmptyTable::new(schema)) as Arc<dyn TableProvider>,
                ),
            ]
        })
        .flatten()
        .try_for_each(|(identifier, table)| {
            ctx.register_table(&identifier, table)?;
            Ok::<_, Error>(())
        })?;

    let sql = match &matview.metadata().current_version()?.representations[0] {
        MaterializedViewRepresentation::SqlMaterialized {
            sql,
            dialect: _,
            format_version: _,
            storage_table: _,
        } => sql,
    };

    let logical_plan = ctx.state().create_logical_plan(sql).await?;

    let batches = ctx
        .execute_logical_plan(logical_plan)
        .await?
        .execute_stream()
        .await?
        .map_err(ArrowError::from);

    let files = write_parquet_partitioned(
        &metadata.location,
        metadata.current_schema()?,
        matview
            .storage_table()
            .metadata()
            .default_partition_spec()?,
        batches,
        matview.object_store(),
    )
    .await?;

    matview
        .storage_table_mut()
        .new_transaction()
        .append(files)
        .commit()
        .await?;
    Ok(())
}

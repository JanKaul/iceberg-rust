use std::{collections::HashMap, sync::Arc};

use datafusion::{
    arrow::error::ArrowError,
    datasource::{empty::EmptyTable, TableProvider},
    prelude::SessionContext,
    sql::{ResolvedTableReference, TableReference},
};
use futures::{stream, StreamExt, TryStreamExt};
use iceberg_rust::{
    arrow::write::write_parquet_partitioned,
    catalog::{identifier::Identifier, tabular::Tabular, CatalogList},
    materialized_view::{MaterializedView, StorageTableState},
    spec::materialized_view_metadata::{SourceTables, SourceViews},
};
use iceberg_rust::{
    spec::{materialized_view_metadata::RefreshState, view_metadata::ViewRepresentation},
    sql::find_relations,
};
use itertools::Itertools;
use uuid::Uuid;

use crate::{
    error::Error,
    sql::{transform_name, transform_relations},
    DataFusionTable,
};

pub async fn refresh_materialized_view(
    matview: &mut MaterializedView,
    catalog_list: Arc<dyn CatalogList>,
    branch: Option<&str>,
) -> Result<(), Error> {
    let ctx = SessionContext::new();

    let version = matview.metadata().current_version(branch)?;

    let sql = match &version.representations[0] {
        ViewRepresentation::Sql { sql, dialect: _ } => sql,
    };

    let storage_table = matview.storage_table().await?;

    let relations = find_relations(sql)?;

    let branch = branch.map(ToString::to_string);

    let old_refresh_state = Arc::new(
        storage_table
            .refresh_state(matview.metadata().current_version_id, branch.clone())
            .await?,
    );

    // Load source tables
    let source_tables = stream::iter(relations.iter())
        .then(|relation| {
            let catalog_list = catalog_list.clone();
            let branch = branch.clone();
            let old_refresh_state = old_refresh_state.clone();
            async move {
                let reference = TableReference::parse_str(relation).resolve(
                    version.default_catalog().as_deref().unwrap_or("datafusion"),
                    &version.default_namespace()[0],
                );
                let catalog_name = reference.catalog.to_string();
                let identifier = Identifier::new(&[reference.schema.to_string()], &reference.table);
                let catalog = catalog_list.catalog(&catalog_name).ok_or(Error::NotFound(
                    "Catalog".to_owned(),
                    catalog_name.to_owned(),
                ))?;

                let tabular = match catalog.load_tabular(&identifier).await? {
                    Tabular::View(_) => {
                        return Err(Error::InvalidFormat("storage table".to_string()))
                    }
                    x => x,
                };
                let current_snapshot_id = match &tabular {
                    Tabular::Table(table) => Ok(*table
                        .metadata()
                        .current_snapshot(branch.as_deref())?
                        // Fallback to main branch
                        .or(table.metadata().current_snapshot(None)?)
                        .ok_or(Error::NotFound(
                            "Snapshot in source table".to_owned(),
                            (&identifier.name()).to_string(),
                        ))?
                        .snapshot_id()),
                    Tabular::MaterializedView(mv) => {
                        let storage_table = mv.storage_table().await?;
                        Ok(*storage_table
                            .metadata()
                            .current_snapshot(branch.as_deref())?
                            // Fallback to main branch
                            .or(storage_table.metadata().current_snapshot(None)?)
                            .ok_or(Error::NotFound(
                                "Snapshot in source table".to_owned(),
                                (&identifier.name()).to_string(),
                            ))?
                            .snapshot_id())
                    }
                    _ => Err(Error::InvalidFormat("storage table".to_string())),
                }?;

                let uuid = *tabular.metadata().as_ref().uuid();

                let table_state = if let Some(old_refresh_state) = old_refresh_state.as_ref() {
                    let revision_id = old_refresh_state.source_table_states.get(&(uuid, None));
                    if Some(&current_snapshot_id) == revision_id {
                        StorageTableState::Fresh
                    } else if Some(&-1) == revision_id {
                        StorageTableState::Invalid
                    } else if let Some(revision_id) = revision_id {
                        StorageTableState::Outdated(*revision_id)
                    } else {
                        StorageTableState::Invalid
                    }
                } else {
                    StorageTableState::Invalid
                };

                Ok((reference, tabular, table_state, uuid, current_snapshot_id))
            }
        })
        .try_collect::<Vec<_>>()
        .await?;

    if source_tables
        .iter()
        .all(|x| matches!(x.2, StorageTableState::Fresh))
    {
        return Ok(());
    }

    // Register source tables in datafusion context and return lineage information
    let source_table_states = source_tables
        .into_iter()
        .flat_map(|(identifier, source_table, _, uuid, snapshot_id)| {
            let table = Arc::new(DataFusionTable::new(
                source_table,
                None,
                None,
                branch.as_deref(),
            )) as Arc<dyn TableProvider>;
            let schema = table.schema().clone();

            vec![
                (identifier.clone(), uuid, snapshot_id, table),
                (
                    ResolvedTableReference {
                        catalog: identifier.catalog.clone(),
                        schema: identifier.schema.clone(),
                        table: (identifier.table.to_string() + "__delta__").as_str().into(),
                    },
                    uuid,
                    snapshot_id,
                    Arc::new(EmptyTable::new(schema)) as Arc<dyn TableProvider>,
                ),
            ]
        })
        .map(|(identifier, uuid, snapshot_id, table)| {
            ctx.register_table(transform_name(&identifier.to_string()), table)?;
            Ok::<_, Error>((identifier, uuid, snapshot_id))
        })
        .filter_ok(|(identifier, _, _)| !identifier.table.ends_with("__delta__"))
        .map(|x| {
            let (_, uuid, snapshot_id) = x?;
            Ok(((uuid, None), snapshot_id))
        })
        .collect::<Result<HashMap<(Uuid, Option<String>), i64>, Error>>()?;

    let sql_statements = transform_relations(sql)?;

    let logical_plan = ctx.state().create_logical_plan(&sql_statements[0]).await?;

    // Calculate arrow record batches from logical plan
    let batches = ctx
        .execute_logical_plan(logical_plan)
        .await?
        .execute_stream()
        .await?
        .map_err(ArrowError::from);

    // Write arrow record batches to datafiles
    let files = write_parquet_partitioned(
        storage_table.metadata(),
        batches,
        matview.object_store(),
        branch.as_deref(),
    )
    .await?;

    let refresh_version_id = matview.metadata().current_version_id;

    let refresh_state = RefreshState {
        refresh_version_id,
        source_table_states: SourceTables(source_table_states),
        source_view_states: SourceViews(HashMap::new()),
    };
    matview
        .new_transaction(branch.as_deref())
        .full_refresh(files, refresh_state)?
        .commit()
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests {

    use datafusion::{arrow::array::Int64Array, prelude::SessionContext};
    use iceberg_rust::spec::{
        partition::{PartitionField, Transform},
        schema::Schema,
        types::{PrimitiveType, StructField, StructType, Type},
    };
    use iceberg_rust::{
        catalog::CatalogList,
        materialized_view::MaterializedView,
        spec::{
            partition::PartitionSpec,
            view_metadata::{Version, ViewRepresentation},
        },
        table::Table,
    };
    use iceberg_sql_catalog::SqlCatalogList;
    use object_store::{memory::InMemory, ObjectStore};
    use std::sync::Arc;

    use crate::{catalog::catalog::IcebergCatalog, materialized_view::refresh_materialized_view};

    #[tokio::test]
    pub async fn test_datafusion_refresh_materialized_view() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        let catalog_list = Arc::new(
            SqlCatalogList::new("sqlite://", object_store.clone())
                .await
                .unwrap(),
        );

        let catalog = catalog_list.catalog("iceberg").unwrap();

        let schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(
                StructType::builder()
                    .with_struct_field(StructField {
                        id: 1,
                        name: "id".to_string(),
                        required: true,
                        field_type: Type::Primitive(PrimitiveType::Long),
                        doc: None,
                    })
                    .with_struct_field(StructField {
                        id: 2,
                        name: "customer_id".to_string(),
                        required: true,
                        field_type: Type::Primitive(PrimitiveType::Long),
                        doc: None,
                    })
                    .with_struct_field(StructField {
                        id: 3,
                        name: "product_id".to_string(),
                        required: true,
                        field_type: Type::Primitive(PrimitiveType::Long),
                        doc: None,
                    })
                    .with_struct_field(StructField {
                        id: 4,
                        name: "date".to_string(),
                        required: true,
                        field_type: Type::Primitive(PrimitiveType::Date),
                        doc: None,
                    })
                    .with_struct_field(StructField {
                        id: 5,
                        name: "amount".to_string(),
                        required: true,
                        field_type: Type::Primitive(PrimitiveType::Int),
                        doc: None,
                    })
                    .build()
                    .unwrap(),
            )
            .build()
            .unwrap();

        let partition_spec = PartitionSpec::builder()
            .with_spec_id(0)
            .with_partition_field(PartitionField::new(4, 1000, "day", Transform::Day))
            .build()
            .expect("Failed to create partition spec");

        Table::builder()
            .with_name("orders")
            .with_location("/test/orders")
            .with_schema(schema.clone())
            .with_partition_spec(partition_spec)
            .build(&["test".to_owned()], catalog.clone())
            .await
            .expect("Failed to create table");

        let matview_schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(
                StructType::builder()
                    .with_struct_field(StructField {
                        id: 1,
                        name: "product_id".to_string(),
                        required: true,
                        field_type: Type::Primitive(PrimitiveType::Long),
                        doc: None,
                    })
                    .with_struct_field(StructField {
                        id: 2,
                        name: "amount".to_string(),
                        required: true,
                        field_type: Type::Primitive(PrimitiveType::Int),
                        doc: None,
                    })
                    .build()
                    .unwrap(),
            )
            .build()
            .unwrap();

        let mut matview = MaterializedView::builder()
            .with_name("orders_view")
            .with_location("test/orders_view")
            .with_schema(matview_schema)
            .with_view_version(
                Version::builder()
                    .with_representation(ViewRepresentation::sql(
                        "select product_id, amount from iceberg.test.orders where product_id < 3;",
                        None,
                    ))
                    .build()
                    .unwrap(),
            )
            .build(&["test".to_owned()], catalog.clone())
            .await
            .expect("Failed to create materialized view");

        let total_matview_schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(
                StructType::builder()
                    .with_struct_field(StructField {
                        id: 1,
                        name: "product_id".to_string(),
                        required: true,
                        field_type: Type::Primitive(PrimitiveType::Long),
                        doc: None,
                    })
                    .with_struct_field(StructField {
                        id: 2,
                        name: "amount".to_string(),
                        required: true,
                        field_type: Type::Primitive(PrimitiveType::Long),
                        doc: None,
                    })
                    .build()
                    .unwrap(),
            )
            .build()
            .unwrap();

        let mut total_matview = MaterializedView::builder()
            .with_name("total_orders")
            .with_location("test/total_orders")
            .with_schema(total_matview_schema)
            .with_view_version(
                Version::builder()
                    .with_representation(ViewRepresentation::sql(
                        "select product_id, sum(amount) from iceberg.test.orders_view group by product_id;",
                        None,
                    ))
                    .build()
                    .unwrap(),
            )
            .build(&["test".to_owned()], catalog.clone())
            .await
            .expect("Failed to create materialized view");

        // Datafusion

        let datafusion_catalog = Arc::new(
            IcebergCatalog::new(catalog, None)
                .await
                .expect("Failed to create datafusion catalog"),
        );

        let ctx = SessionContext::new();

        ctx.register_catalog("iceberg", datafusion_catalog);

        ctx.sql(
            "INSERT INTO iceberg.test.orders (id, customer_id, product_id, date, amount) VALUES 
                (1, 1, 1, '2020-01-01', 1),
                (2, 2, 1, '2020-01-01', 1),
                (3, 3, 1, '2020-01-01', 3),
                (4, 1, 2, '2020-02-02', 1),
                (5, 1, 1, '2020-02-02', 2),
                (6, 3, 3, '2020-02-02', 3);",
        )
        .await
        .expect("Failed to create query plan for insert")
        .collect()
        .await
        .expect("Failed to insert values into table");

        refresh_materialized_view(&mut matview, catalog_list.clone(), None)
            .await
            .expect("Failed to refresh materialized view");

        let batches = ctx
            .sql(
                "select product_id, sum(amount) from iceberg.test.orders_view group by product_id;",
            )
            .await
            .expect("Failed to create plan for select")
            .collect()
            .await
            .expect("Failed to execute select query");

        for batch in batches {
            if batch.num_rows() != 0 {
                let (order_ids, amounts) = (
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap(),
                    batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap(),
                );
                for (order_id, amount) in order_ids.iter().zip(amounts) {
                    if order_id.unwrap() == 1 {
                        assert_eq!(amount.unwrap(), 7)
                    } else if order_id.unwrap() == 2 {
                        assert_eq!(amount.unwrap(), 1)
                    } else {
                        panic!("Unexpected order id")
                    }
                }
            }
        }

        ctx.sql(
            "INSERT INTO iceberg.test.orders (id, customer_id, product_id, date, amount) VALUES 
                (7, 1, 3, '2020-01-03', 1),
                (8, 2, 1, '2020-01-03', 2),
                (9, 2, 2, '2020-01-03', 1);",
        )
        .await
        .expect("Failed to create query plan for insert")
        .collect()
        .await
        .expect("Failed to insert values into table");

        refresh_materialized_view(&mut matview, catalog_list.clone(), None)
            .await
            .expect("Failed to refresh materialized view");

        let batches = ctx
            .sql(
                "select product_id, sum(amount) from iceberg.test.orders_view group by product_id;",
            )
            .await
            .expect("Failed to create plan for select")
            .collect()
            .await
            .expect("Failed to execute select query");

        for batch in batches {
            if batch.num_rows() != 0 {
                let (order_ids, amounts) = (
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap(),
                    batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap(),
                );
                for (order_id, amount) in order_ids.iter().zip(amounts) {
                    if order_id.unwrap() == 1 {
                        assert_eq!(amount.unwrap(), 9)
                    } else if order_id.unwrap() == 2 {
                        assert_eq!(amount.unwrap(), 2)
                    } else {
                        panic!("Unexpected order id")
                    }
                }
            }
        }

        refresh_materialized_view(&mut total_matview, catalog_list.clone(), None)
            .await
            .expect("Failed to refresh materialized view");

        let batches = ctx
            .sql("select product_id, amount from iceberg.test.total_orders;")
            .await
            .expect("Failed to create plan for select")
            .collect()
            .await
            .expect("Failed to execute select query");

        for batch in batches {
            if batch.num_rows() != 0 {
                let (order_ids, amounts) = (
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap(),
                    batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap(),
                );
                for (order_id, amount) in order_ids.iter().zip(amounts) {
                    if order_id.unwrap() == 1 {
                        assert_eq!(amount.unwrap(), 9)
                    } else if order_id.unwrap() == 2 {
                        assert_eq!(amount.unwrap(), 2)
                    } else {
                        panic!("Unexpected order id")
                    }
                }
            }
        }
    }
}

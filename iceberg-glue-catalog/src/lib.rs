use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use async_trait::async_trait;
use aws_config::{BehaviorVersion, SdkConfig};
use aws_sdk_glue::{
    types::{StorageDescriptor, TableInput},
    Client,
};
use iceberg_rust::{
    catalog::{
        bucket::Bucket,
        commit::{
            apply_table_updates, apply_view_updates, check_table_requirements,
            check_view_requirements, CommitTable, CommitView, TableRequirement,
        },
        create::{CreateMaterializedView, CreateTable, CreateView},
        identifier::Identifier,
        namespace::Namespace,
        tabular::Tabular,
        Catalog, CatalogList,
    },
    error::Error as IcebergError,
    materialized_view::MaterializedView,
    spec::{
        self,
        materialized_view_metadata::MaterializedViewMetadata,
        table_metadata::{new_metadata_location, TableMetadata},
        tabular::TabularMetadata,
        util::strip_prefix,
        view_metadata::ViewMetadata,
    },
    table::Table,
    view::View,
};
use object_store::ObjectStore;
use schema::schema_to_glue;
use uuid::Uuid;

use crate::error::Error;

#[derive(Debug)]
pub struct GlueCatalog {
    name: String,
    client: Client,
    object_store: Arc<dyn ObjectStore>,
    cache: Arc<RwLock<HashMap<Identifier, (String, TabularMetadata)>>>,
}

pub mod error;
pub mod schema;

impl GlueCatalog {
    pub async fn new(
        config: &SdkConfig,
        name: &str,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Self, Error> {
        Ok(GlueCatalog {
            name: name.to_owned(),
            client: Client::new(&config),
            object_store,
            cache: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    pub fn catalog_list(&self) -> Arc<GlueCatalogList> {
        Arc::new(GlueCatalogList {
            client: self.client.clone(),
            object_store: self.object_store.clone(),
        })
    }
}

#[derive(Debug)]
struct TableRef {
    table_namespace: String,
    table_name: String,
    metadata_location: String,
    _previous_metadata_location: Option<String>,
}

// fn query_map(row: &AnyRow) -> Result<TableRef, sqlx::Error> {
//     Ok(TableRef {
//         table_namespace: row.try_get(0)?,
//         table_name: row.try_get(1)?,
//         metadata_location: row.try_get(2)?,
//         _previous_metadata_location: row.try_get::<String, _>(3).map(Some).or_else(|err| {
//             if let sqlx::Error::ColumnDecode {
//                 index: _,
//                 source: _,
//             } = err
//             {
//                 Ok(None)
//             } else {
//                 Err(err)
//             }
//         })?,
//     })
// }

#[async_trait]
impl Catalog for GlueCatalog {
    /// Catalog name
    fn name(&self) -> &str {
        &self.name
    }
    /// Create a namespace in the catalog
    async fn create_namespace(
        &self,
        _namespace: &Namespace,
        _properties: Option<HashMap<String, String>>,
    ) -> Result<HashMap<String, String>, IcebergError> {
        todo!()
    }
    /// Drop a namespace in the catalog
    async fn drop_namespace(&self, _namespace: &Namespace) -> Result<(), IcebergError> {
        todo!()
    }
    /// Load the namespace properties from the catalog
    async fn load_namespace(
        &self,
        _namespace: &Namespace,
    ) -> Result<HashMap<String, String>, IcebergError> {
        todo!()
    }
    /// Update the namespace properties in the catalog
    async fn update_namespace(
        &self,
        _namespace: &Namespace,
        _updates: Option<HashMap<String, String>>,
        _removals: Option<Vec<String>>,
    ) -> Result<(), IcebergError> {
        todo!()
    }
    /// Check if a namespace exists
    async fn namespace_exists(&self, _namespace: &Namespace) -> Result<bool, IcebergError> {
        todo!()
    }
    async fn list_tabulars(&self, namespace: &Namespace) -> Result<Vec<Identifier>, IcebergError> {
        let name = self.name.clone();
        let namespace = namespace.to_string();

        // let rows = {
        //     sqlx::query(&format!("select table_namespace, table_name, metadata_location, previous_metadata_location from iceberg_tables where catalog_name = '{}' and table_namespace = '{}';",&name, &namespace)).fetch_all(&self.pool).await.map_err(Error::from)?
        // };
        // let iter = rows.iter().map(query_map);

        // Ok(iter
        //     .map(|x| {
        //         x.and_then(|y| {
        //             Identifier::parse(&(y.table_namespace.to_string() + "." + &y.table_name), None)
        //                 .map_err(|err| sqlx::Error::Decode(Box::new(err)))
        //         })
        //     })
        //     .collect::<Result<_, sqlx::Error>>()
        //     .map_err(Error::from)?)
        unimplemented!()
    }
    async fn list_namespaces(&self, parent: Option<&str>) -> Result<Vec<Namespace>, IcebergError> {
        if parent.is_some() {
            return Ok(Vec::new());
        }

        let result = self
            .client
            .get_databases()
            .send()
            .await
            .map_err(Error::from)
            .unwrap();

        Ok(result
            .database_list
            .into_iter()
            .map(|x| Namespace::try_new(&[x.name; 1]))
            .collect::<Result<_, spec::error::Error>>()?)
    }
    async fn tabular_exists(&self, identifier: &Identifier) -> Result<bool, IcebergError> {
        let catalog_name = self.name.clone();
        let namespace = identifier.namespace().to_string();
        let name = identifier.name().to_string();

        // let rows = {
        //     sqlx::query(&format!("select table_namespace, table_name, metadata_location, previous_metadata_location from iceberg_tables where catalog_name = '{}' and table_namespace = '{}' and table_name = '{}';",&catalog_name,
        //         &namespace,
        //         &name)).fetch_all(&self.pool).await.map_err(Error::from)?
        // };
        // let mut iter = rows.iter().map(query_map);

        // Ok(iter.next().is_some())
        unimplemented!()
    }
    async fn drop_table(&self, identifier: &Identifier) -> Result<(), IcebergError> {
        let catalog_name = self.name.clone();
        let namespace = identifier.namespace().to_string();
        let name = identifier.name().to_string();

        // sqlx::query(&format!("delete from iceberg_tables where catalog_name = '{}' and table_namespace = '{}' and table_name = '{}';",&catalog_name,
        //         &namespace,
        //         &name)).execute(&self.pool).await.map_err(Error::from)?;
        // Ok(())
        unimplemented!()
    }
    async fn drop_view(&self, identifier: &Identifier) -> Result<(), IcebergError> {
        let catalog_name = self.name.clone();
        let namespace = identifier.namespace().to_string();
        let name = identifier.name().to_string();

        // sqlx::query(&format!("delete from iceberg_tables where catalog_name = '{}' and table_namespace = '{}' and table_name = '{}';",&catalog_name,
        //         &namespace,
        //         &name)).execute(&self.pool).await.map_err(Error::from)?;
        // Ok(())
        unimplemented!()
    }
    async fn drop_materialized_view(&self, identifier: &Identifier) -> Result<(), IcebergError> {
        let catalog_name = self.name.clone();
        let namespace = identifier.namespace().to_string();
        let name = identifier.name().to_string();

        // sqlx::query(&format!("delete from iceberg_tables where catalog_name = '{}' and table_namespace = '{}' and table_name = '{}';",&catalog_name,
        //         &namespace,
        //         &name)).execute(&self.pool).await.map_err(Error::from)?;
        // Ok(())
        unimplemented!()
    }
    async fn load_tabular(
        self: Arc<Self>,
        identifier: &Identifier,
    ) -> Result<Tabular, IcebergError> {
        // let path = {
        //     let catalog_name = self.name.clone();
        //     let namespace = identifier.namespace().to_string();
        //     let name = identifier.name().to_string();

        //     let row = {
        //         sqlx::query(&format!("select table_namespace, table_name, metadata_location, previous_metadata_location from iceberg_tables where catalog_name = '{}' and table_namespace = '{}' and table_name = '{}';",&catalog_name,
        //             &namespace,
        //             &name)).fetch_one(&self.pool).await.map_err(Error::from)?
        //     };
        //     let row = query_map(&row).map_err(Error::from)?;

        //     row.metadata_location
        // };
        // let bytes = &self
        //     .object_store
        //     .get(&strip_prefix(&path).as_str().into())
        //     .await?
        //     .bytes()
        //     .await?;
        // let metadata: TabularMetadata = serde_json::from_slice(bytes)?;
        // self.cache
        //     .write()
        //     .unwrap()
        //     .insert(identifier.clone(), (path.clone(), metadata.clone()));
        // match metadata {
        //     TabularMetadata::Table(metadata) => Ok(Tabular::Table(
        //         Table::new(identifier.clone(), self.clone(), metadata).await?,
        //     )),
        //     TabularMetadata::View(metadata) => Ok(Tabular::View(
        //         View::new(identifier.clone(), self.clone(), metadata).await?,
        //     )),
        //     TabularMetadata::MaterializedView(metadata) => Ok(Tabular::MaterializedView(
        //         MaterializedView::new(identifier.clone(), self.clone(), metadata).await?,
        //     )),
        // }
        unimplemented!()
    }

    async fn create_table(
        self: Arc<Self>,
        identifier: Identifier,
        create_table: CreateTable,
    ) -> Result<Table, IcebergError> {
        let metadata: TableMetadata = create_table.try_into()?;
        // Create metadata
        let location = metadata.location.to_string();

        // Write metadata to object_store
        let bucket = Bucket::from_path(&location)?;
        let object_store = self.object_store(bucket);

        let metadata_json = serde_json::to_string(&metadata)?;
        let metadata_location = new_metadata_location(&metadata);
        object_store
            .put(
                &strip_prefix(&metadata_location).into(),
                metadata_json.into(),
            )
            .await?;

        let schema = metadata.current_schema(None)?;

        self.client
            .create_table()
            .database_name(&identifier.namespace().to_string())
            .table_input(
                TableInput::builder()
                    .name(identifier.name())
                    .storage_descriptor(
                        StorageDescriptor::builder()
                            .location(&metadata_location)
                            .set_columns(schema_to_glue(schema.fields()).ok())
                            .build(),
                    )
                    .build()
                    .map_err(Error::from)?,
            )
            .send()
            .await
            .map_err(Error::from)?;

        let table = self
            .client
            .get_table()
            .database_name(&identifier.namespace().to_string())
            .name(identifier.name())
            .send()
            .await
            .map_err(Error::from)?;

        self.cache.write().unwrap().insert(
            identifier.clone(),
            (
                table
                    .table()
                    .and_then(|x| x.version_id())
                    .ok_or(Error::Text(
                        "Glue create table didn't return a table.".to_owned(),
                    ))?
                    .to_string(),
                metadata.clone().into(),
            ),
        );
        Ok(Table::new(identifier.clone(), self.clone(), metadata).await?)
    }

    async fn create_view(
        self: Arc<Self>,
        identifier: Identifier,
        create_view: CreateView<Option<()>>,
    ) -> Result<View, IcebergError> {
        // let metadata: ViewMetadata = create_view.try_into()?;
        // // Create metadata
        // let location = metadata.location.to_string();

        // // Write metadata to object_store
        // let bucket = Bucket::from_path(&location)?;
        // let object_store = self.object_store(bucket);

        // let metadata_json = serde_json::to_string(&metadata)?;
        // let metadata_location = new_metadata_location(&metadata);
        // object_store
        //     .put(
        //         &strip_prefix(&metadata_location).into(),
        //         metadata_json.into(),
        //     )
        //     .await?;
        // {
        //     let catalog_name = self.name.clone();
        //     let namespace = identifier.namespace().to_string();
        //     let name = identifier.name().to_string();
        //     let metadata_location = metadata_location.to_string();

        //     sqlx::query(&format!("insert into iceberg_tables (catalog_name, table_namespace, table_name, metadata_location) values ('{}', '{}', '{}', '{}');",catalog_name,namespace,name, metadata_location)).execute(&self.pool).await.map_err(Error::from)?;
        // }
        // self.cache.write().unwrap().insert(
        //     identifier.clone(),
        //     (metadata_location.clone(), metadata.clone().into()),
        // );
        // Ok(View::new(identifier.clone(), self.clone(), metadata).await?)
        unimplemented!()
    }

    async fn create_materialized_view(
        self: Arc<Self>,
        identifier: Identifier,
        create_view: CreateMaterializedView,
    ) -> Result<MaterializedView, IcebergError> {
        // let (create_view, create_table) = create_view.into();
        // let metadata: MaterializedViewMetadata = create_view.try_into()?;
        // let table_metadata: TableMetadata = create_table.try_into()?;
        // // Create metadata
        // let location = metadata.location.to_string();

        // // Write metadata to object_store
        // let bucket = Bucket::from_path(&location)?;
        // let object_store = self.object_store(bucket);

        // let metadata_json = serde_json::to_string(&metadata)?;
        // let metadata_location = new_metadata_location(&metadata);

        // let table_metadata_json = serde_json::to_string(&table_metadata)?;
        // let table_metadata_location = new_metadata_location(&table_metadata);
        // let table_identifier = metadata.current_version(None)?.storage_table();
        // object_store
        //     .put(
        //         &strip_prefix(&metadata_location).into(),
        //         metadata_json.into(),
        //     )
        //     .await?;
        // object_store
        //     .put(
        //         &strip_prefix(&table_metadata_location).into(),
        //         table_metadata_json.into(),
        //     )
        //     .await?;
        // {
        //     let mut transaction = self.pool.begin().await.map_err(Error::from)?;
        //     let catalog_name = self.name.clone();
        //     let namespace = identifier.namespace().to_string();
        //     let name = identifier.name().to_string();
        //     let metadata_location = metadata_location.to_string();

        //     sqlx::query(&format!("insert into iceberg_tables (catalog_name, table_namespace, table_name, metadata_location) values ('{}', '{}', '{}', '{}');",catalog_name,namespace,name, metadata_location)).execute(&mut *transaction).await.map_err(Error::from)?;

        //     let table_catalog_name = self.name.clone();
        //     let table_namespace = table_identifier.namespace().to_string();
        //     let table_name = table_identifier.name().to_string();
        //     let table_metadata_location = table_metadata_location.to_string();

        //     sqlx::query(&format!("insert into iceberg_tables (catalog_name, table_namespace, table_name, metadata_location) values ('{}', '{}', '{}', '{}');",table_catalog_name,table_namespace,table_name, table_metadata_location)).execute(&mut *transaction).await.map_err(Error::from)?;

        //     transaction.commit().await.map_err(Error::from)?;
        // }
        // self.cache.write().unwrap().insert(
        //     identifier.clone(),
        //     (metadata_location.clone(), metadata.clone().into()),
        // );
        // Ok(MaterializedView::new(identifier.clone(), self.clone(), metadata).await?)
        unimplemented!()
    }

    async fn update_table(self: Arc<Self>, commit: CommitTable) -> Result<Table, IcebergError> {
        // let identifier = commit.identifier;
        // let Some(entry) = self.cache.read().unwrap().get(&identifier).cloned() else {
        //     #[allow(clippy::if_same_then_else)]
        //     if !matches!(commit.requirements[0], TableRequirement::AssertCreate) {
        //         return Err(IcebergError::InvalidFormat(
        //             "Create table assertion".to_owned(),
        //         ));
        //     } else {
        //         return Err(IcebergError::InvalidFormat(
        //             "Create table assertion".to_owned(),
        //         ));
        //     }
        // };
        // let (previous_metadata_location, metadata) = entry;

        // let TabularMetadata::Table(mut metadata) = metadata else {
        //     return Err(IcebergError::InvalidFormat(
        //         "Table update on entity that is not a table".to_owned(),
        //     ));
        // };
        // if !check_table_requirements(&commit.requirements, &metadata) {
        //     return Err(IcebergError::InvalidFormat(
        //         "Table requirements not valid".to_owned(),
        //     ));
        // }
        // apply_table_updates(&mut metadata, commit.updates)?;
        // let metadata_location = new_metadata_location(&metadata);
        // self.object_store
        //     .put(
        //         &strip_prefix(&metadata_location).into(),
        //         serde_json::to_string(&metadata)?.into(),
        //     )
        //     .await?;
        // let catalog_name = self.name.clone();
        // let namespace = identifier.namespace().to_string();
        // let name = identifier.name().to_string();
        // let metadata_file_location = metadata_location.to_string();
        // let previous_metadata_file_location = previous_metadata_location.to_string();

        // sqlx::query(&format!("update iceberg_tables set metadata_location = '{}', previous_metadata_location = '{}' where catalog_name = '{}' and table_namespace = '{}' and table_name = '{}';", metadata_file_location, previous_metadata_file_location,catalog_name,namespace,name)).execute(&self.pool).await.map_err(Error::from)?;

        // self.cache.write().unwrap().insert(
        //     identifier.clone(),
        //     (metadata_location.clone(), metadata.clone().into()),
        // );

        // Ok(Table::new(identifier.clone(), self.clone(), metadata).await?)
        unimplemented!()
    }

    async fn update_view(
        self: Arc<Self>,
        commit: CommitView<Option<()>>,
    ) -> Result<View, IcebergError> {
        // let identifier = commit.identifier;
        // let Some(entry) = self.cache.read().unwrap().get(&identifier).cloned() else {
        //     return Err(IcebergError::InvalidFormat(
        //         "Create table assertion".to_owned(),
        //     ));
        // };
        // let (previous_metadata_location, mut metadata) = entry;
        // let metadata_location = match &mut metadata {
        //     TabularMetadata::View(metadata) => {
        //         if !check_view_requirements(&commit.requirements, metadata) {
        //             return Err(IcebergError::InvalidFormat(
        //                 "View requirements not valid".to_owned(),
        //             ));
        //         }
        //         apply_view_updates(metadata, commit.updates)?;
        //         let metadata_location = metadata.location.to_string()
        //             + "/metadata/"
        //             + &metadata.current_version_id.to_string()
        //             + "-"
        //             + &Uuid::new_v4().to_string()
        //             + ".metadata.json";
        //         self.object_store
        //             .put(
        //                 &strip_prefix(&metadata_location).into(),
        //                 serde_json::to_string(&metadata)?.into(),
        //             )
        //             .await?;
        //         Ok(metadata_location)
        //     }
        //     _ => Err(IcebergError::InvalidFormat(
        //         "View update on entity that is not a view".to_owned(),
        //     )),
        // }?;

        // let catalog_name = self.name.clone();
        // let namespace = identifier.namespace().to_string();
        // let name = identifier.name().to_string();
        // let metadata_file_location = metadata_location.to_string();
        // let previous_metadata_file_location = previous_metadata_location.to_string();

        // sqlx::query(&format!("update iceberg_tables set metadata_location = '{}', previous_metadata_location = '{}' where catalog_name = '{}' and table_namespace = '{}' and table_name = '{}';", metadata_file_location, previous_metadata_file_location,catalog_name,namespace,name)).execute(&self.pool).await.map_err(Error::from)?;
        // self.cache.write().unwrap().insert(
        //     identifier.clone(),
        //     (metadata_location.clone(), metadata.clone()),
        // );
        // if let TabularMetadata::View(metadata) = metadata {
        //     Ok(View::new(identifier.clone(), self.clone(), metadata).await?)
        // } else {
        //     Err(IcebergError::InvalidFormat(
        //         "Entity is not a view".to_owned(),
        //     ))
        // }
        unimplemented!()
    }
    async fn update_materialized_view(
        self: Arc<Self>,
        commit: CommitView<Identifier>,
    ) -> Result<MaterializedView, IcebergError> {
        // let identifier = commit.identifier;
        // let Some(entry) = self.cache.read().unwrap().get(&identifier).cloned() else {
        //     return Err(IcebergError::InvalidFormat(
        //         "Create table assertion".to_owned(),
        //     ));
        // };
        // let (previous_metadata_location, mut metadata) = entry;
        // let metadata_location = match &mut metadata {
        //     TabularMetadata::MaterializedView(metadata) => {
        //         if !check_view_requirements(&commit.requirements, metadata) {
        //             return Err(IcebergError::InvalidFormat(
        //                 "Materialized view requirements not valid".to_owned(),
        //             ));
        //         }
        //         apply_view_updates(metadata, commit.updates)?;
        //         let metadata_location = metadata.location.to_string()
        //             + "/metadata/"
        //             + &metadata.current_version_id.to_string()
        //             + "-"
        //             + &Uuid::new_v4().to_string()
        //             + ".metadata.json";
        //         self.object_store
        //             .put(
        //                 &strip_prefix(&metadata_location).into(),
        //                 serde_json::to_string(&metadata)?.into(),
        //             )
        //             .await?;
        //         Ok(metadata_location)
        //     }
        //     _ => Err(IcebergError::InvalidFormat(
        //         "Materialized view update on entity that is not a materialized view".to_owned(),
        //     )),
        // }?;

        // let catalog_name = self.name.clone();
        // let namespace = identifier.namespace().to_string();
        // let name = identifier.name().to_string();
        // let metadata_file_location = metadata_location.to_string();
        // let previous_metadata_file_location = previous_metadata_location.to_string();

        // sqlx::query(&format!("update iceberg_tables set metadata_location = '{}', previous_metadata_location = '{}' where catalog_name = '{}' and table_namespace = '{}' and table_name = '{}';", metadata_file_location, previous_metadata_file_location,catalog_name,namespace,name)).execute(&self.pool).await.map_err(Error::from)?;
        // self.cache.write().unwrap().insert(
        //     identifier.clone(),
        //     (metadata_location.clone(), metadata.clone()),
        // );
        // if let TabularMetadata::MaterializedView(metadata) = metadata {
        //     Ok(MaterializedView::new(identifier.clone(), self.clone(), metadata).await?)
        // } else {
        //     Err(IcebergError::InvalidFormat(
        //         "Entity is not a materialized view".to_owned(),
        //     ))
        // }
        unimplemented!()
    }

    async fn register_table(
        self: Arc<Self>,
        identifier: Identifier,
        metadata_location: &str,
    ) -> Result<Table, IcebergError> {
        // let bucket = Bucket::from_path(metadata_location)?;
        // let object_store = self.object_store(bucket);

        // let metadata: TableMetadata = serde_json::from_slice(
        //     &object_store
        //         .get(&metadata_location.into())
        //         .await?
        //         .bytes()
        //         .await?,
        // )?;

        // {
        //     let catalog_name = self.name.clone();
        //     let namespace = identifier.namespace().to_string();
        //     let name = identifier.name().to_string();
        //     let metadata_location = metadata_location.to_string();

        //     sqlx::query(&format!("insert into iceberg_tables (catalog_name, table_namespace, table_name, metadata_location) values ('{}', '{}', '{}', '{}');",catalog_name,namespace,name, metadata_location)).execute(&self.pool).await.map_err(Error::from)?;
        // }
        // self.cache.write().unwrap().insert(
        //     identifier.clone(),
        //     (metadata_location.to_string(), metadata.clone().into()),
        // );
        // Ok(Table::new(identifier.clone(), self.clone(), metadata).await?)
        unimplemented!()
    }

    fn object_store(&self, _: Bucket) -> Arc<dyn object_store::ObjectStore> {
        self.object_store.clone()
    }
}

impl GlueCatalog {
    pub fn duplicate(&self, name: &str) -> Self {
        Self {
            name: name.to_owned(),
            client: self.client.clone(),
            object_store: self.object_store.clone(),
            cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

#[derive(Debug)]
pub struct GlueCatalogList {
    client: Client,
    object_store: Arc<dyn ObjectStore>,
}

impl GlueCatalogList {
    pub async fn new(
        config: &SdkConfig,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Self, Error> {
        let client = Client::new(&config);

        Ok(GlueCatalogList {
            client,
            object_store,
        })
    }
}

#[async_trait]
impl CatalogList for GlueCatalogList {
    fn catalog(&self, name: &str) -> Option<Arc<dyn Catalog>> {
        Some(Arc::new(GlueCatalog {
            name: name.to_owned(),
            client: self.client.clone(),
            object_store: self.object_store.clone(),
            cache: Arc::new(RwLock::new(HashMap::new())),
        }))
    }
    async fn list_catalogs(&self) -> Vec<String> {
        // let rows = {
        //     sqlx::query("select distinct catalog_name from iceberg_tables;")
        //         .fetch_all(&self.pool)
        //         .await
        //         .map_err(Error::from)
        //         .unwrap_or_default()
        // };
        // let iter = rows.iter().map(|row| row.try_get::<String, _>(0));

        // iter.collect::<Result<_, sqlx::Error>>()
        //     .map_err(Error::from)
        //     .unwrap_or_default()
        unimplemented!()
    }
}

#[cfg(test)]
pub mod tests {
    use aws_config::BehaviorVersion;
    use aws_sdk_glue::{types::DatabaseInput, Client};
    use iceberg_rust::{
        catalog::{identifier::Identifier, namespace::Namespace, Catalog},
        spec::{
            schema::Schema,
            types::{PrimitiveType, StructField, StructType, Type},
        },
        table::Table,
    };
    use object_store::{memory::InMemory, ObjectStore};
    use std::{sync::Arc, time::Duration};
    use testcontainers::{
        core::{
            wait::{HttpWaitStrategy, LogWaitStrategy},
            ContainerPort, ExecCommand, WaitFor,
        },
        runners::AsyncRunner,
        GenericImage, ImageExt,
    };
    use testcontainers_modules::localstack::LocalStack;

    use crate::GlueCatalog;

    #[tokio::test]
    async fn test_create_update_drop_table() {
        let moto = GenericImage::new("motoserver/moto", "5.0.20")
            .with_exposed_port(ContainerPort::Tcp(5000))
            .with_wait_for(WaitFor::Log(LogWaitStrategy::stderr(
                " * Running on all addresses (0.0.0.0)",
            )))
            .start()
            .await
            .unwrap();

        let moto_host = moto.get_host().await.unwrap();
        let moto_port = moto.get_host_port_ipv4(5000).await.unwrap();

        let config = aws_config::defaults(BehaviorVersion::v2024_03_28())
            .endpoint_url(
                "http://".to_owned() + &moto_host.to_string() + ":" + &moto_port.to_string(),
            )
            .test_credentials()
            .load()
            .await;

        Client::new(&config)
            .create_database()
            .database_input(DatabaseInput::builder().name("public").build().unwrap())
            .send()
            .await
            .unwrap();

        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let catalog: Arc<dyn Catalog> = Arc::new(
            GlueCatalog::new(&config, "warehouse", object_store)
                .await
                .unwrap(),
        );
        let identifier = Identifier::parse("public.test", None).unwrap();

        let schema = Schema::builder()
            .with_schema_id(0)
            .with_identifier_field_ids(vec![1, 2])
            .with_fields(
                StructType::builder()
                    .with_struct_field(StructField {
                        id: 1,
                        name: "one".to_string(),
                        required: false,
                        field_type: Type::Primitive(PrimitiveType::String),
                        doc: None,
                    })
                    .with_struct_field(StructField {
                        id: 2,
                        name: "two".to_string(),
                        required: false,
                        field_type: Type::Primitive(PrimitiveType::String),
                        doc: None,
                    })
                    .build()
                    .unwrap(),
            )
            .build()
            .unwrap();

        let mut table = Table::builder()
            .with_name(identifier.name())
            .with_location("/")
            .with_schema(schema)
            .build(identifier.namespace(), catalog.clone())
            .await
            .expect("Failed to create table");

        // let exists = Arc::clone(&catalog)
        //     .tabular_exists(&identifier)
        //     .await
        //     .expect("Table doesn't exist");
        // assert!(exists);

        // let tables = catalog
        //     .clone()
        //     .list_tabulars(
        //         &Namespace::try_new(&["load_table".to_owned()])
        //             .expect("Failed to create namespace"),
        //     )
        //     .await
        //     .expect("Failed to list Tables");
        // assert_eq!(tables[0].to_string(), "load_table.table3".to_owned());

        let namespaces = catalog
            .clone()
            .list_namespaces(None)
            .await
            .expect("Failed to list namespaces");
        assert_eq!(namespaces[0].to_string(), "public");

        // let transaction = table.new_transaction(None);
        // transaction.commit().await.expect("Transaction failed.");

        // catalog
        //     .drop_table(&identifier)
        //     .await
        //     .expect("Failed to drop table.");

        // let exists = Arc::clone(&catalog)
        //     .tabular_exists(&identifier)
        //     .await
        //     .expect("Table exists failed");
        // assert!(!exists);
    }
}

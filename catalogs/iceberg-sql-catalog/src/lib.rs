use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use async_trait::async_trait;
use iceberg_rust::{
    catalog::{
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
    object_store::{store::IcebergStore, Bucket, ObjectStoreBuilder},
    spec::{
        identifier::FullIdentifier,
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
use sqlx::{
    any::{install_default_drivers, AnyPoolOptions, AnyRow},
    pool::PoolOptions,
    AnyPool, Executor, Row,
};

use crate::error::Error;

#[derive(Debug)]
pub struct SqlCatalog {
    name: String,
    pool: AnyPool,
    object_store: ObjectStoreBuilder,
    cache: Arc<RwLock<HashMap<Identifier, (String, TabularMetadata)>>>,
}

pub mod error;

impl SqlCatalog {
    pub async fn new(
        url: &str,
        name: &str,
        object_store: ObjectStoreBuilder,
    ) -> Result<Self, Error> {
        install_default_drivers();

        let mut pool_options = PoolOptions::new();

        if url == "sqlite://" {
            pool_options = pool_options.max_connections(1);
        }

        let pool = AnyPoolOptions::after_connect(pool_options, |connection, _| {
            Box::pin(async move {
                connection
                    .execute(
                        "create table if not exists iceberg_tables (
                                catalog_name varchar(255) not null,
                                table_namespace varchar(255) not null,
                                table_name varchar(255) not null,
                                metadata_location varchar(255) not null,
                                previous_metadata_location varchar(255),
                                primary key (catalog_name, table_namespace, table_name)
                            );",
                    )
                    .await?;
                connection
                    .execute(
                        "create table if not exists iceberg_namespace_properties (
                                catalog_name varchar(255) not null,
                                namespace varchar(255) not null,
                                property_key varchar(255),
                                property_value varchar(255),
                                primary key (catalog_name, namespace, property_key)
                            );",
                    )
                    .await?;
                Ok(())
            })
        })
        .connect_lazy(url)?;

        Ok(SqlCatalog {
            name: name.to_owned(),
            pool,
            object_store,
            cache: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    pub fn catalog_list(&self) -> Arc<SqlCatalogList> {
        Arc::new(SqlCatalogList {
            pool: self.pool.clone(),
            object_store: self.object_store.clone(),
        })
    }
    fn default_object_store(&self, bucket: Bucket) -> Arc<dyn object_store::ObjectStore> {
        Arc::new(self.object_store.build(bucket).unwrap())
    }
}

#[derive(Debug)]
struct TableRef {
    table_namespace: String,
    table_name: String,
    metadata_location: String,
    _previous_metadata_location: Option<String>,
}

fn query_map(row: &AnyRow) -> Result<TableRef, sqlx::Error> {
    Ok(TableRef {
        table_namespace: row.try_get(0)?,
        table_name: row.try_get(1)?,
        metadata_location: row.try_get(2)?,
        _previous_metadata_location: row.try_get::<String, _>(3).map(Some).or_else(|err| {
            if let sqlx::Error::ColumnDecode {
                index: _,
                source: _,
            } = err
            {
                Ok(None)
            } else {
                Err(err)
            }
        })?,
    })
}

#[async_trait]
impl Catalog for SqlCatalog {
    /// Catalog name
    fn name(&self) -> &str {
        &self.name
    }
    /// Create a namespace in the catalog
    async fn create_namespace(
        &self,
        namespace: &Namespace,
        properties: Option<HashMap<String, String>>,
    ) -> Result<HashMap<String, String>, IcebergError> {
        let catalog_name = self.name.clone();
        let namespace_str = namespace.to_string();
        let properties = properties.unwrap_or_default();

        // Insert namespace properties into the database
        for (key, value) in &properties {
            sqlx::query(&format!(
                "insert into iceberg_namespace_properties (catalog_name, namespace, property_key, property_value) values ('{catalog_name}', '{namespace_str}', '{key}', '{value}');"
            ))
            .execute(&self.pool)
            .await
            .map_err(Error::from)?;
        }

        // If no properties were provided, still create an entry to mark the namespace as existing
        if properties.is_empty() {
            sqlx::query(&format!(
                "insert into iceberg_namespace_properties (catalog_name, namespace, property_key, property_value) values ('{catalog_name}', '{namespace_str}', 'exists', 'true');"
            ))
            .execute(&self.pool)
            .await
            .map_err(Error::from)?;
        }

        Ok(properties)
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

        let rows = {
            sqlx::query(&format!("select table_namespace, table_name, metadata_location, previous_metadata_location from iceberg_tables where catalog_name = '{}' and table_namespace = '{}';",&name, &namespace)).fetch_all(&self.pool).await.map_err(Error::from)?
        };
        let iter = rows.iter().map(query_map);

        Ok(iter
            .map(|x| {
                x.and_then(|y| {
                    Identifier::parse(&(y.table_namespace.to_string() + "." + &y.table_name), None)
                        .map_err(|err| sqlx::Error::Decode(Box::new(err)))
                })
            })
            .collect::<Result<_, sqlx::Error>>()
            .map_err(Error::from)?)
    }
    async fn list_namespaces(&self, _parent: Option<&str>) -> Result<Vec<Namespace>, IcebergError> {
        let name = self.name.clone();

        let rows = {
            sqlx::query(&format!(
                "select distinct namespace from iceberg_namespace_properties where catalog_name = '{name}';",
            ))
            .fetch_all(&self.pool)
            .await
            .map_err(Error::from)?
        };
        let iter = rows.iter().map(|row| row.try_get::<String, _>(0));

        Ok(iter
            .map(|x| {
                x.and_then(|y| {
                    Namespace::try_new(&y.split('.').map(ToString::to_string).collect::<Vec<_>>())
                        .map_err(|err| sqlx::Error::Decode(Box::new(err)))
                })
            })
            .collect::<Result<_, sqlx::Error>>()
            .map_err(Error::from)?)
    }
    async fn tabular_exists(&self, identifier: &Identifier) -> Result<bool, IcebergError> {
        let catalog_name = self.name.clone();
        let namespace = identifier.namespace().to_string();
        let name = identifier.name().to_string();

        let rows = {
            sqlx::query(&format!("select table_namespace, table_name, metadata_location, previous_metadata_location from iceberg_tables where catalog_name = '{}' and table_namespace = '{}' and table_name = '{}';",&catalog_name,
                &namespace,
                &name)).fetch_all(&self.pool).await.map_err(Error::from)?
        };
        let mut iter = rows.iter().map(query_map);

        Ok(iter.next().is_some())
    }
    async fn drop_table(&self, identifier: &Identifier) -> Result<(), IcebergError> {
        let catalog_name = self.name.clone();
        let namespace = identifier.namespace().to_string();
        let name = identifier.name().to_string();

        sqlx::query(&format!("delete from iceberg_tables where catalog_name = '{}' and table_namespace = '{}' and table_name = '{}';",&catalog_name,
                &namespace,
                &name)).execute(&self.pool).await.map_err(Error::from)?;
        Ok(())
    }
    async fn drop_view(&self, identifier: &Identifier) -> Result<(), IcebergError> {
        let catalog_name = self.name.clone();
        let namespace = identifier.namespace().to_string();
        let name = identifier.name().to_string();

        sqlx::query(&format!("delete from iceberg_tables where catalog_name = '{}' and table_namespace = '{}' and table_name = '{}';",&catalog_name,
                &namespace,
                &name)).execute(&self.pool).await.map_err(Error::from)?;
        Ok(())
    }
    async fn drop_materialized_view(&self, identifier: &Identifier) -> Result<(), IcebergError> {
        let catalog_name = self.name.clone();
        let namespace = identifier.namespace().to_string();
        let name = identifier.name().to_string();

        sqlx::query(&format!("delete from iceberg_tables where catalog_name = '{}' and table_namespace = '{}' and table_name = '{}';",&catalog_name,
                &namespace,
                &name)).execute(&self.pool).await.map_err(Error::from)?;
        Ok(())
    }
    async fn load_tabular(
        self: Arc<Self>,
        identifier: &Identifier,
    ) -> Result<Tabular, IcebergError> {
        let path = {
            let catalog_name = self.name.clone();
            let namespace = identifier.namespace().to_string();
            let name = identifier.name().to_string();

            let row = {
                sqlx::query(&format!("select table_namespace, table_name, metadata_location, previous_metadata_location from iceberg_tables where catalog_name = '{}' and table_namespace = '{}' and table_name = '{}';",&catalog_name,
                    &namespace,
                    &name)).fetch_one(&self.pool).await.map_err(|_| IcebergError::CatalogNotFound)?
            };
            let row = query_map(&row).map_err(Error::from)?;

            row.metadata_location
        };

        let bucket = Bucket::from_path(&path)?;
        let object_store = self.default_object_store(bucket);

        let bytes = object_store
            .get(&strip_prefix(&path).as_str().into())
            .await?
            .bytes()
            .await?;
        let metadata: TabularMetadata = serde_json::from_slice(&bytes)?;
        self.cache
            .write()
            .unwrap()
            .insert(identifier.clone(), (path.clone(), metadata.clone()));
        match metadata {
            TabularMetadata::Table(metadata) => Ok(Tabular::Table(
                Table::new(
                    identifier.clone(),
                    self.clone(),
                    object_store.clone(),
                    metadata,
                )
                .await?,
            )),
            TabularMetadata::View(metadata) => Ok(Tabular::View(
                View::new(identifier.clone(), self.clone(), metadata).await?,
            )),
            TabularMetadata::MaterializedView(metadata) => Ok(Tabular::MaterializedView(
                MaterializedView::new(identifier.clone(), self.clone(), metadata).await?,
            )),
        }
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
        let object_store = self.default_object_store(bucket);

        let metadata_location = new_metadata_location(&metadata);
        object_store
            .put_metadata(&metadata_location, metadata.as_ref())
            .await?;

        object_store.put_version_hint(&metadata_location).await.ok();
        {
            let catalog_name = self.name.clone();
            let namespace = identifier.namespace().to_string();
            let name = identifier.name().to_string();
            let metadata_location = metadata_location.to_string();

            sqlx::query(&format!("insert into iceberg_tables (catalog_name, table_namespace, table_name, metadata_location) values ('{catalog_name}', '{namespace}', '{name}', '{metadata_location}');")).execute(&self.pool).await.map_err(Error::from)?;
        }
        self.cache.write().unwrap().insert(
            identifier.clone(),
            (metadata_location.clone(), metadata.clone().into()),
        );
        Ok(Table::new(
            identifier.clone(),
            self.clone(),
            object_store.clone(),
            metadata,
        )
        .await?)
    }

    async fn create_view(
        self: Arc<Self>,
        identifier: Identifier,
        create_view: CreateView<Option<()>>,
    ) -> Result<View, IcebergError> {
        let metadata: ViewMetadata = create_view.try_into()?;
        // Create metadata
        let location = metadata.location.to_string();

        // Write metadata to object_store
        let bucket = Bucket::from_path(&location)?;
        let object_store = self.default_object_store(bucket);

        let metadata_location = new_metadata_location(&metadata);
        object_store
            .put_metadata(&metadata_location, metadata.as_ref())
            .await?;

        object_store.put_version_hint(&metadata_location).await.ok();
        {
            let catalog_name = self.name.clone();
            let namespace = identifier.namespace().to_string();
            let name = identifier.name().to_string();
            let metadata_location = metadata_location.to_string();

            sqlx::query(&format!("insert into iceberg_tables (catalog_name, table_namespace, table_name, metadata_location) values ('{catalog_name}', '{namespace}', '{name}', '{metadata_location}');")).execute(&self.pool).await.map_err(Error::from)?;
        }
        self.cache.write().unwrap().insert(
            identifier.clone(),
            (metadata_location.clone(), metadata.clone().into()),
        );
        Ok(View::new(identifier.clone(), self.clone(), metadata).await?)
    }

    async fn create_materialized_view(
        self: Arc<Self>,
        identifier: Identifier,
        create_view: CreateMaterializedView,
    ) -> Result<MaterializedView, IcebergError> {
        let (create_view, create_table) = create_view.into();
        let metadata: MaterializedViewMetadata = create_view.try_into()?;
        let table_metadata: TableMetadata = create_table.try_into()?;
        // Create metadata
        let location = metadata.location.to_string();

        // Write metadata to object_store
        let bucket = Bucket::from_path(&location)?;
        let object_store = self.default_object_store(bucket);

        let metadata_location = new_metadata_location(&metadata);

        let table_metadata_location = new_metadata_location(&table_metadata);
        let table_identifier = metadata.current_version(None)?.storage_table();
        object_store
            .put_metadata(&metadata_location, metadata.as_ref())
            .await?;
        object_store.put_version_hint(&metadata_location).await.ok();

        object_store
            .put_metadata(&table_metadata_location, table_metadata.as_ref())
            .await?;
        {
            let mut transaction = self.pool.begin().await.map_err(Error::from)?;
            let catalog_name = self.name.clone();
            let namespace = identifier.namespace().to_string();
            let name = identifier.name().to_string();
            let metadata_location = metadata_location.to_string();

            sqlx::query(&format!("insert into iceberg_tables (catalog_name, table_namespace, table_name, metadata_location) values ('{catalog_name}', '{namespace}', '{name}', '{metadata_location}');")).execute(&mut *transaction).await.map_err(Error::from)?;

            let table_catalog_name = self.name.clone();
            let table_namespace = table_identifier.namespace().to_string();
            let table_name = table_identifier.name().to_string();
            let table_metadata_location = table_metadata_location.to_string();

            sqlx::query(&format!("insert into iceberg_tables (catalog_name, table_namespace, table_name, metadata_location) values ('{table_catalog_name}', '{table_namespace}', '{table_name}', '{table_metadata_location}');")).execute(&mut *transaction).await.map_err(Error::from)?;

            transaction.commit().await.map_err(Error::from)?;
        }
        self.cache.write().unwrap().insert(
            identifier.clone(),
            (metadata_location.clone(), metadata.clone().into()),
        );
        Ok(MaterializedView::new(identifier.clone(), self.clone(), metadata).await?)
    }

    async fn update_table(self: Arc<Self>, commit: CommitTable) -> Result<Table, IcebergError> {
        let identifier = commit.identifier;
        let Some(entry) = self.cache.read().unwrap().get(&identifier).cloned() else {
            #[allow(clippy::if_same_then_else)]
            if !matches!(commit.requirements[0], TableRequirement::AssertCreate) {
                return Err(IcebergError::InvalidFormat(
                    "Create table assertion".to_owned(),
                ));
            } else {
                return Err(IcebergError::InvalidFormat(
                    "Create table assertion".to_owned(),
                ));
            }
        };
        let (previous_metadata_location, metadata) = entry;

        let bucket = Bucket::from_path(&previous_metadata_location)?;
        let object_store = self.default_object_store(bucket);

        let TabularMetadata::Table(mut metadata) = metadata else {
            return Err(IcebergError::InvalidFormat(
                "Table update on entity that is not a table".to_owned(),
            ));
        };
        if !check_table_requirements(&commit.requirements, &metadata) {
            return Err(IcebergError::InvalidFormat(
                "Table requirements not valid".to_owned(),
            ));
        }
        apply_table_updates(&mut metadata, commit.updates)?;
        let metadata_location = new_metadata_location(&metadata);
        object_store
            .put_metadata(&metadata_location, metadata.as_ref())
            .await?;
        object_store.put_version_hint(&metadata_location).await.ok();

        let catalog_name = self.name.clone();
        let namespace = identifier.namespace().to_string();
        let name = identifier.name().to_string();
        let metadata_file_location = metadata_location.to_string();
        let previous_metadata_file_location = previous_metadata_location.to_string();

        sqlx::query(&format!("update iceberg_tables set metadata_location = '{metadata_file_location}', previous_metadata_location = '{previous_metadata_file_location}' where catalog_name = '{catalog_name}' and table_namespace = '{namespace}' and table_name = '{name}' and metadata_location = '{previous_metadata_file_location}';")).execute(&self.pool).await.map_err(Error::from)?;

        self.cache.write().unwrap().insert(
            identifier.clone(),
            (metadata_location.clone(), metadata.clone().into()),
        );

        Ok(Table::new(
            identifier.clone(),
            self.clone(),
            object_store.clone(),
            metadata,
        )
        .await?)
    }

    async fn update_view(
        self: Arc<Self>,
        commit: CommitView<Option<()>>,
    ) -> Result<View, IcebergError> {
        let identifier = commit.identifier;
        let Some(entry) = self.cache.read().unwrap().get(&identifier).cloned() else {
            return Err(IcebergError::InvalidFormat(
                "Create table assertion".to_owned(),
            ));
        };
        let (previous_metadata_location, mut metadata) = entry;

        let bucket = Bucket::from_path(&previous_metadata_location)?;
        let object_store = self.default_object_store(bucket);

        let metadata_location = match &mut metadata {
            TabularMetadata::View(metadata) => {
                if !check_view_requirements(&commit.requirements, metadata) {
                    return Err(IcebergError::InvalidFormat(
                        "View requirements not valid".to_owned(),
                    ));
                }
                apply_view_updates(metadata, commit.updates)?;
                let metadata_location = new_metadata_location(&*metadata);
                object_store
                    .put_metadata(&metadata_location, metadata.as_ref())
                    .await?;
                object_store.put_version_hint(&metadata_location).await.ok();

                Ok(metadata_location)
            }
            _ => Err(IcebergError::InvalidFormat(
                "View update on entity that is not a view".to_owned(),
            )),
        }?;

        let catalog_name = self.name.clone();
        let namespace = identifier.namespace().to_string();
        let name = identifier.name().to_string();
        let metadata_file_location = metadata_location.to_string();
        let previous_metadata_file_location = previous_metadata_location.to_string();

        sqlx::query(&format!("update iceberg_tables set metadata_location = '{metadata_file_location}', previous_metadata_location = '{previous_metadata_file_location}' where catalog_name = '{catalog_name}' and table_namespace = '{namespace}' and table_name = '{name}' and metadata_location = '{previous_metadata_file_location}';")).execute(&self.pool).await.map_err(Error::from)?;
        self.cache.write().unwrap().insert(
            identifier.clone(),
            (metadata_location.clone(), metadata.clone()),
        );
        if let TabularMetadata::View(metadata) = metadata {
            Ok(View::new(identifier.clone(), self.clone(), metadata).await?)
        } else {
            Err(IcebergError::InvalidFormat(
                "Entity is not a view".to_owned(),
            ))
        }
    }
    async fn update_materialized_view(
        self: Arc<Self>,
        commit: CommitView<FullIdentifier>,
    ) -> Result<MaterializedView, IcebergError> {
        let identifier = commit.identifier;
        let Some(entry) = self.cache.read().unwrap().get(&identifier).cloned() else {
            return Err(IcebergError::InvalidFormat(
                "Create table assertion".to_owned(),
            ));
        };
        let (previous_metadata_location, mut metadata) = entry;

        let bucket = Bucket::from_path(&previous_metadata_location)?;
        let object_store = self.default_object_store(bucket);

        let metadata_location = match &mut metadata {
            TabularMetadata::MaterializedView(metadata) => {
                if !check_view_requirements(&commit.requirements, metadata) {
                    return Err(IcebergError::InvalidFormat(
                        "Materialized view requirements not valid".to_owned(),
                    ));
                }
                apply_view_updates(metadata, commit.updates)?;

                let metadata_location = new_metadata_location(&*metadata);
                object_store
                    .put_metadata(&metadata_location, metadata.as_ref())
                    .await?;
                object_store.put_version_hint(&metadata_location).await.ok();

                Ok(metadata_location)
            }
            _ => Err(IcebergError::InvalidFormat(
                "Materialized view update on entity that is not a materialized view".to_owned(),
            )),
        }?;

        let catalog_name = self.name.clone();
        let namespace = identifier.namespace().to_string();
        let name = identifier.name().to_string();
        let metadata_file_location = metadata_location.to_string();
        let previous_metadata_file_location = previous_metadata_location.to_string();

        sqlx::query(&format!("update iceberg_tables set metadata_location = '{metadata_file_location}', previous_metadata_location = '{previous_metadata_file_location}' where catalog_name = '{catalog_name}' and table_namespace = '{namespace}' and table_name = '{name}' and metadata_location = '{previous_metadata_file_location}';")).execute(&self.pool).await.map_err(Error::from)?;
        self.cache.write().unwrap().insert(
            identifier.clone(),
            (metadata_location.clone(), metadata.clone()),
        );
        if let TabularMetadata::MaterializedView(metadata) = metadata {
            Ok(MaterializedView::new(identifier.clone(), self.clone(), metadata).await?)
        } else {
            Err(IcebergError::InvalidFormat(
                "Entity is not a materialized view".to_owned(),
            ))
        }
    }

    async fn register_table(
        self: Arc<Self>,
        identifier: Identifier,
        metadata_location: &str,
    ) -> Result<Table, IcebergError> {
        let bucket = Bucket::from_path(metadata_location)?;
        let object_store = self.default_object_store(bucket);

        let metadata: TableMetadata = serde_json::from_slice(
            &object_store
                .get(&metadata_location.into())
                .await?
                .bytes()
                .await?,
        )?;

        {
            let catalog_name = self.name.clone();
            let namespace = identifier.namespace().to_string();
            let name = identifier.name().to_string();
            let metadata_location = metadata_location.to_string();

            sqlx::query(&format!("insert into iceberg_tables (catalog_name, table_namespace, table_name, metadata_location) values ('{catalog_name}', '{namespace}', '{name}', '{metadata_location}');")).execute(&self.pool).await.map_err(Error::from)?;
        }
        self.cache.write().unwrap().insert(
            identifier.clone(),
            (metadata_location.to_string(), metadata.clone().into()),
        );
        Ok(Table::new(
            identifier.clone(),
            self.clone(),
            object_store.clone(),
            metadata,
        )
        .await?)
    }
}

impl SqlCatalog {
    pub fn duplicate(&self, name: &str) -> Self {
        Self {
            name: name.to_owned(),
            pool: self.pool.clone(),
            object_store: self.object_store.clone(),
            cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

#[derive(Debug)]
pub struct SqlCatalogList {
    pool: AnyPool,
    object_store: ObjectStoreBuilder,
}

impl SqlCatalogList {
    pub async fn new(url: &str, object_store: ObjectStoreBuilder) -> Result<Self, Error> {
        install_default_drivers();

        let mut pool_options = PoolOptions::new();

        if url.starts_with("sqlite") {
            pool_options = pool_options.max_connections(1);
        }

        let pool = AnyPoolOptions::after_connect(pool_options, |connection, _| {
            Box::pin(async move {
                connection
                    .execute(
                        "create table if not exists iceberg_tables (
                                catalog_name varchar(255) not null,
                                table_namespace varchar(255) not null,
                                table_name varchar(255) not null,
                                metadata_location varchar(255) not null,
                                previous_metadata_location varchar(255),
                                primary key (catalog_name, table_namespace, table_name)
                            );",
                    )
                    .await?;
                connection
                    .execute(
                        "create table if not exists iceberg_namespace_properties (
                                catalog_name varchar(255) not null,
                                namespace varchar(255) not null,
                                property_key varchar(255),
                                property_value varchar(255),
                                primary key (catalog_name, namespace, property_key)
                            );",
                    )
                    .await?;
                Ok(())
            })
        })
        .connect(url)
        .await?;

        Ok(SqlCatalogList { pool, object_store })
    }
}

#[async_trait]
impl CatalogList for SqlCatalogList {
    fn catalog(&self, name: &str) -> Option<Arc<dyn Catalog>> {
        Some(Arc::new(SqlCatalog {
            name: name.to_owned(),
            pool: self.pool.clone(),
            object_store: self.object_store.clone(),
            cache: Arc::new(RwLock::new(HashMap::new())),
        }))
    }
    async fn list_catalogs(&self) -> Vec<String> {
        let rows = {
            sqlx::query("select distinct catalog_name from iceberg_tables;")
                .fetch_all(&self.pool)
                .await
                .map_err(Error::from)
                .unwrap_or_default()
        };
        let iter = rows.iter().map(|row| row.try_get::<String, _>(0));

        iter.collect::<Result<_, sqlx::Error>>()
            .map_err(Error::from)
            .unwrap_or_default()
    }
}

#[cfg(test)]
pub mod tests {
    use datafusion::{
        arrow::array::{Float64Array, Int64Array},
        common::tree_node::{TransformedResult, TreeNode},
        execution::SessionStateBuilder,
        prelude::SessionContext,
    };
    use datafusion_iceberg::{
        catalog::catalog::IcebergCatalog,
        planner::{iceberg_transform, IcebergQueryPlanner},
    };
    use iceberg_rust::{
        catalog::{namespace::Namespace, Catalog},
        object_store::ObjectStoreBuilder,
        spec::util::strip_prefix,
    };
    use testcontainers::{core::ExecCommand, runners::AsyncRunner, ImageExt};
    use testcontainers_modules::{localstack::LocalStack, postgres::Postgres};
    use tokio::time::sleep;

    use crate::SqlCatalog;
    use iceberg_rust::object_store::store::version_hint_content;
    use std::{sync::Arc, time::Duration};

    #[tokio::test]
    async fn test_create_update_drop_table() {
        let localstack = LocalStack::default()
            .with_env_var("SERVICES", "s3")
            .with_env_var("AWS_ACCESS_KEY_ID", "user")
            .with_env_var("AWS_SECRET_ACCESS_KEY", "password")
            .start()
            .await
            .unwrap();

        let command = localstack
            .exec(ExecCommand::new(vec![
                "awslocal",
                "s3api",
                "create-bucket",
                "--bucket",
                "warehouse",
            ]))
            .await
            .unwrap();

        while command.exit_code().await.unwrap().is_none() {
            sleep(Duration::from_millis(100)).await;
        }

        let postgres = Postgres::default()
            .with_db_name("postgres")
            .with_user("postgres")
            .with_password("postgres")
            .start()
            .await
            .unwrap();

        let postgres_host = postgres.get_host().await.unwrap();
        let postgres_port = postgres.get_host_port_ipv4(5432).await.unwrap();

        while command.exit_code().await.unwrap().is_none() {
            sleep(Duration::from_millis(100)).await;
        }

        let localstack_host = localstack.get_host().await.unwrap();
        let localstack_port = localstack.get_host_port_ipv4(4566).await.unwrap();

        let object_store = ObjectStoreBuilder::s3()
            .with_config("aws_access_key_id", "user")
            .unwrap()
            .with_config("aws_secret_access_key", "password")
            .unwrap()
            .with_config(
                "endpoint",
                format!("http://{localstack_host}:{localstack_port}"),
            )
            .unwrap()
            .with_config("region", "us-east-1")
            .unwrap()
            .with_config("allow_http", "true")
            .unwrap();

        let iceberg_catalog = Arc::new(
            SqlCatalog::new(
                &format!("postgres://postgres:postgres@{postgres_host}:{postgres_port}/postgres"),
                "warehouse",
                object_store,
            )
            .await
            .unwrap(),
        );

        let catalog = Arc::new(
            IcebergCatalog::new(iceberg_catalog.clone(), None)
                .await
                .unwrap(),
        );

        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_query_planner(Arc::new(IcebergQueryPlanner::new()))
            .build();

        let ctx = SessionContext::new_with_state(state);

        ctx.register_catalog("warehouse", catalog);

        let sql = &"CREATE SCHEMA warehouse.tpch;".to_string();

        let plan = ctx.state().create_logical_plan(sql).await.unwrap();

        let transformed = plan.transform(iceberg_transform).data().unwrap();

        ctx.execute_logical_plan(transformed)
            .await
            .unwrap()
            .collect()
            .await
            .expect("Failed to execute query plan.");

        let sql = "CREATE EXTERNAL TABLE lineitem ( 
    L_ORDERKEY BIGINT NOT NULL, 
    L_PARTKEY BIGINT NOT NULL, 
    L_SUPPKEY BIGINT NOT NULL, 
    L_LINENUMBER INT NOT NULL, 
    L_QUANTITY DOUBLE NOT NULL, 
    L_EXTENDED_PRICE DOUBLE NOT NULL, 
    L_DISCOUNT DOUBLE NOT NULL, 
    L_TAX DOUBLE NOT NULL, 
    L_RETURNFLAG CHAR NOT NULL, 
    L_LINESTATUS CHAR NOT NULL, 
    L_SHIPDATE DATE NOT NULL, 
    L_COMMITDATE DATE NOT NULL, 
    L_RECEIPTDATE DATE NOT NULL, 
    L_SHIPINSTRUCT VARCHAR NOT NULL, 
    L_SHIPMODE VARCHAR NOT NULL, 
    L_COMMENT VARCHAR NOT NULL ) STORED AS CSV LOCATION '../../datafusion_iceberg/testdata/tpch/lineitem.csv' OPTIONS ('has_header' 'false');";

        let plan = ctx.state().create_logical_plan(sql).await.unwrap();

        let transformed = plan.transform(iceberg_transform).data().unwrap();

        ctx.execute_logical_plan(transformed)
            .await
            .unwrap()
            .collect()
            .await
            .expect("Failed to execute query plan.");

        let sql = "CREATE EXTERNAL TABLE warehouse.tpch.lineitem ( 
    L_ORDERKEY BIGINT NOT NULL, 
    L_PARTKEY BIGINT NOT NULL, 
    L_SUPPKEY BIGINT NOT NULL, 
    L_LINENUMBER INT NOT NULL, 
    L_QUANTITY DOUBLE NOT NULL, 
    L_EXTENDED_PRICE DOUBLE NOT NULL, 
    L_DISCOUNT DOUBLE NOT NULL, 
    L_TAX DOUBLE NOT NULL, 
    L_RETURNFLAG CHAR NOT NULL, 
    L_LINESTATUS CHAR NOT NULL, 
    L_SHIPDATE DATE NOT NULL, 
    L_COMMITDATE DATE NOT NULL, 
    L_RECEIPTDATE DATE NOT NULL, 
    L_SHIPINSTRUCT VARCHAR NOT NULL, 
    L_SHIPMODE VARCHAR NOT NULL, 
    L_COMMENT VARCHAR NOT NULL ) STORED AS ICEBERG LOCATION 's3://warehouse/tpch/lineitem' PARTITIONED BY ( \"month(L_SHIPDATE)\" );";

        let plan = ctx.state().create_logical_plan(sql).await.unwrap();

        let transformed = plan.transform(iceberg_transform).data().unwrap();

        ctx.execute_logical_plan(transformed)
            .await
            .unwrap()
            .collect()
            .await
            .expect("Failed to execute query plan.");

        let tables = iceberg_catalog
            .clone()
            .list_tabulars(
                &Namespace::try_new(&["tpch".to_owned()]).expect("Failed to create namespace"),
            )
            .await
            .expect("Failed to list Tables");
        assert_eq!(tables[0].to_string(), "tpch.lineitem".to_owned());

        let sql = "insert into warehouse.tpch.lineitem select * from lineitem;";

        let plan = ctx.state().create_logical_plan(sql).await.unwrap();

        let transformed = plan.transform(iceberg_transform).data().unwrap();

        ctx.execute_logical_plan(transformed)
            .await
            .unwrap()
            .collect()
            .await
            .expect("Failed to execute query plan.");

        let batches = ctx
        .sql("select sum(L_QUANTITY), L_PARTKEY from warehouse.tpch.lineitem group by L_PARTKEY;")
        .await
        .expect("Failed to create plan for select")
        .collect()
        .await
        .expect("Failed to execute select query");

        let mut once = false;

        for batch in batches {
            if batch.num_rows() != 0 {
                let (amounts, product_ids) = (
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .unwrap(),
                    batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap(),
                );
                for (product_id, amount) in product_ids.iter().zip(amounts) {
                    if product_id.unwrap() == 24027 {
                        assert_eq!(amount.unwrap(), 24.0)
                    } else if product_id.unwrap() == 63700 {
                        assert_eq!(amount.unwrap(), 23.0)
                    }
                }
                once = true
            }
        }

        assert!(once);

        let object_store = iceberg_catalog
            .default_object_store(iceberg_rust::object_store::Bucket::S3("warehouse"));

        let version_hint = object_store
            .get(&strip_prefix("s3://warehouse/tpch/lineitem/metadata/version-hint.text").into())
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();

        let cache = iceberg_catalog.cache.read().unwrap();
        let keys = cache.values().collect::<Vec<_>>();
        let version = version_hint_content(&keys[0].clone().0);

        assert_eq!(std::str::from_utf8(&version_hint).unwrap(), version);
    }
}

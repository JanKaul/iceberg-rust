use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use futures::lock::Mutex;
use iceberg_rust::{
    catalog::{
        bucket::{parse_bucket, Bucket},
        commit::{
            apply_table_updates, apply_view_updates, check_table_requirements,
            check_view_requirements, CommitTable, CommitView, TableRequirement,
        },
        identifier::Identifier,
        namespace::Namespace,
        tabular::Tabular,
        Catalog, CatalogList,
    },
    error::Error as IcebergError,
    materialized_view::MaterializedView,
    spec::{
        materialized_view_metadata::MaterializedViewMetadata,
        table_metadata::{new_metadata_location, TableMetadata},
        tabular::TabularMetadata,
        view_metadata::ViewMetadata,
    },
    table::Table,
    util::strip_prefix,
    view::View,
};
use object_store::ObjectStore;
use sqlx::{
    any::{install_default_drivers, AnyConnectOptions, AnyRow},
    AnyConnection, ConnectOptions, Connection, Row,
};
use uuid::Uuid;

use crate::error::Error;

#[derive(Debug)]
pub struct SqlCatalog {
    name: String,
    connection: Arc<Mutex<AnyConnection>>,
    object_store: Arc<dyn ObjectStore>,
    cache: Arc<DashMap<Identifier, (String, TabularMetadata)>>,
}

pub mod error;

impl SqlCatalog {
    pub async fn new(
        url: &str,
        name: &str,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Self, Error> {
        install_default_drivers();

        let mut connection =
            AnyConnectOptions::connect(&AnyConnectOptions::from_url(&url.try_into()?)?).await?;

        connection
            .transaction(|txn| {
                Box::pin(async move {
                    sqlx::query(
                        "create table if not exists iceberg_tables (
                                catalog_name varchar(255) not null,
                                table_namespace varchar(255) not null,
                                table_name varchar(255) not null,
                                metadata_location varchar(255) not null,
                                previous_metadata_location varchar(255),
                                primary key (catalog_name, table_namespace, table_name)
                            );",
                    )
                    .execute(&mut **txn)
                    .await
                })
            })
            .await?;

        connection
            .transaction(|txn| {
                Box::pin(async move {
                    sqlx::query(
                        "create table if not exists iceberg_namespace_properties (
                                catalog_name varchar(255) not null,
                                namespace varchar(255) not null,
                                property_key varchar(255),
                                property_value varchar(255),
                                primary key (catalog_name, namespace, property_key)
                            );",
                    )
                    .execute(&mut **txn)
                    .await
                })
            })
            .await?;

        Ok(SqlCatalog {
            name: name.to_owned(),
            connection: Arc::new(Mutex::new(connection)),
            object_store,
            cache: Arc::new(DashMap::new()),
        })
    }

    pub fn catalog_list(&self) -> Arc<SqlCatalogList> {
        Arc::new(SqlCatalogList {
            connection: self.connection.clone(),
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
    async fn list_tables(&self, namespace: &Namespace) -> Result<Vec<Identifier>, IcebergError> {
        let mut connection = self.connection.lock().await;
        let rows = connection.transaction(|txn|{
            let name = self.name.clone();
            let namespace = namespace.to_string();
            Box::pin(async move {
            sqlx::query(&format!("select table_namespace, table_name, metadata_location, previous_metadata_location from iceberg_tables where catalog_name = '{}' and table_namespace = '{}';",&name, &namespace)).fetch_all(&mut **txn).await
        })}).await.map_err(Error::from)?;
        let iter = rows.iter().map(query_map);

        Ok(iter
            .map(|x| {
                x.and_then(|y| {
                    Identifier::parse(&(y.table_namespace.to_string() + "." + &y.table_name))
                        .map_err(|err| sqlx::Error::Decode(Box::new(err)))
                })
            })
            .collect::<Result<_, sqlx::Error>>()
            .map_err(Error::from)?)
    }
    async fn list_namespaces(&self, _parent: Option<&str>) -> Result<Vec<Namespace>, IcebergError> {
        let mut connection = self.connection.lock().await;
        let rows = connection.transaction(|txn|{
            let name = self.name.clone();
            Box::pin(async move {
            sqlx::query(&format!("select distinct table_namespace from iceberg_tables where catalog_name = '{}';",&name)).fetch_all(&mut **txn).await
        })}).await.map_err(Error::from)?;
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
    async fn table_exists(&self, identifier: &Identifier) -> Result<bool, IcebergError> {
        let mut connection = self.connection.lock().await;
        let rows = connection.transaction(|txn|{
            let catalog_name = self.name.clone();
            let namespace = identifier.namespace().to_string();
            let name = identifier.name().to_string();
            Box::pin(async move {
            sqlx::query(&format!("select table_namespace, table_name, metadata_location, previous_metadata_location from iceberg_tables where catalog_name = '{}' and table_namespace = '{}' and table_name = '{}';",&catalog_name,
                &namespace,
                &name)).fetch_all(&mut **txn).await
        })}).await.map_err(Error::from)?;
        let mut iter = rows.iter().map(query_map);

        Ok(iter.next().is_some())
    }
    async fn drop_table(&self, identifier: &Identifier) -> Result<(), IcebergError> {
        let mut connection = self.connection.lock().await;
        connection.transaction(|txn|{
            let catalog_name = self.name.clone();
            let namespace = identifier.namespace().to_string();
            let name = identifier.name().to_string();
            Box::pin(async move {
            sqlx::query(&format!("delete from iceberg_tables where catalog_name = '{}' and table_namespace = '{}' and table_name = '{}';",&catalog_name,
                &namespace,
                &name)).execute(&mut **txn).await
        })}).await.map_err(Error::from)?;
        Ok(())
    }
    async fn load_tabular(
        self: Arc<Self>,
        identifier: &Identifier,
    ) -> Result<Tabular, IcebergError> {
        let path = {
            let mut connection = self.connection.lock().await;
            let row = connection.transaction(|txn|{
            let catalog_name = self.name.clone();
            let namespace = identifier.namespace().to_string();
            let name = identifier.name().to_string();
                Box::pin(async move {
            sqlx::query(&format!("select table_namespace, table_name, metadata_location, previous_metadata_location from iceberg_tables where catalog_name = '{}' and table_namespace = '{}' and table_name = '{}';",&catalog_name,
                    &namespace,
                    &name)).fetch_one(&mut **txn).await
        })}).await.map_err(Error::from)?;
            let row = query_map(&row).map_err(Error::from)?;

            row.metadata_location
        };
        let bytes = &self
            .object_store
            .get(&strip_prefix(&path).as_str().into())
            .await?
            .bytes()
            .await?;
        let metadata: TabularMetadata = serde_json::from_str(std::str::from_utf8(bytes)?)?;
        self.cache
            .insert(identifier.clone(), (path.clone(), metadata.clone()));
        match metadata {
            TabularMetadata::Table(metadata) => Ok(Tabular::Table(
                Table::new(identifier.clone(), self.clone(), metadata).await?,
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
        metadata: TableMetadata,
    ) -> Result<Table, IcebergError> {
        // Create metadata
        let location = metadata.location.to_string();

        // Write metadata to object_store
        let bucket = parse_bucket(&location)?;
        let object_store = self.object_store(bucket);

        let uuid = Uuid::new_v4();
        let version = &metadata.last_sequence_number;
        let metadata_json = serde_json::to_string(&metadata)?;
        let metadata_location = location
            + "/metadata/"
            + &version.to_string()
            + "-"
            + &uuid.to_string()
            + ".metadata.json";
        object_store
            .put(
                &strip_prefix(&metadata_location).into(),
                metadata_json.into(),
            )
            .await?;
        {
            let mut connection = self.connection.lock().await;
            connection.transaction(|txn|{
                let catalog_name = self.name.clone();
                let namespace = identifier.namespace().to_string();
                let name = identifier.name().to_string();
                let metadata_location = metadata_location.to_string();
                Box::pin(async move {
            sqlx::query(&format!("insert into iceberg_tables (catalog_name, table_namespace, table_name, metadata_location) values ('{}', '{}', '{}', '{}');",catalog_name,namespace,name, metadata_location)).execute(&mut **txn).await
        })}).await.map_err(Error::from)?;
        }
        self.clone()
            .load_tabular(&identifier)
            .await
            .and_then(|x| match x {
                Tabular::Table(table) => Ok(table),
                _ => Err(IcebergError::InvalidFormat(
                    "Table update on an entity that is nor a table".to_owned(),
                )),
            })
    }

    async fn create_view(
        self: Arc<Self>,
        identifier: Identifier,
        metadata: ViewMetadata,
    ) -> Result<View, IcebergError> {
        // Create metadata
        let location = metadata.location.to_string();

        // Write metadata to object_store
        let bucket = parse_bucket(&location)?;
        let object_store = self.object_store(bucket);

        let uuid = Uuid::new_v4();
        let version = &metadata.current_version_id;
        let metadata_json = serde_json::to_string(&metadata)?;
        let metadata_location = location
            + "/metadata/"
            + &version.to_string()
            + "-"
            + &uuid.to_string()
            + ".metadata.json";
        object_store
            .put(
                &strip_prefix(&metadata_location).into(),
                metadata_json.into(),
            )
            .await?;
        {
            let mut connection = self.connection.lock().await;
            connection.transaction(|txn|{
                let catalog_name = self.name.clone();
                let namespace = identifier.namespace().to_string();
                let name = identifier.name().to_string();
                let metadata_location = metadata_location.to_string();
                Box::pin(async move {
            sqlx::query(&format!("insert into iceberg_tables (catalog_name, table_namespace, table_name, metadata_location) values ('{}', '{}', '{}', '{}');",catalog_name,namespace,name, metadata_location)).execute(&mut **txn).await
        })}).await.map_err(Error::from)?;
        }
        self.clone()
            .load_tabular(&identifier)
            .await
            .and_then(|x| match x {
                Tabular::View(view) => Ok(view),
                _ => Err(IcebergError::InvalidFormat(
                    "Table update on an entity that is nor a table".to_owned(),
                )),
            })
    }

    async fn create_materialized_view(
        self: Arc<Self>,
        identifier: Identifier,
        metadata: MaterializedViewMetadata,
    ) -> Result<MaterializedView, IcebergError> {
        // Create metadata
        let location = metadata.location.to_string();

        // Write metadata to object_store
        let bucket = parse_bucket(&location)?;
        let object_store = self.object_store(bucket);

        let uuid = Uuid::new_v4();
        let version = &metadata.current_version_id;
        let metadata_json = serde_json::to_string(&metadata)?;
        let metadata_location = location
            + "/metadata/"
            + &version.to_string()
            + "-"
            + &uuid.to_string()
            + ".metadata.json";
        object_store
            .put(
                &strip_prefix(&metadata_location).into(),
                metadata_json.into(),
            )
            .await?;
        {
            let mut connection = self.connection.lock().await;
            connection.transaction(|txn|{
                let catalog_name = self.name.clone();
                let namespace = identifier.namespace().to_string();
                let name = identifier.name().to_string();
                let metadata_location = metadata_location.to_string();
                Box::pin(async move {
            sqlx::query(&format!("insert into iceberg_tables (catalog_name, table_namespace, table_name, metadata_location) values ('{}', '{}', '{}', '{}');",catalog_name,namespace,name, metadata_location)).execute(&mut **txn).await
        })}).await.map_err(Error::from)?;
        }
        self.clone()
            .load_tabular(&identifier)
            .await
            .and_then(|x| match x {
                Tabular::MaterializedView(matview) => Ok(matview),
                _ => Err(IcebergError::InvalidFormat(
                    "Table update on an entity that is nor a table".to_owned(),
                )),
            })
    }

    async fn update_table(self: Arc<Self>, commit: CommitTable) -> Result<Table, IcebergError> {
        let identifier = commit.identifier;
        match self.cache.get(&identifier) {
            None => {
                if !matches!(commit.requirements[0], TableRequirement::AssertCreate) {
                    return Err(IcebergError::InvalidFormat(
                        "Create table assertion".to_owned(),
                    ));
                } else {
                    return Err(IcebergError::InvalidFormat(
                        "Create table assertion".to_owned(),
                    ));
                }
            }
            Some(entry) => {
                let (previous_metadata_location, metadata) = entry.value();

                let TabularMetadata::Table(mut metadata) = metadata.clone() else {
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
                let metadata_location = new_metadata_location((&metadata).into());
                self.object_store
                    .put(
                        &strip_prefix(&metadata_location).into(),
                        serde_json::to_string(&metadata)?.into(),
                    )
                    .await?;
                let mut connection = self.connection.lock().await;
                connection.transaction(|txn|{
                let catalog_name = self.name.clone();
                let namespace = identifier.namespace().to_string();
                let name = identifier.name().to_string();
                let metadata_file_location = metadata_location.to_string();
                let previous_metadata_file_location = previous_metadata_location.to_string();
                Box::pin(async move {
            sqlx::query(&format!("update iceberg_tables set metadata_location = '{}', previous_metadata_location = '{}' where catalog_name = '{}' and table_namespace = '{}' and table_name = '{}';", metadata_file_location, previous_metadata_file_location,catalog_name,namespace,name)).execute(&mut **txn).await
        })}).await.map_err(Error::from)?;
            }
        }
        self.clone()
            .load_tabular(&identifier)
            .await
            .and_then(|x| match x {
                Tabular::Table(table) => Ok(table),
                _ => Err(IcebergError::InvalidFormat(
                    "Table update on an entity that is nor a table".to_owned(),
                )),
            })
    }

    async fn update_view(self: Arc<Self>, commit: CommitView) -> Result<View, IcebergError> {
        let identifier = commit.identifier;
        match self.cache.get(&identifier) {
            None => {
                return Err(IcebergError::InvalidFormat(
                    "Create table assertion".to_owned(),
                ))
            }
            Some(entry) => {
                let (previous_metadata_location, metadata) = entry.value();
                let mut metadata = metadata.clone();
                let metadata_location = match &mut metadata {
                    TabularMetadata::View(metadata) => {
                        if !check_view_requirements(&commit.requirements, metadata) {
                            return Err(IcebergError::InvalidFormat(
                                "View requirements not valid".to_owned(),
                            ));
                        }
                        apply_view_updates(metadata, commit.updates)?;
                        let metadata_location = metadata.location.to_string()
                            + "/metadata/"
                            + &metadata.current_version_id.to_string()
                            + "-"
                            + &Uuid::new_v4().to_string()
                            + ".metadata.json";
                        self.object_store
                            .put(
                                &strip_prefix(&metadata_location).into(),
                                serde_json::to_string(&metadata)?.into(),
                            )
                            .await?;
                        Ok(metadata_location)
                    }
                    TabularMetadata::MaterializedView(metadata) => {
                        if !check_view_requirements(&commit.requirements, metadata) {
                            return Err(IcebergError::InvalidFormat(
                                "Materialized view requirements not valid".to_owned(),
                            ));
                        }
                        apply_view_updates(metadata, commit.updates)?;
                        let metadata_location = metadata.location.to_string()
                            + "/metadata/"
                            + &metadata.current_version_id.to_string()
                            + "-"
                            + &Uuid::new_v4().to_string()
                            + ".metadata.json";
                        self.object_store
                            .put(
                                &strip_prefix(&metadata_location).into(),
                                serde_json::to_string(&metadata)?.into(),
                            )
                            .await?;
                        Ok(metadata_location)
                    }
                    _ => Err(IcebergError::InvalidFormat(
                        "Table update on entity that is not a table".to_owned(),
                    )),
                }?;

                let mut connection = self.connection.lock().await;
                connection.transaction(|txn|{
                let catalog_name = self.name.clone();
                let namespace = identifier.namespace().to_string();
                let name = identifier.name().to_string();
                let metadata_file_location = metadata_location.to_string();
                let previous_metadata_file_location = previous_metadata_location.to_string();
                Box::pin(async move {
            sqlx::query(&format!("update iceberg_tables set metadata_location = '{}', previous_metadata_location = '{}' where catalog_name = '{}' and table_namespace = '{}' and table_name = '{}';", metadata_file_location, previous_metadata_file_location,catalog_name,namespace,name)).execute(&mut **txn).await
        })}).await.map_err(Error::from)?;
            }
        }
        if let Tabular::View(view) = self.clone().load_tabular(&identifier).await? {
            Ok(view)
        } else {
            Err(IcebergError::InvalidFormat(
                "Entity is not a view".to_owned(),
            ))
        }
    }
    async fn update_materialized_view(
        self: Arc<Self>,
        commit: CommitView,
    ) -> Result<MaterializedView, IcebergError> {
        let identifier = commit.identifier;
        match self.cache.get(&identifier) {
            None => {
                return Err(IcebergError::InvalidFormat(
                    "Create table assertion".to_owned(),
                ))
            }
            Some(entry) => {
                let (previous_metadata_location, metadata) = entry.value();
                let mut metadata = metadata.clone();
                let metadata_location = match &mut metadata {
                    TabularMetadata::View(metadata) => {
                        if !check_view_requirements(&commit.requirements, metadata) {
                            return Err(IcebergError::InvalidFormat(
                                "View requirements not valid".to_owned(),
                            ));
                        }
                        apply_view_updates(metadata, commit.updates)?;
                        let metadata_location = metadata.location.to_string()
                            + "/metadata/"
                            + &metadata.current_version_id.to_string()
                            + "-"
                            + &Uuid::new_v4().to_string()
                            + ".metadata.json";
                        self.object_store
                            .put(
                                &strip_prefix(&metadata_location).into(),
                                serde_json::to_string(&metadata)?.into(),
                            )
                            .await?;
                        Ok(metadata_location)
                    }
                    TabularMetadata::MaterializedView(metadata) => {
                        if !check_view_requirements(&commit.requirements, metadata) {
                            return Err(IcebergError::InvalidFormat(
                                "Materialized view requirements not valid".to_owned(),
                            ));
                        }
                        apply_view_updates(metadata, commit.updates)?;
                        let metadata_location = metadata.location.to_string()
                            + "/metadata/"
                            + &metadata.current_version_id.to_string()
                            + "-"
                            + &Uuid::new_v4().to_string()
                            + ".metadata.json";
                        self.object_store
                            .put(
                                &strip_prefix(&metadata_location).into(),
                                serde_json::to_string(&metadata)?.into(),
                            )
                            .await?;
                        Ok(metadata_location)
                    }
                    _ => Err(IcebergError::InvalidFormat(
                        "Table update on entity that is not a table".to_owned(),
                    )),
                }?;

                let mut connection = self.connection.lock().await;
                connection.transaction(|txn|{
                let catalog_name = self.name.clone();
                let namespace = identifier.namespace().to_string();
                let name = identifier.name().to_string();
                let metadata_file_location = metadata_location.to_string();
                let previous_metadata_file_location = previous_metadata_location.to_string();
                Box::pin(async move {
            sqlx::query(&format!("update iceberg_tables set metadata_location = '{}', previous_metadata_location = '{}' where catalog_name = '{}' and table_namespace = '{}' and table_name = '{}';", metadata_file_location, previous_metadata_file_location,catalog_name,namespace,name)).execute(&mut **txn).await
        })}).await.map_err(Error::from)?;
            }
        }
        if let Tabular::MaterializedView(matview) = self.clone().load_tabular(&identifier).await? {
            Ok(matview)
        } else {
            Err(IcebergError::InvalidFormat(
                "Entity is not a materialized view".to_owned(),
            ))
        }
    }

    fn object_store(&self, _: Bucket) -> Arc<dyn object_store::ObjectStore> {
        self.object_store.clone()
    }
}

impl SqlCatalog {
    pub fn duplicate(&self, name: &str) -> Self {
        Self {
            name: name.to_owned(),
            connection: self.connection.clone(),
            object_store: self.object_store.clone(),
            cache: Arc::new(DashMap::new()),
        }
    }
}

#[derive(Debug)]
pub struct SqlCatalogList {
    connection: Arc<Mutex<AnyConnection>>,
    object_store: Arc<dyn ObjectStore>,
}

impl SqlCatalogList {
    pub async fn new(url: &str, object_store: Arc<dyn ObjectStore>) -> Result<Self, Error> {
        install_default_drivers();

        let mut connection =
            AnyConnectOptions::connect(&AnyConnectOptions::from_url(&url.try_into()?)?).await?;

        connection
            .transaction(|txn| {
                Box::pin(async move {
                    sqlx::query(
                        "create table if not exists iceberg_tables (
                                catalog_name varchar(255) not null,
                                table_namespace varchar(255) not null,
                                table_name varchar(255) not null,
                                metadata_location varchar(255) not null,
                                previous_metadata_location varchar(255),
                                primary key (catalog_name, table_namespace, table_name)
                            );",
                    )
                    .execute(&mut **txn)
                    .await
                })
            })
            .await?;

        Ok(SqlCatalogList {
            connection: Arc::new(Mutex::new(connection)),
            object_store,
        })
    }
}

#[async_trait]
impl CatalogList for SqlCatalogList {
    async fn catalog(&self, name: &str) -> Option<Arc<dyn Catalog>> {
        Some(Arc::new(SqlCatalog {
            name: name.to_owned(),
            connection: self.connection.clone(),
            object_store: self.object_store.clone(),
            cache: Arc::new(DashMap::new()),
        }))
    }
    async fn list_catalogs(&self) -> Vec<String> {
        let mut connection = self.connection.lock().await;
        let rows = connection
            .transaction(|txn| {
                Box::pin(async move {
                    sqlx::query("select distinct catalog_name from iceberg_tables;")
                        .fetch_all(&mut **txn)
                        .await
                })
            })
            .await
            .map_err(Error::from)
            .unwrap_or_default();
        let iter = rows.iter().map(|row| row.try_get::<String, _>(0));

        iter.collect::<Result<_, sqlx::Error>>()
            .map_err(Error::from)
            .unwrap_or_default()
    }
}

#[cfg(test)]
pub mod tests {
    use iceberg_rust::{
        catalog::{identifier::Identifier, namespace::Namespace, Catalog},
        spec::{
            schema::Schema,
            types::{PrimitiveType, StructField, StructType, Type},
        },
        table::table_builder::TableBuilder,
    };
    use object_store::{memory::InMemory, ObjectStore};
    use std::sync::Arc;

    use crate::SqlCatalog;

    #[tokio::test]
    async fn test_create_update_drop_table() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let catalog: Arc<dyn Catalog> = Arc::new(
            SqlCatalog::new("sqlite://", "test", object_store)
                .await
                .unwrap(),
        );
        let identifier = Identifier::parse("load_table.table3").unwrap();
        let schema = Schema::builder()
            .with_schema_id(1)
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

        let mut builder = TableBuilder::new(&identifier, catalog.clone())
            .expect("Failed to create table builder.");
        builder
            .location("/")
            .with_schema((1, schema))
            .current_schema_id(1);
        let mut table = builder.build().await.expect("Failed to create table.");

        let exists = Arc::clone(&catalog)
            .table_exists(&identifier)
            .await
            .expect("Table doesn't exist");
        assert!(exists);

        let tables = catalog
            .clone()
            .list_tables(
                &Namespace::try_new(&["load_table".to_owned()])
                    .expect("Failed to create namespace"),
            )
            .await
            .expect("Failed to list Tables");
        assert_eq!(tables[0].to_string(), "load_table.table3".to_owned());

        let namespaces = catalog
            .clone()
            .list_namespaces(None)
            .await
            .expect("Failed to list namespaces");
        assert_eq!(namespaces[0].to_string(), "load_table");

        let transaction = table.new_transaction(None);
        transaction.commit().await.expect("Transaction failed.");

        catalog
            .drop_table(&identifier)
            .await
            .expect("Failed to drop table.");

        let exists = Arc::clone(&catalog)
            .table_exists(&identifier)
            .await
            .expect("Table exists failed");
        assert!(!exists);
    }
}

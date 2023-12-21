use std::sync::Arc;

use async_trait::async_trait;
use futures::lock::Mutex;
use iceberg_rust::{
    catalog::{
        identifier::Identifier,
        namespace::Namespace,
        tabular::{Tabular, TabularMetadata},
        Catalog, CatalogList, bucket::Bucket,
    },
    error::Error as IcebergError,
    materialized_view::MaterializedView,
    table::Table,
    util::strip_prefix,
    view::View,
};
use object_store::ObjectStore;
use sqlx::{
    any::{AnyConnectOptions, AnyRow, install_default_drivers},
    AnyConnection, ConnectOptions, Connection, Row,
};

use crate::error::Error;

#[derive(Debug)]
pub struct SqlCatalog {
    name: String,
    connection: Arc<Mutex<AnyConnection>>,
    object_store: Arc<dyn ObjectStore>,
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
                                catalog_name text not null,
                                table_namespace textl not null,
                                table_name text not null,
                                metadata_location text not null,
                                previous_metadata_location text,
                                primary key (catalog_name, table_namespace, table_name)
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
        })
    }
    pub fn catalog_list(&self) -> Arc<SqlCatalogList> {
    Arc::new(SqlCatalogList { connection: self.connection.clone(), object_store: self.object_store.clone() })
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
        _previous_metadata_location: row.try_get::<String,_>(3).map(Some).or_else(|err|if let sqlx::Error::ColumnDecode { index: _, source: _ } = err {Ok(None)} else {Err(err)})?,
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
    async fn load_table(self: Arc<Self>, identifier: &Identifier) -> Result<Tabular, IcebergError> {
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
        match metadata {
            TabularMetadata::Table(metadata) => Ok(Tabular::Table(
                Table::new(
                    identifier.clone(),
                    self.clone(),
                    metadata,
                    &path.to_string(),
                )
                .await?,
            )),
            TabularMetadata::View(metadata) => Ok(Tabular::View(
                View::new(
                    identifier.clone(),
                    self.clone(),
                    metadata,
                    &path.to_string(),
                )
                .await?,
            )),
            TabularMetadata::MaterializedView(metadata) => Ok(Tabular::MaterializedView(
                MaterializedView::new(
                    identifier.clone(),
                    self.clone(),
                    metadata,
                    &path.to_string(),
                )
                .await?,
            )),
        }
    }
    async fn invalidate_table(&self, _identifier: &Identifier) -> Result<(), IcebergError> {
        unimplemented!()
    }
    async fn register_table(
        self: Arc<Self>,
        identifier: Identifier,
        metadata_file_location: &str,
    ) -> Result<Tabular, IcebergError> {
        {
            let mut connection = self.connection.lock().await;
            connection.transaction(|txn|{
                let catalog_name = self.name.clone();
                let namespace = identifier.namespace().to_string();
                let name = identifier.name().to_string();
                let metadata_file_location = metadata_file_location.to_string();
                Box::pin(async move {
            sqlx::query(&format!("insert into iceberg_tables (catalog_name, table_namespace, table_name, metadata_location) values ('{}', '{}', '{}', '{}');",catalog_name,namespace.to_string(),name.to_string(), metadata_file_location)).execute(&mut **txn).await
        })}).await.map_err(Error::from)?;
        }
        self.load_table(&identifier).await
    }
    async fn update_table(
        self: Arc<Self>,
        identifier: Identifier,
        metadata_file_location: &str,
        previous_metadata_file_location: &str,
    ) -> Result<Tabular, IcebergError> {
        {
            let mut connection = self.connection.lock().await;
            connection.transaction(|txn|{
                let catalog_name = self.name.clone();
                let namespace = identifier.namespace().to_string();
                let name = identifier.name().to_string();
                let metadata_file_location = metadata_file_location.to_string();
                let previous_metadata_file_location = previous_metadata_file_location.to_string();
                Box::pin(async move {
            sqlx::query(&format!("update iceberg_tables set metadata_location = '{}', previous_metadata_location = '{}' where catalog_name = '{}' and table_namespace = '{}' and table_name = '{}';", metadata_file_location, previous_metadata_file_location,catalog_name,namespace,name)).execute(&mut **txn).await
        })}).await.map_err(Error::from)?;
        }
        self.load_table(&identifier).await
    }
    async fn initialize(
        self: Arc<Self>,
        _properties: &std::collections::HashMap<String, String>,
    ) -> Result<(), IcebergError> {
        unimplemented!()
    }
    fn object_store(&self, _: Bucket) -> Arc<dyn object_store::ObjectStore> {
        self.object_store.clone()
    }
}

impl SqlCatalog {
    pub fn duplicate(&self, name: &str) -> Self {
        Self { name: name.to_owned(), connection: self.connection.clone(), object_store: self.object_store.clone() }
    }
}

#[derive(Debug)]
pub struct SqlCatalogList {
    connection: Arc<Mutex<AnyConnection>>,
    object_store: Arc<dyn ObjectStore>,
}

impl SqlCatalogList {
    pub async fn new(
        url: &str,
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
                                catalog_name text not null,
                                table_namespace text not null,
                                table_name text not null,
                                metadata_location text not null,
                                previous_metadata_location text,
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
    async fn catalog(&self, name: &str) -> Option<Arc<dyn Catalog>>{
        Some(Arc::new(SqlCatalog {name: name.to_owned(),connection: self.connection.clone(), object_store: self.object_store.clone()}))
    }
    async fn list_catalogs(&self) -> Vec<String>{
        let mut connection = self.connection.lock().await;
        let rows = connection.transaction(|txn|{
            Box::pin(async move {
            sqlx::query("select distinct catalog_name from iceberg_tables;").fetch_all(&mut **txn).await
        })}).await.map_err(Error::from).unwrap_or_default();
        let iter = rows.iter().map(|row| row.try_get::<String, _>(0));

        iter
            .collect::<Result<_, sqlx::Error>>()
            .map_err(Error::from).unwrap_or_default()
    }
}

#[cfg(test)]
pub mod tests {
    use iceberg_rust::{spec::{
        schema::Schema,
        types::{PrimitiveType, StructField, StructType, Type},
    }, catalog::{Catalog, identifier::Identifier}, table::table_builder::TableBuilder};
    use object_store::{memory::InMemory, ObjectStore};
    use std::sync::Arc;

    use crate::SqlCatalog;

    #[tokio::test]
    async fn test_create_update_drop_table() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let catalog: Arc<dyn Catalog> = Arc::new(SqlCatalog::new("sqlite://","test", object_store).await.unwrap());
        let identifier = Identifier::parse("load_table.table3").unwrap();
        let schema = Schema {
            schema_id: 1,
            identifier_field_ids: Some(vec![1, 2]),
            fields: StructType::new(vec![
                StructField {
                    id: 1,
                    name: "one".to_string(),
                    required: false,
                    field_type: Type::Primitive(PrimitiveType::String),
                    doc: None,
                },
                StructField {
                    id: 2,
                    name: "two".to_string(),
                    required: false,
                    field_type: Type::Primitive(PrimitiveType::String),
                    doc: None,
                },
            ]),
        };
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

        let metadata_location = table.metadata_location().to_string();

        let transaction = table.new_transaction(None);
        transaction.commit().await.expect("Transaction failed.");

        let new_metadata_location = table.metadata_location().to_string();

        assert_ne!(metadata_location, new_metadata_location);

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

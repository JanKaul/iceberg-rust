/*!
Defining an in memory catalog struct.
*/

use std::sync::Arc;

use async_trait::async_trait;
use futures::lock::Mutex;
use object_store::ObjectStore;
use rusqlite::{Connection, Row};

use crate::{
    error::Error, materialized_view::MaterializedView, table::Table, util::strip_prefix, view::View,
};

use super::{
    identifier::Identifier,
    namespace::Namespace,
    relation::{RelationMetadata, Tabular},
    Catalog,
};

#[derive(Debug)]
/// In memory catalog
pub struct MemoryCatalog {
    name: String,
    connection: Arc<Mutex<Connection>>,
    object_store: Arc<dyn ObjectStore>,
}

impl MemoryCatalog {
    /// create new in memory catalog
    pub fn new(name: &str, object_store: Arc<dyn ObjectStore>) -> Result<Self, Error> {
        let connection = Connection::open_in_memory()?;
        connection.execute(
            "create table iceberg_tables (
                catalog_name varchar(255) not null,
                table_namespace varchar(255) not null,
                table_name varchar(255) not null,
                metadata_location varchar(255) not null,
                previous_metadata_location varchar(255),
                primary key (catalog_name, table_namespace, table_name)
            );",
            (),
        )?;
        Ok(MemoryCatalog {
            name: name.to_owned(),
            connection: Arc::new(Mutex::new(connection)),
            object_store,
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

fn query_map(row: &Row<'_>) -> Result<TableRef, rusqlite::Error> {
    Ok(TableRef {
        table_namespace: row.get(0)?,
        table_name: row.get(1)?,
        metadata_location: row.get(2)?,
        _previous_metadata_location: row.get(3)?,
    })
}

#[async_trait]
impl Catalog for MemoryCatalog {
    async fn list_tables(
        &self,
        namespace: &super::namespace::Namespace,
    ) -> Result<Vec<super::identifier::Identifier>, Error> {
        let connection = self.connection.lock().await;
        let mut stmt = connection.prepare("select table_namespace, table_name, metadata_location, previous_metadata_location from iceberg_tables where catalog_name = ?1 and table_namespace = ?2")?;
        let iter = stmt.query_map([&self.name, &namespace.to_string()], query_map)?;

        iter.map(|x| {
            x.and_then(|y| {
                Identifier::parse(&(y.table_namespace.to_string() + "." + &y.table_name))
                    .map_err(|_| rusqlite::Error::InvalidQuery)
            })
        })
        .collect::<Result<_, rusqlite::Error>>()
        .map_err(Into::into)
    }
    async fn list_namespaces(
        &self,
        _parent: Option<&str>,
    ) -> Result<Vec<super::namespace::Namespace>, Error> {
        let connection = self.connection.lock().await;
        let mut stmt = connection.prepare(
            "select distinct table_namespace from iceberg_tables where catalog_name = ?1",
        )?;
        let iter = stmt.query_map([&self.name], |row| row.get::<_, String>(0))?;

        iter.map(|x| {
            x.and_then(|y| {
                Namespace::try_new(&y.split(".").map(ToString::to_string).collect::<Vec<_>>())
                    .map_err(|_| rusqlite::Error::InvalidQuery)
            })
        })
        .collect::<Result<_, rusqlite::Error>>()
        .map_err(Into::into)
    }
    async fn table_exists(
        &self,
        identifier: &super::identifier::Identifier,
    ) -> Result<bool, Error> {
        let connection = self.connection.lock().await;
        let mut stmt = connection.prepare("select table_namespace, table_name, metadata_location, previous_metadata_location from iceberg_tables where catalog_name = ?1 and table_namespace = ?2 and table_name = ?3")?;
        let mut iter = stmt.query_map(
            [
                &self.name,
                &identifier.namespace().to_string(),
                &identifier.name().to_string(),
            ],
            query_map,
        )?;

        Ok(iter.next().is_some())
    }
    async fn drop_table(&self, identifier: &super::identifier::Identifier) -> Result<(), Error> {
        let connection = self.connection.lock().await;
        connection.execute("delete from iceberg_tables where catalog_name = ?1 and table_namespace = ?2 and table_name = ?3", (self.name.clone(),identifier.namespace().to_string(),identifier.name().to_string()))?;
        Ok(())
    }
    async fn load_table(
        self: Arc<Self>,
        identifier: &super::identifier::Identifier,
    ) -> Result<super::relation::Tabular, Error> {
        let path = {
            let connection = self.connection.lock().await;
            let mut stmt = connection.prepare("select table_namespace, table_name, metadata_location, previous_metadata_location from iceberg_tables where catalog_name = ?1 and table_namespace = ?2 and table_name = ?3")?;
            let mut iter = stmt.query_map(
                [
                    &self.name,
                    &identifier.namespace().to_string(),
                    &identifier.name().to_string(),
                ],
                query_map,
            )?;

            iter.next()
                .ok_or(Error::NotFound(
                    "Table".to_string(),
                    format!("{}", identifier),
                ))??
                .metadata_location
        };
        let bytes = &self
            .object_store
            .get(&strip_prefix(&path).as_str().into())
            .await?
            .bytes()
            .await?;
        let metadata: RelationMetadata = serde_json::from_str(std::str::from_utf8(bytes)?)?;
        let catalog: Arc<dyn Catalog> = self;
        match metadata {
            RelationMetadata::Table(metadata) => Ok(Tabular::Table(
                Table::new(
                    identifier.clone(),
                    Arc::clone(&catalog),
                    metadata,
                    &path.to_string(),
                )
                .await?,
            )),
            RelationMetadata::View(metadata) => Ok(Tabular::View(
                View::new(
                    identifier.clone(),
                    Arc::clone(&catalog),
                    metadata,
                    &path.to_string(),
                )
                .await?,
            )),
            RelationMetadata::MaterializedView(metadata) => Ok(Tabular::MaterializedView(
                MaterializedView::new(
                    identifier.clone(),
                    catalog.clone(),
                    metadata,
                    &path.to_string(),
                )
                .await?,
            )),
        }
    }
    async fn invalidate_table(
        &self,
        _identifier: &super::identifier::Identifier,
    ) -> Result<(), Error> {
        unimplemented!()
    }
    async fn register_table(
        self: Arc<Self>,
        identifier: super::identifier::Identifier,
        metadata_file_location: &str,
    ) -> Result<super::relation::Tabular, Error> {
        {
            let connection = self.connection.lock().await;
            connection.execute("insert into iceberg_tables (catalog_name, table_namespace, table_name, metadata_location) values (?1, ?2, ?3, ?4)", (self.name.clone(),identifier.namespace().to_string(),identifier.name().to_string(), metadata_file_location.to_string()))?;
        }
        self.load_table(&identifier).await
    }
    async fn update_table(
        self: Arc<Self>,
        identifier: super::identifier::Identifier,
        metadata_file_location: &str,
        previous_metadata_file_location: &str,
    ) -> Result<super::relation::Tabular, Error> {
        {
            let connection = self.connection.lock().await;
            connection.execute("update iceberg_tables set metadata_location = ?4, previous_metadata_location = ?5 where catalog_name = ?1 and table_namespace = ?2 and table_name = ?3", (self.name.clone(),identifier.namespace().to_string(),identifier.name().to_string(), metadata_file_location, previous_metadata_file_location))?;
        }
        self.load_table(&identifier).await
    }
    async fn initialize(
        self: Arc<Self>,
        _properties: &std::collections::HashMap<String, String>,
    ) -> Result<(), Error> {
        unimplemented!()
    }
    fn object_store(&self) -> Arc<dyn object_store::ObjectStore> {
        self.object_store.clone()
    }
}

#[cfg(test)]
pub mod tests {
    use std::sync::Arc;

    use crate::{
        catalog::{identifier::Identifier, memory::MemoryCatalog, Catalog},
        object_store::{memory::InMemory, ObjectStore},
        spec::{
            schema::Schema,
            types::{PrimitiveType, StructField, StructType, Type},
        },
        table::table_builder::TableBuilder,
    };

    #[tokio::test]
    async fn test_create_update_drop_table() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let catalog: Arc<dyn Catalog> = Arc::new(MemoryCatalog::new("test", object_store).unwrap());
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

        let transaction = table.new_transaction();
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

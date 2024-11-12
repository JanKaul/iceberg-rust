use std::{
    collections::HashMap,
    convert::identity,
    sync::{Arc, RwLock},
};

use async_trait::async_trait;
use futures::{future, TryStreamExt};
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

use crate::error::Error;

#[derive(Debug)]
pub struct FileCatalog {
    name: String,
    path: String,
    object_store: Arc<dyn ObjectStore>,
    cache: Arc<RwLock<HashMap<Identifier, (String, TabularMetadata)>>>,
}

pub mod error;

impl FileCatalog {
    pub async fn new(
        path: &str,
        name: &str,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Self, Error> {
        Ok(FileCatalog {
            name: name.to_owned(),
            path: path.to_owned(),
            object_store,
            cache: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    pub fn catalog_list(&self) -> Arc<FileCatalogList> {
        Arc::new(FileCatalogList {
            path: self.path.clone(),
            object_store: self.object_store.clone(),
        })
    }
}

#[async_trait]
impl Catalog for FileCatalog {
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
        self.object_store
            .list(Some(
                &strip_prefix(&self.namespace_path(&namespace[0])).into(),
            ))
            .map_err(IcebergError::from)
            .map_ok(|x| {
                let path = x.location.as_ref();
                self.identifier(path)
            })
            .try_collect()
            .await
    }
    async fn list_namespaces(&self, _parent: Option<&str>) -> Result<Vec<Namespace>, IcebergError> {
        self.object_store
            .list_with_delimiter(Some(
                &strip_prefix(&(self.path.trim_start_matches('/').to_owned() + "/" + &self.name))
                    .into(),
            ))
            .await
            .map_err(IcebergError::from)?
            .common_prefixes
            .into_iter()
            .map(|x| self.namespace(x.as_ref()))
            .collect::<Result<_, IcebergError>>()
            .map_err(IcebergError::from)
    }
    async fn tabular_exists(&self, identifier: &Identifier) -> Result<bool, IcebergError> {
        self.metadata_location(identifier).await.map(|_| true)
    }
    async fn drop_table(&self, _identifierr: &Identifier) -> Result<(), IcebergError> {
        todo!()
    }
    async fn drop_view(&self, _identifier: &Identifier) -> Result<(), IcebergError> {
        todo!()
    }
    async fn drop_materialized_view(&self, _identifier: &Identifier) -> Result<(), IcebergError> {
        todo!()
    }
    async fn load_tabular(
        self: Arc<Self>,
        identifier: &Identifier,
    ) -> Result<Tabular, IcebergError> {
        let metadata_location = self.metadata_location(identifier).await?;

        let bytes = &self
            .object_store
            .get(&strip_prefix(&metadata_location).as_str().into())
            .await
            .map_err(|_| IcebergError::CatalogNotFound)?
            .bytes()
            .await?;

        let metadata: TabularMetadata = serde_json::from_slice(bytes)?;

        self.cache.write().unwrap().insert(
            identifier.clone(),
            (metadata_location.clone(), metadata.clone()),
        );

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
        mut create_table: CreateTable,
    ) -> Result<Table, IcebergError> {
        if self.tabular_exists(&identifier).await.is_ok_and(identity) {
            return Err(IcebergError::InvalidFormat(
                "Table already exists. Path".to_owned(),
            ));
        }
        create_table.location =
            Some(self.tabular_path(&identifier.namespace()[0], identifier.name()));
        let metadata: TableMetadata = create_table.try_into()?;
        // Create metadata
        let location = metadata.location.to_string();

        // Write metadata to object_store
        let bucket = Bucket::from_path(&location)?;
        let object_store = self.object_store(bucket);

        let metadata_json = serde_json::to_string(&metadata)?;
        let metadata_location = location + "/metadata/v0.metadata.json";

        object_store
            .put(
                &strip_prefix(&metadata_location).into(),
                metadata_json.into(),
            )
            .await?;

        self.cache.write().unwrap().insert(
            identifier.clone(),
            (metadata_location.clone(), metadata.clone().into()),
        );
        Ok(Table::new(identifier.clone(), self.clone(), metadata).await?)
    }

    async fn create_view(
        self: Arc<Self>,
        identifier: Identifier,
        mut create_view: CreateView<Option<()>>,
    ) -> Result<View, IcebergError> {
        if self.tabular_exists(&identifier).await.is_ok_and(identity) {
            return Err(IcebergError::InvalidFormat(
                "View already exists. Path".to_owned(),
            ));
        }

        create_view.location =
            Some(self.tabular_path(&identifier.namespace()[0], identifier.name()));
        let metadata: ViewMetadata = create_view.try_into()?;
        // Create metadata
        let location = metadata.location.to_string();

        // Write metadata to object_store
        let bucket = Bucket::from_path(&location)?;
        let object_store = self.object_store(bucket);

        let metadata_json = serde_json::to_string(&metadata)?;
        let metadata_location = location + "/metadata/v0.metadata.json";

        object_store
            .put(
                &strip_prefix(&metadata_location).into(),
                metadata_json.into(),
            )
            .await?;

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
        if self.tabular_exists(&identifier).await.is_ok_and(identity) {
            return Err(IcebergError::InvalidFormat(
                "View already exists. Path".to_owned(),
            ));
        }

        let (mut create_view, mut create_table) = create_view.into();

        create_view.location =
            Some(self.tabular_path(&identifier.namespace()[0], identifier.name()));
        let metadata: MaterializedViewMetadata = create_view.try_into()?;
        let table_identifier = metadata.current_version(None)?.storage_table();

        create_table.location =
            Some(self.tabular_path(&table_identifier.namespace()[0], table_identifier.name()));
        let table_metadata: TableMetadata = create_table.try_into()?;
        // Create metadata
        let location = metadata.location.to_string();

        // Write metadata to object_store
        let bucket = Bucket::from_path(&location)?;
        let object_store = self.object_store(bucket);

        let metadata_json = serde_json::to_string(&metadata)?;
        let metadata_location = location + "/metadata/v0.metadata.json";

        let table_metadata_json = serde_json::to_string(&table_metadata)?;
        let table_metadata_location =
            table_metadata.location.clone() + "/metadata/v0.metadata.json";

        object_store
            .put(
                &strip_prefix(&metadata_location).into(),
                metadata_json.into(),
            )
            .await?;

        object_store
            .put(
                &strip_prefix(&table_metadata_location).into(),
                table_metadata_json.into(),
            )
            .await?;

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
        let temp_metadata_location = new_metadata_location(&metadata);

        self.object_store
            .put(
                &strip_prefix(&temp_metadata_location).into(),
                serde_json::to_string(&metadata)?.into(),
            )
            .await?;

        let current_version = parse_version(&previous_metadata_location)? + 1;
        let metadata_location = metadata.location.clone()
            + "/metadata/v"
            + &current_version.to_string()
            + ".metadata.json";

        self.object_store
            .copy_if_not_exists(
                &temp_metadata_location.into(),
                &metadata_location.as_str().into(),
            )
            .await?;

        self.cache.write().unwrap().insert(
            identifier.clone(),
            (metadata_location.clone(), metadata.clone().into()),
        );

        Ok(Table::new(identifier.clone(), self.clone(), metadata).await?)
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
        let metadata_location = match &mut metadata {
            TabularMetadata::View(metadata) => {
                if !check_view_requirements(&commit.requirements, metadata) {
                    return Err(IcebergError::InvalidFormat(
                        "View requirements not valid".to_owned(),
                    ));
                }
                apply_view_updates(metadata, commit.updates)?;
                let temp_metadata_location = new_metadata_location(&*metadata);

                self.object_store
                    .put(
                        &strip_prefix(&temp_metadata_location).into(),
                        serde_json::to_string(&metadata)?.into(),
                    )
                    .await?;

                let current_version = parse_version(&previous_metadata_location)? + 1;
                let metadata_location = metadata.location.clone()
                    + "/metadata/v"
                    + &current_version.to_string()
                    + ".metadata.json";

                self.object_store
                    .copy_if_not_exists(
                        &temp_metadata_location.into(),
                        &metadata_location.as_str().into(),
                    )
                    .await?;

                Ok(metadata_location)
            }
            _ => Err(IcebergError::InvalidFormat(
                "View update on entity that is not a view".to_owned(),
            )),
        }?;

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
        commit: CommitView<Identifier>,
    ) -> Result<MaterializedView, IcebergError> {
        let identifier = commit.identifier;
        let Some(entry) = self.cache.read().unwrap().get(&identifier).cloned() else {
            return Err(IcebergError::InvalidFormat(
                "Create table assertion".to_owned(),
            ));
        };
        let (previous_metadata_location, mut metadata) = entry;
        let metadata_location = match &mut metadata {
            TabularMetadata::MaterializedView(metadata) => {
                if !check_view_requirements(&commit.requirements, metadata) {
                    return Err(IcebergError::InvalidFormat(
                        "Materialized view requirements not valid".to_owned(),
                    ));
                }
                apply_view_updates(metadata, commit.updates)?;
                let temp_metadata_location = new_metadata_location(&*metadata);
                self.object_store
                    .put(
                        &strip_prefix(&temp_metadata_location).into(),
                        serde_json::to_string(&metadata)?.into(),
                    )
                    .await?;

                let current_version = parse_version(&previous_metadata_location)? + 1;
                let metadata_location = metadata.location.clone()
                    + "/metadata/v"
                    + &current_version.to_string()
                    + ".metadata.json";

                self.object_store
                    .copy_if_not_exists(
                        &temp_metadata_location.into(),
                        &metadata_location.as_str().into(),
                    )
                    .await?;

                Ok(metadata_location)
            }
            _ => Err(IcebergError::InvalidFormat(
                "Materialized view update on entity that is not a materialized view".to_owned(),
            )),
        }?;

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
        _identifier: Identifier,
        _metadata_location: &str,
    ) -> Result<Table, IcebergError> {
        unimplemented!()
    }

    fn object_store(&self, _: Bucket) -> Arc<dyn object_store::ObjectStore> {
        self.object_store.clone()
    }
}

impl FileCatalog {
    fn namespace_path(&self, namespace: &str) -> String {
        self.path.as_str().trim_end_matches('/').to_owned() + "/" + &self.name + "/" + namespace
    }

    fn tabular_path(&self, namespace: &str, name: &str) -> String {
        self.path.as_str().trim_end_matches('/').to_owned()
            + "/"
            + &self.name
            + "/"
            + namespace
            + "/"
            + name
    }

    async fn metadata_location(&self, identifier: &Identifier) -> Result<String, IcebergError> {
        let path = self.tabular_path(&identifier.namespace()[0], identifier.name()) + "/metadata";
        let mut files: Vec<String> = self
            .object_store
            .list(Some(&strip_prefix(&path).trim_start_matches('/').into()))
            .map_ok(|x| x.location.to_string())
            .try_filter(|x| {
                future::ready(
                    x.ends_with("metadata.json")
                        && x.starts_with((path.clone() + "/v").trim_start_matches('/')),
                )
            })
            .try_collect()
            .await
            .map_err(IcebergError::from)?;
        files.sort_unstable();
        files.into_iter().last().ok_or(IcebergError::NotFound(
            "Metadata".to_owned(),
            "file".to_owned(),
        ))
    }

    fn identifier(&self, path: &str) -> Identifier {
        let parts: Vec<&str> = path
            .trim_start_matches(self.path.trim_start_matches('/'))
            .trim_start_matches('/')
            .split('/')
            .skip(1)
            .take(2)
            .collect();
        Identifier::new(&[parts[0].to_owned()], parts[1])
    }

    fn namespace(&self, path: &str) -> Result<Namespace, IcebergError> {
        let parts = path
            .trim_start_matches(self.path.trim_start_matches('/'))
            .trim_start_matches('/')
            .split('/')
            .nth(1)
            .ok_or(IcebergError::InvalidFormat("Namespace in path".to_owned()))?
            .to_owned();
        Namespace::try_new(&[parts]).map_err(IcebergError::from)
    }
}

fn parse_version(path: &str) -> Result<u64, IcebergError> {
    path.split('/')
        .last()
        .ok_or(IcebergError::InvalidFormat("Metadata location".to_owned()))?
        .trim_start_matches('v')
        .trim_end_matches(".metadata.json")
        .parse()
        .map_err(IcebergError::from)
}

#[derive(Debug)]
pub struct FileCatalogList {
    path: String,
    object_store: Arc<dyn ObjectStore>,
}

impl FileCatalogList {
    pub async fn new(path: &str, object_store: Arc<dyn ObjectStore>) -> Result<Self, Error> {
        Ok(FileCatalogList {
            path: path.to_owned(),
            object_store,
        })
    }
}

#[async_trait]
impl CatalogList for FileCatalogList {
    fn catalog(&self, name: &str) -> Option<Arc<dyn Catalog>> {
        Some(Arc::new(FileCatalog {
            name: name.to_owned(),
            path: self.path.clone(),
            object_store: self.object_store.clone(),
            cache: Arc::new(RwLock::new(HashMap::new())),
        }))
    }
    async fn list_catalogs(&self) -> Vec<String> {
        todo!()
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
        table::Table,
    };
    use object_store::{memory::InMemory, ObjectStore};
    use std::sync::Arc;

    use crate::FileCatalog;

    #[tokio::test]
    async fn test_create_update_drop_table() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let catalog: Arc<dyn Catalog> =
            Arc::new(FileCatalog::new("/", "test", object_store).await.unwrap());
        let identifier = Identifier::parse("load_table.table3", None).unwrap();
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
            .with_schema(schema)
            .build(identifier.namespace(), catalog.clone())
            .await
            .expect("Failed to create table");

        let exists = Arc::clone(&catalog)
            .tabular_exists(&identifier)
            .await
            .expect("Table doesn't exist");
        assert!(exists);

        let tables = catalog
            .clone()
            .list_tabulars(
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
    }
}

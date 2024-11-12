use dashmap::DashMap;
use datafusion::{datasource::TableProvider, error::DataFusionError};
use futures::{executor::LocalPool, task::LocalSpawnExt};
use std::{collections::HashSet, sync::Arc};

use iceberg_rust::spec::{tabular::TabularMetadata, view_metadata::REF_PREFIX};
use iceberg_rust::{
    catalog::{
        bucket::Bucket,
        create::{CreateMaterializedView, CreateView},
        identifier::Identifier,
        namespace::Namespace,
        tabular::Tabular,
        Catalog,
    },
    error::Error as IcebergError,
    spec::table_metadata::new_metadata_location,
    spec::util::strip_prefix,
};

use crate::{error::Error, DataFusionTable};

type NamespaceNode = HashSet<String>;

#[derive(Debug)]
enum Node {
    Namespace(NamespaceNode),
    Relation(Identifier),
}

#[derive(Debug)]
pub struct Mirror {
    storage: DashMap<String, Node>,
    catalog: Arc<dyn Catalog>,
    branch: Option<String>,
}

impl Mirror {
    pub async fn new(
        catalog: Arc<dyn Catalog>,
        branch: Option<String>,
    ) -> Result<Self, DataFusionError> {
        let storage = DashMap::new();
        let namespaces = catalog
            .clone()
            .list_namespaces(None)
            .await
            .map_err(|err| DataFusionError::Internal(format!("{}", err)))?;
        for namespace in namespaces {
            let mut namespace_node = HashSet::new();
            let tables = catalog
                .clone()
                .list_tabulars(&namespace)
                .await
                .map_err(|err| DataFusionError::Internal(format!("{}", err)))?;
            for identifier in tables {
                namespace_node.insert(identifier.to_string());
                storage.insert(identifier.to_string(), Node::Relation(identifier));
            }
            storage.insert(namespace.to_string(), Node::Namespace(namespace_node));
        }

        Ok(Mirror {
            storage,
            catalog,
            branch,
        })
    }
    pub fn new_sync(catalog: Arc<dyn Catalog>, branch: Option<String>) -> Self {
        let storage = DashMap::new();

        Mirror {
            storage,
            catalog,
            branch,
        }
    }
    /// Lists all tables in the given namespace.
    pub fn table_names(&self, namespace: &Namespace) -> Result<Vec<Identifier>, DataFusionError> {
        let node = self
            .storage
            .get(&namespace.to_string())
            .ok_or_else(|| Error::InvalidFormat("namespace in catalog".to_string()))
            .map_err(|err| DataFusionError::Internal(format!("{}", err)))?;
        let names = if let Node::Namespace(names) = node.value() {
            Ok(names)
        } else {
            Err(Error::InvalidFormat("table in namespace".to_string()))
        }
        .map_err(|err| DataFusionError::Internal(format!("{}", err)))?;
        Ok(names
            .iter()
            .filter_map(|r| {
                let r = &self.storage.get(r)?;
                match &r.value() {
                    Node::Relation(ident) => Some(ident.clone()),
                    Node::Namespace(_) => None,
                }
            })
            .collect())
    }
    /// Lists all namespaces in the catalog.
    pub fn schema_names(&self, _parent: Option<&str>) -> Result<Vec<Namespace>, DataFusionError> {
        self.storage
            .iter()
            .filter_map(|r| match r.value() {
                Node::Relation(_) => None,
                Node::Namespace(_) => Some(r.key().clone()),
            })
            .map(|x| {
                Namespace::try_new(
                    x.split('.')
                        .map(|s| s.to_owned())
                        .collect::<Vec<_>>()
                        .as_slice(),
                )
            })
            .collect::<Result<_, iceberg_rust::spec::error::Error>>()
            .map_err(|err| DataFusionError::Internal(format!("{}", err)))
    }
    pub async fn table(
        &self,
        identifier: Identifier,
    ) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        self.catalog
            .clone()
            .load_tabular(&identifier)
            .await
            .map(|tabular| match tabular {
                Tabular::Table(table) => {
                    let end = self
                        .branch
                        .as_ref()
                        .and_then(|branch| table.metadata().refs.get(branch))
                        .map(|x| x.snapshot_id);
                    Arc::new(DataFusionTable::new(
                        Tabular::Table(table),
                        None,
                        end,
                        self.branch.as_deref(),
                    )) as Arc<dyn TableProvider>
                }
                Tabular::View(view) => {
                    let end = self
                        .branch
                        .as_ref()
                        .and_then(|branch| {
                            view.metadata()
                                .properties
                                .get(&(REF_PREFIX.to_string() + branch))
                        })
                        .map(|x| x.parse::<i64>().unwrap());
                    Arc::new(DataFusionTable::new(
                        Tabular::View(view),
                        None,
                        end,
                        self.branch.as_deref(),
                    )) as Arc<dyn TableProvider>
                }
                Tabular::MaterializedView(matview) => {
                    let end = self
                        .branch
                        .as_ref()
                        .and_then(|branch| {
                            matview
                                .metadata()
                                .properties
                                .get(&(REF_PREFIX.to_string() + branch))
                        })
                        .map(|x| x.parse::<i64>().unwrap());
                    Arc::new(DataFusionTable::new(
                        Tabular::MaterializedView(matview),
                        None,
                        end,
                        self.branch.as_deref(),
                    )) as Arc<dyn TableProvider>
                }
            })
            .map(Some)
            .or_else(|err| {
                if matches!(err, IcebergError::CatalogNotFound) {
                    Ok(None)
                } else {
                    Err(err)
                }
            })
            .map_err(Error::from)
            .map_err(DataFusionError::from)
    }
    pub fn table_exists(&self, identifier: Identifier) -> bool {
        self.storage.contains_key(&identifier.to_string())
    }
    pub fn register_table(
        &self,
        identifier: Identifier,
        table: Arc<dyn TableProvider>,
    ) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        self.storage
            .insert(identifier.to_string(), Node::Relation(identifier.clone()));
        match self
            .storage
            .get_mut(&identifier.namespace().to_string())
            .ok_or(DataFusionError::Internal(
                "Namespace doesn't exist".to_string(),
            ))?
            .value_mut()
        {
            Node::Namespace(namespace) => {
                namespace.insert(identifier.to_string());
            }
            Node::Relation(_) => {}
        };
        let pool = LocalPool::new();
        let spawner = pool.spawner();
        let cloned_catalog = self.catalog.clone();
        spawner
            .spawn_local({
                let table = table.clone();
                async move {
                    let metadata = table
                        .clone()
                        .as_any()
                        .downcast_ref::<DataFusionTable>()
                        .ok_or(DataFusionError::Internal(
                            "Table is not an iceberg datafusion table.".to_owned(),
                        ))
                        .unwrap()
                        .tabular
                        .read()
                        .await
                        .metadata()
                        .to_owned();
                    match metadata {
                        TabularMetadata::Table(_) => {
                            let metadata_location = new_metadata_location(&metadata);
                            let object_store = cloned_catalog
                                .object_store(Bucket::from_path(&metadata_location).unwrap());
                            object_store
                                .put(
                                    &strip_prefix(&metadata_location).into(),
                                    serde_json::to_vec(&metadata).unwrap().into(),
                                )
                                .await
                                .unwrap();
                            cloned_catalog
                                .register_table(identifier, &metadata_location)
                                .await
                                .unwrap();
                        }
                        TabularMetadata::View(metadata) => {
                            let name = identifier.name().to_owned();
                            let view_version =
                                metadata.versions[&metadata.current_version_id].clone();
                            cloned_catalog
                                .create_view(
                                    identifier,
                                    CreateView {
                                        name,
                                        location: Some(metadata.location),
                                        schema: metadata.schemas[&view_version.schema_id].clone(),
                                        view_version,
                                        properties: metadata.properties,
                                    },
                                )
                                .await
                                .unwrap();
                        }
                        TabularMetadata::MaterializedView(metadata) => {
                            let name = identifier.name().to_owned();
                            let view_version =
                                metadata.versions[&metadata.current_version_id].clone();
                            cloned_catalog
                                .create_materialized_view(
                                    identifier,
                                    CreateMaterializedView {
                                        name,
                                        location: Some(metadata.location),
                                        schema: metadata.schemas[&view_version.schema_id].clone(),
                                        view_version,
                                        properties: metadata.properties,
                                        partition_spec: None,
                                        write_order: None,
                                        stage_create: None,
                                        table_properties: None,
                                    },
                                )
                                .await
                                .unwrap();
                        }
                    }
                }
            })
            .map_err(|err| DataFusionError::Internal(format!("{}", err)))?;
        Ok(Some(table))
    }
    pub fn deregister_table(
        &self,
        identifier: Identifier,
    ) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        match self
            .storage
            .get_mut(&identifier.namespace().to_string())
            .ok_or(DataFusionError::Internal(
                "Namespace doesn't exist".to_string(),
            ))?
            .value_mut()
        {
            Node::Namespace(namespace) => {
                namespace.remove(&identifier.to_string());
            }
            Node::Relation(_) => {}
        };
        let pool = LocalPool::new();
        let spawner = pool.spawner();
        let cloned_catalog = self.catalog.clone();
        spawner
            .spawn_local(async move {
                cloned_catalog.drop_table(&identifier).await.unwrap();
            })
            .map_err(|err| DataFusionError::Internal(format!("{}", err)))?;
        // Currently can't synchronously return a table which has to be fetched asynchronously
        Ok(None)
    }

    pub fn catalog(&self) -> Arc<dyn Catalog> {
        self.catalog.clone()
    }
}

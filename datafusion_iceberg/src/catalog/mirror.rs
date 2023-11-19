use dashmap::DashMap;
use datafusion::{datasource::TableProvider, error::DataFusionError};
use futures::{executor::LocalPool, task::LocalSpawnExt};
use std::{collections::HashSet, sync::Arc};

use iceberg_rust::{
    catalog::{identifier::Identifier, namespace::Namespace, tabular::Tabular, Catalog},
    error::Error as IcebergError,
};
use iceberg_rust_spec::spec::view_metadata::REF_PREFIX;

use crate::{error::Error, DataFusionTable};

type NamespaceNode = HashSet<String>;

#[derive(Debug)]
enum Node {
    Namespace(NamespaceNode),
    Relation(Identifier),
}

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
                .list_tables(&namespace)
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
    /// Lists all tables in the given namespace.
    pub fn table_names(&self, namespace: &Namespace) -> Result<Vec<Identifier>, DataFusionError> {
        let tables = self
            .storage
            .get(&namespace.to_string())
            .ok_or_else(|| Error::InvalidFormat("namespace in catalog".to_string()))
            .map_err(|err| DataFusionError::Internal(format!("{}", err)))?;
        let names = match tables.value() {
            Node::Relation(_) => Err(Error::InvalidFormat("table in namespace".to_string())),
            Node::Namespace(names) => Ok(names),
        }
        .map_err(|err| DataFusionError::Internal(format!("{}", err)))?;
        names
            .iter()
            .map(|x| {
                Identifier::parse(x).map_err(|err| DataFusionError::Internal(format!("{}", err)))
            })
            .collect::<Result<_, DataFusionError>>()
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
            .collect::<Result<_, IcebergError>>()
            .map_err(|err| DataFusionError::Internal(format!("{}", err)))
    }
    pub async fn table(&self, identifier: Identifier) -> Option<Arc<dyn TableProvider>> {
        self.catalog
            .clone()
            .load_table(&identifier)
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
                                .get(&(REF_PREFIX.to_string() + &branch))
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
                                .get(&(REF_PREFIX.to_string() + &branch))
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
            .ok()
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
                    let metadata_location = table
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
                        .metadata_location()
                        .to_owned();
                    cloned_catalog
                        .register_table(identifier, &metadata_location)
                        .await
                        .unwrap();
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
}

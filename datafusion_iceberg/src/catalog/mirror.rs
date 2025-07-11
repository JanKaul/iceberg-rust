use dashmap::DashMap;
use datafusion::{datasource::TableProvider, error::DataFusionError};
use std::{collections::HashSet, sync::Arc};
use tokio::runtime::Handle;

use iceberg_rust::spec::view_metadata::REF_PREFIX;
use iceberg_rust::{
    catalog::{identifier::Identifier, namespace::Namespace, tabular::Tabular, Catalog},
    error::Error as IcebergError,
};

use crate::{error::Error, DataFusionTable};

type NamespaceNode = HashSet<String>;

#[derive(Debug, Clone)]
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
            .map_err(|err| DataFusionError::External(Box::new(err)))?;
        for namespace in namespaces {
            let mut namespace_node = HashSet::new();
            let tables = catalog
                .clone()
                .list_tabulars(&namespace)
                .await
                .map_err(|err| DataFusionError::External(Box::new(err)))?;
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
            .ok_or_else(|| IcebergError::InvalidFormat("namespace in catalog".to_string()))
            .map_err(|err| DataFusionError::External(Box::new(err)))?;
        let names = if let Node::Namespace(names) = node.value() {
            Ok(names)
        } else {
            Err(IcebergError::InvalidFormat(
                "table in namespace".to_string(),
            ))
        }
        .map_err(|err| DataFusionError::External(Box::new(err)))?;
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
            .map_err(|err| DataFusionError::External(Box::new(err)))
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
        self.storage
            .get(&identifier.to_string())
            .is_some_and(|node| matches!(node.value(), Node::Relation(_)))
    }

    pub fn schema_exists(&self, name: &str) -> bool {
        self.storage
            .get(name)
            .is_some_and(|node| matches!(node.value(), Node::Namespace(_)))
    }

    pub fn catalog(&self) -> Arc<dyn Catalog> {
        self.catalog.clone()
    }

    pub fn register_schema(&self, name: &str) -> Result<Option<NamespaceNode>, DataFusionError> {
        let namespace = Namespace::try_new(
            &name
                .split('.')
                .map(|z| z.to_owned())
                .collect::<Vec<String>>(),
        )
        .map_err(|err| DataFusionError::External(Box::new(err)))?;

        let old_value =
            self.storage
                .get(&namespace.to_string())
                .and_then(|entry| match entry.value() {
                    Node::Namespace(namespace_node) => Some(namespace_node.clone()),
                    _ => None,
                });

        self.storage
            .insert(namespace.to_string(), Node::Namespace(HashSet::new()));
        Ok(old_value)
    }
}

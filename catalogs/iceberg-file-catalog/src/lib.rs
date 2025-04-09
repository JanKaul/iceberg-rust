use std::{
    collections::HashMap,
    convert::identity,
    sync::{Arc, RwLock},
};

use async_trait::async_trait;
use futures::{future, TryStreamExt};
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

use crate::error::Error;

#[derive(Debug)]
pub struct FileCatalog {
    path: String,
    object_store: ObjectStoreBuilder,
    cache: Arc<RwLock<HashMap<Identifier, (String, TabularMetadata)>>>,
}

pub mod error;

impl FileCatalog {
    pub async fn new(path: &str, object_store: ObjectStoreBuilder) -> Result<Self, Error> {
        Ok(FileCatalog {
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
        self.path.trim_end_matches('/').split("/").last().unwrap()
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
        let bucket = Bucket::from_path(&self.path)?;
        let object_store = self.object_store.build(bucket)?;

        object_store
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
        let bucket = Bucket::from_path(&self.path)?;
        let object_store = self.object_store.build(bucket)?;

        object_store
            .list_with_delimiter(Some(
                &strip_prefix(self.path.trim_start_matches('/')).into(),
            ))
            .await
            .map_err(IcebergError::from)?
            .common_prefixes
            .into_iter()
            .map(|x| self.namespace(x.as_ref()))
            .collect::<Result<_, IcebergError>>()
    }
    async fn tabular_exists(&self, identifier: &Identifier) -> Result<bool, IcebergError> {
        self.metadata_location(identifier)
            .await
            .map(|_| true)
            .or(Ok(false))
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
        let bucket = Bucket::from_path(&self.path)?;
        let object_store = self.object_store.build(bucket)?;

        let metadata_location = self.metadata_location(identifier).await?;

        let bytes = object_store
            .get(&strip_prefix(&metadata_location).as_str().into())
            .await
            .map_err(|_| IcebergError::CatalogNotFound)?
            .bytes()
            .await?;

        let metadata: TabularMetadata = serde_json::from_slice(&bytes)?;

        self.cache.write().unwrap().insert(
            identifier.clone(),
            (metadata_location.clone(), metadata.clone()),
        );

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
        let object_store = self.default_object_store(bucket);

        let metadata_location = location + "/metadata/v0.metadata.json";

        object_store
            .put_metadata(&metadata_location, metadata.as_ref())
            .await?;

        object_store.put_version_hint(&metadata_location).await.ok();

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
        let object_store = self.default_object_store(bucket);

        let metadata_location = location + "/metadata/v0.metadata.json";

        object_store
            .put_metadata(&metadata_location, metadata.as_ref())
            .await?;

        object_store.put_version_hint(&metadata_location).await.ok();

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
        let object_store = self.default_object_store(bucket);

        let metadata_location = location + "/metadata/v0.metadata.json";

        let table_metadata_location =
            table_metadata.location.clone() + "/metadata/v0.metadata.json";

        object_store
            .put_metadata(&metadata_location, metadata.as_ref())
            .await?;

        object_store.put_version_hint(&metadata_location).await.ok();

        object_store
            .put_metadata(&table_metadata_location, table_metadata.as_ref())
            .await?;

        self.cache.write().unwrap().insert(
            identifier.clone(),
            (metadata_location.clone(), metadata.clone().into()),
        );

        Ok(MaterializedView::new(identifier.clone(), self.clone(), metadata).await?)
    }

    async fn update_table(self: Arc<Self>, commit: CommitTable) -> Result<Table, IcebergError> {
        let bucket = Bucket::from_path(&self.path)?;
        let object_store = self.object_store.build(bucket)?;

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

        object_store
            .put_metadata(&temp_metadata_location, metadata.as_ref())
            .await?;

        let metadata_location =
            new_filesystem_metadata_location(&metadata.location, &previous_metadata_location)?;

        object_store
            .copy_if_not_exists(
                &strip_prefix(&temp_metadata_location).into(),
                &strip_prefix(&metadata_location).into(),
            )
            .await?;

        object_store.put_version_hint(&metadata_location).await.ok();

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
        let bucket = Bucket::from_path(&self.path)?;
        let object_store = self.object_store.build(bucket)?;

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

                object_store
                    .put_metadata(&temp_metadata_location, metadata.as_ref())
                    .await?;

                let metadata_location = new_filesystem_metadata_location(
                    &metadata.location,
                    &previous_metadata_location,
                )?;

                object_store
                    .copy_if_not_exists(
                        &strip_prefix(&temp_metadata_location).into(),
                        &strip_prefix(&metadata_location).into(),
                    )
                    .await?;

                object_store.put_version_hint(&metadata_location).await.ok();

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
        commit: CommitView<FullIdentifier>,
    ) -> Result<MaterializedView, IcebergError> {
        let bucket = Bucket::from_path(&self.path)?;
        let object_store = self.object_store.build(bucket)?;

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

                object_store
                    .put_metadata(&temp_metadata_location, metadata.as_ref())
                    .await?;

                let metadata_location = new_filesystem_metadata_location(
                    &metadata.location,
                    &previous_metadata_location,
                )?;

                object_store
                    .copy_if_not_exists(
                        &strip_prefix(&temp_metadata_location).into(),
                        &strip_prefix(&metadata_location).into(),
                    )
                    .await?;

                object_store.put_version_hint(&metadata_location).await.ok();

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
}

impl FileCatalog {
    fn default_object_store(&self, bucket: Bucket) -> Arc<dyn object_store::ObjectStore> {
        Arc::new(self.object_store.build(bucket).unwrap())
    }
    fn namespace_path(&self, namespace: &str) -> String {
        self.path.as_str().trim_end_matches('/').to_owned() + "/" + namespace
    }

    fn tabular_path(&self, namespace: &str, name: &str) -> String {
        self.path.as_str().trim_end_matches('/').to_owned() + "/" + namespace + "/" + name
    }

    async fn metadata_location(&self, identifier: &Identifier) -> Result<String, IcebergError> {
        let bucket = Bucket::from_path(&self.path)?;
        let object_store = self.object_store.build(bucket)?;

        let path = self.tabular_path(&identifier.namespace()[0], identifier.name()) + "/metadata";
        let mut files: Vec<String> = object_store
            .list(Some(&strip_prefix(&path).into()))
            .map_ok(|x| x.location.to_string())
            .try_filter(|x| {
                future::ready(
                    x.ends_with("metadata.json")
                        && x.starts_with((strip_prefix(&path) + "/v").trim_start_matches('/')),
                )
            })
            .try_collect()
            .await
            .map_err(IcebergError::from)?;
        files.sort_by(|x, y| {
            let x = x
                .trim_start_matches((strip_prefix(&path) + "/v").trim_start_matches("/"))
                .trim_end_matches("/")
                .trim_end_matches(".metadata.json")
                .parse::<usize>()
                .unwrap();
            let y = y
                .trim_start_matches((strip_prefix(&path) + "/v").trim_start_matches("/"))
                .trim_end_matches("/")
                .trim_end_matches(".metadata.json")
                .parse::<usize>()
                .unwrap();
            x.cmp(&y)
        });
        files
            .into_iter()
            .next_back()
            .ok_or(IcebergError::CatalogNotFound)
    }

    fn identifier(&self, path: &str) -> Identifier {
        let parts: Vec<&str> = trim_start_path(path)
            .trim_start_matches(trim_start_path(&self.path))
            .trim_start_matches('/')
            .split('/')
            .take(2)
            .collect();
        Identifier::new(&[parts[0].to_owned()], parts[1])
    }

    fn namespace(&self, path: &str) -> Result<Namespace, IcebergError> {
        let parts = trim_start_path(path)
            .trim_start_matches(trim_start_path(&self.path))
            .trim_start_matches('/')
            .split('/')
            .next()
            .ok_or(IcebergError::InvalidFormat("Namespace in path".to_owned()))?
            .to_owned();
        Namespace::try_new(&[parts]).map_err(IcebergError::from)
    }
}

fn trim_start_path(path: &str) -> &str {
    path.trim_start_matches('/').trim_start_matches("s3://")
}

fn parse_version(path: &str) -> Result<u64, IcebergError> {
    path.split('/')
        .next_back()
        .ok_or(IcebergError::InvalidFormat("Metadata location".to_owned()))?
        .trim_start_matches('v')
        .trim_end_matches(".metadata.json")
        .parse()
        .map_err(IcebergError::from)
}

fn new_filesystem_metadata_location(
    metadata_location: &str,
    previous_metadata_location: &str,
) -> Result<String, IcebergError> {
    let current_version = parse_version(previous_metadata_location)? + 1;
    Ok(metadata_location.to_string()
        + "/metadata/v"
        + &current_version.to_string()
        + ".metadata.json")
}

#[derive(Debug)]
pub struct FileCatalogList {
    path: String,
    object_store: ObjectStoreBuilder,
}

impl FileCatalogList {
    pub async fn new(path: &str, object_store: ObjectStoreBuilder) -> Result<Self, Error> {
        Ok(FileCatalogList {
            path: path.to_owned(),
            object_store,
        })
    }

    fn parse_catalog(&self, path: &str) -> Result<String, IcebergError> {
        trim_start_path(path.trim_start_matches(trim_start_path(&self.path)))
            .trim_start_matches('/')
            .split('/')
            .next()
            .ok_or(IcebergError::InvalidFormat("Catalog in path".to_owned()))
            .map(ToOwned::to_owned)
    }
}

#[async_trait]
impl CatalogList for FileCatalogList {
    fn catalog(&self, name: &str) -> Option<Arc<dyn Catalog>> {
        Some(Arc::new(FileCatalog {
            path: self.path.clone() + "/" + name,
            object_store: self.object_store.clone(),
            cache: Arc::new(RwLock::new(HashMap::new())),
        }))
    }
    async fn list_catalogs(&self) -> Vec<String> {
        let bucket = Bucket::from_path(&self.path).unwrap();
        let object_store = self.object_store.build(bucket).unwrap();

        object_store
            .list_with_delimiter(Some(&strip_prefix(trim_start_path(&self.path)).into()))
            .await
            .map_err(IcebergError::from)
            .unwrap()
            .common_prefixes
            .into_iter()
            .map(|x| self.parse_catalog(x.as_ref()))
            .collect::<Result<_, IcebergError>>()
            .unwrap()
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
        object_store::{Bucket, ObjectStoreBuilder},
        spec::util::strip_prefix,
    };
    use std::{sync::Arc, time::Duration};
    use testcontainers::{core::ExecCommand, runners::AsyncRunner, ImageExt};
    use testcontainers_modules::localstack::LocalStack;
    use tokio::time::sleep;
    // use testcontainers::{core::ExecCommand, runners::AsyncRunner, ImageExt};
    // use testcontainers_modules::localstack::LocalStack;

    use crate::FileCatalog;

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

        let localstack_host = localstack.get_host().await.unwrap();
        let localstack_port = localstack.get_host_port_ipv4(4566).await.unwrap();

        let object_store = ObjectStoreBuilder::s3()
            .with_config("aws_access_key_id".parse().unwrap(), "user")
            .with_config("aws_secret_access_key".parse().unwrap(), "password")
            .with_config(
                "endpoint".parse().unwrap(),
                format!("http://{}:{}", localstack_host, localstack_port),
            )
            .with_config("region".parse().unwrap(), "us-east-1")
            .with_config("allow_http".parse().unwrap(), "true");
        // let object_store = ObjectStoreBuilder::memory();

        let iceberg_catalog: Arc<dyn Catalog> = Arc::new(
            FileCatalog::new("s3://warehouse", object_store.clone())
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

        let object_store = object_store
            .build(Bucket::from_path("s3://warehouse").unwrap())
            .unwrap();

        let version_hint = object_store
            .get(&strip_prefix("s3://warehouse/tpch/lineitem/metadata/version-hint.text").into())
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();

        assert_eq!(
            std::str::from_utf8(&version_hint).unwrap(),
            "s3://warehouse/tpch/lineitem/metadata/v1.metadata.json"
        );
    }

    #[tokio::test]
    async fn test_namespace_path_normal_case() {
        let test_struct = FileCatalog::new("/base/path", ObjectStoreBuilder::memory())
            .await
            .unwrap();
        assert_eq!(
            test_struct.namespace_path("test_namespace"),
            "/base/path/test_namespace"
        );
    }

    #[tokio::test]
    async fn test_namespace_path_s3() {
        let test_struct = FileCatalog::new("s3://base/path", ObjectStoreBuilder::memory())
            .await
            .unwrap();
        assert_eq!(
            test_struct.namespace_path("test_namespace"),
            "s3://base/path/test_namespace"
        );
    }

    #[tokio::test]
    async fn test_identifier_normal_case() {
        let test_struct = FileCatalog::new("/base/path", ObjectStoreBuilder::memory())
            .await
            .unwrap();

        let result = test_struct.identifier("/base/path/test_namespace/test_table");
        assert_eq!(result.namespace()[0], "test_namespace");
        assert_eq!(result.name(), "test_table");
    }

    #[tokio::test]
    async fn test_namespace_normal_case() {
        let test_struct = FileCatalog::new("/base/path", ObjectStoreBuilder::memory())
            .await
            .unwrap();

        let result = test_struct.namespace("/base/path/test_namespace").unwrap();
        assert_eq!(result.as_ref(), &["test_namespace".to_string()]);
    }
}

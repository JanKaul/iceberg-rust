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
        materialized_view_metadata::MaterializedViewMetadata,
        table_metadata::{new_metadata_location, MetadataLog, TableMetadata},
        tabular::TabularMetadata,
        util::strip_prefix,
        view_metadata::ViewMetadata,
    },
    table::Table,
    view::View,
};
use object_store::ObjectStoreExt;
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

/// How a catalog connects to its database.
///
/// The catalog opens its pool through sqlx's `Any` driver, which takes a URL
/// and nothing else, so a caller had no way to size the pool or to configure
/// the sessions on it. Both matter under load: the pool's defaults let ten
/// writers contend on a database that may serialize them, and settings such as
/// SQLite's `busy_timeout` or PostgreSQL's `statement_timeout` are per-session
/// and cannot be carried on the URL — sqlx's SQLite URL parser rejects them as
/// query parameters outright.
#[derive(Debug, Clone, Default)]
pub struct SqlCatalogOptions {
    pool: Option<PoolOptions<sqlx::Any>>,
    session_statements: Vec<String>,
}

impl SqlCatalogOptions {
    /// Options that reproduce the defaults: sqlx's pool settings, except that a
    /// private in-memory SQLite database is held to a single connection, since
    /// a second connection would open a different database.
    pub fn new() -> Self {
        Self::default()
    }

    /// Size and time-bound the connection pool.
    pub fn with_pool_options(mut self, pool: PoolOptions<sqlx::Any>) -> Self {
        self.pool = Some(pool);
        self
    }

    /// Statements to run on every new connection before it is used, such as
    /// `pragma busy_timeout = 30000` or `set statement_timeout = '30s'`.
    ///
    /// They run in order, after the catalog's own tables are ensured, and an
    /// error from any of them fails the connection.
    pub fn with_session_statements(
        mut self,
        statements: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.session_statements = statements.into_iter().map(Into::into).collect();
        self
    }
}

impl SqlCatalog {
    pub async fn new(
        url: &str,
        name: &str,
        object_store: ObjectStoreBuilder,
    ) -> Result<Self, Error> {
        Self::new_with_options(url, name, object_store, SqlCatalogOptions::default()).await
    }

    /// Open a catalog with explicit connection options.
    pub async fn new_with_options(
        url: &str,
        name: &str,
        object_store: ObjectStoreBuilder,
        options: SqlCatalogOptions,
    ) -> Result<Self, Error> {
        install_default_drivers();

        let mut pool_options = options.pool.unwrap_or_default();
        if url == "sqlite://" {
            // A private in-memory SQLite database is only reachable through the
            // connection that created it; a second connection opens a distinct,
            // empty database. Enforce the single-connection cap even when the
            // caller supplied their own pool options.
            pool_options = pool_options.max_connections(1);
        }

        let pool = pool_options_with_setup(
            pool_options,
            url.starts_with("sqlite"),
            options.session_statements,
        )
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

    /// Atomically move the catalog's metadata pointer for `identifier` from
    /// `previous_metadata_location` to `metadata_location`.
    ///
    /// The update only matches while the previous location is still current, so
    /// a writer that committed first leaves this update matching no rows. That
    /// outcome is a lost commit, not a success: the new metadata file exists in
    /// the object store but nothing references it. Report it as a conflict and
    /// drop the cache entry, whose metadata is now known to be stale, so the
    /// next load refreshes from the catalog.
    async fn swap_metadata_location(
        &self,
        identifier: &Identifier,
        previous_metadata_location: &str,
        metadata_location: &str,
    ) -> Result<(), IcebergError> {
        let catalog_name = &self.name;
        let namespace = identifier.namespace().to_string();
        let name = identifier.name();

        let result = sqlx::query(&format!("update iceberg_tables set metadata_location = '{metadata_location}', previous_metadata_location = '{previous_metadata_location}' where catalog_name = '{catalog_name}' and table_namespace = '{namespace}' and table_name = '{name}' and metadata_location = '{previous_metadata_location}';")).execute(&self.pool).await.map_err(Error::from)?;

        if result.rows_affected() == 0 {
            self.cache.write().unwrap().remove(identifier);
            return Err(IcebergError::CommitConflict(identifier.to_string()));
        }

        Ok(())
    }
}

/// Pool options whose connections get the default SQLite pragmas, then the
/// catalog's tables, then the caller's session statements.
///
/// The caller's statements run last so they can override a default: a table
/// wanting `synchronous = full`, say, sets it and wins.
fn pool_options_with_setup(
    pool_options: PoolOptions<sqlx::Any>,
    is_sqlite: bool,
    session_statements: Vec<String>,
) -> PoolOptions<sqlx::Any> {
    // The hook runs per connection, so the statements are shared rather than
    // cloned for each one.
    let session_statements = Arc::new(session_statements);

    AnyPoolOptions::after_connect(pool_options, move |connection, _| {
        let session_statements = session_statements.clone();
        Box::pin(async move {
            if is_sqlite {
                // Enable write-ahead logging and a busy timeout on every SQLite
                // connection. WAL lets readers proceed during a write and makes
                // each write cheaper, so concurrent catalog commits don't
                // serialize behind an exclusive rollback-journal lock; the busy
                // timeout avoids immediate "database is locked" errors under
                // brief write contention. Both are no-ops on an in-memory
                // database.
                connection.execute("PRAGMA journal_mode=WAL;").await?;
                connection.execute("PRAGMA busy_timeout=30000;").await?;
            }
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
            for statement in session_statements.iter() {
                connection.execute(statement.as_str()).await?;
            }
            Ok(())
        })
    })
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
    async fn drop_namespace(&self, namespace: &Namespace) -> Result<(), IcebergError> {
        let catalog_name = self.name.clone();
        let namespace_str = namespace.to_string();

        sqlx::query(&format!(
            "delete from iceberg_namespace_properties where catalog_name = '{catalog_name}' and namespace = '{namespace_str}';"
        ))
        .execute(&self.pool)
        .await
        .map_err(Error::from)?;

        Ok(())
    }
    /// Load the namespace properties from the catalog
    async fn load_namespace(
        &self,
        namespace: &Namespace,
    ) -> Result<HashMap<String, String>, IcebergError> {
        let catalog_name = self.name.clone();
        let namespace_str = namespace.to_string();

        let rows = sqlx::query(&format!(
            "select property_key, property_value from iceberg_namespace_properties where catalog_name = '{catalog_name}' and namespace = '{namespace_str}';"
        ))
        .fetch_all(&self.pool)
        .await
        .map_err(Error::from)?;

        let mut properties = HashMap::new();
        for row in &rows {
            let key: String = row.try_get(0).map_err(Error::from)?;
            // Skip the synthetic "exists" marker that create_namespace inserts when
            // no real properties were provided.
            if key == "exists" {
                continue;
            }
            let value: String = row.try_get(1).map_err(Error::from)?;
            properties.insert(key, value);
        }

        Ok(properties)
    }
    /// Update the namespace properties in the catalog
    async fn update_namespace(
        &self,
        namespace: &Namespace,
        updates: Option<HashMap<String, String>>,
        removals: Option<Vec<String>>,
    ) -> Result<(), IcebergError> {
        let catalog_name = self.name.clone();
        let namespace_str = namespace.to_string();

        if let Some(removals) = removals {
            for key in removals {
                sqlx::query(&format!(
                    "delete from iceberg_namespace_properties where catalog_name = '{catalog_name}' and namespace = '{namespace_str}' and property_key = '{key}';"
                ))
                .execute(&self.pool)
                .await
                .map_err(Error::from)?;
            }
        }

        if let Some(updates) = updates {
            for (key, value) in updates {
                // Delete-then-insert keeps this portable across sqlite/postgres/mysql
                // (sqlx::any doesn't expose dialect-specific upsert syntax uniformly).
                sqlx::query(&format!(
                    "delete from iceberg_namespace_properties where catalog_name = '{catalog_name}' and namespace = '{namespace_str}' and property_key = '{key}';"
                ))
                .execute(&self.pool)
                .await
                .map_err(Error::from)?;

                sqlx::query(&format!(
                    "insert into iceberg_namespace_properties (catalog_name, namespace, property_key, property_value) values ('{catalog_name}', '{namespace_str}', '{key}', '{value}');"
                ))
                .execute(&self.pool)
                .await
                .map_err(Error::from)?;
            }
        }

        Ok(())
    }
    /// Check if a namespace exists
    async fn namespace_exists(&self, namespace: &Namespace) -> Result<bool, IcebergError> {
        let catalog_name = self.name.clone();
        let namespace_str = namespace.to_string();

        let rows = sqlx::query(&format!(
            "select 1 from iceberg_namespace_properties where catalog_name = '{catalog_name}' and namespace = '{namespace_str}' limit 1;"
        ))
        .fetch_all(&self.pool)
        .await
        .map_err(Error::from)?;

        Ok(!rows.is_empty())
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
        // Timestamp of the metadata file we are about to supersede, recorded in
        // the metadata log per the Iceberg spec (the log entry marks when the
        // previous metadata was created).
        let previous_last_updated_ms = metadata.last_updated_ms;
        apply_table_updates(&mut metadata, commit.updates)?;

        // Maintain the metadata log and honor `write.metadata.previous-versions-max`
        // / `write.metadata.delete-after-commit.enabled`. iceberg-rust did not
        // previously record superseded metadata files, so every commit left an
        // orphan `metadata.json` behind; this bounds that growth.
        let delete_after_commit = metadata
            .properties
            .get("write.metadata.delete-after-commit.enabled")
            .map(|v| v == "true")
            .unwrap_or(false);
        let previous_versions_max = metadata
            .properties
            .get("write.metadata.previous-versions-max")
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(100);
        metadata.metadata_log.push(MetadataLog {
            metadata_file: previous_metadata_location.to_string(),
            timestamp_ms: previous_last_updated_ms,
        });
        let expired_metadata_files: Vec<String> =
            if metadata.metadata_log.len() > previous_versions_max {
                let remove = metadata.metadata_log.len() - previous_versions_max;
                metadata
                    .metadata_log
                    .drain(0..remove)
                    .map(|entry| entry.metadata_file)
                    .collect()
            } else {
                Vec::new()
            };

        let metadata_location = new_metadata_location(&metadata);
        object_store
            .put_metadata(&metadata_location, metadata.as_ref())
            .await?;
        object_store.put_version_hint(&metadata_location).await.ok();

        // A lost compare-and-swap returns early, so reaching this point means
        // our commit landed and the aged-out files are ours to reclaim — a
        // concurrent winner's files are never touched. Best effort: a failed
        // delete only leaves an orphan, never corrupts state.
        self.swap_metadata_location(&identifier, &previous_metadata_location, &metadata_location)
            .await?;

        if delete_after_commit {
            for path in expired_metadata_files {
                let _ = object_store.delete(&strip_prefix(&path).into()).await;
            }
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

        self.swap_metadata_location(&identifier, &previous_metadata_location, &metadata_location)
            .await?;

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

        self.swap_metadata_location(&identifier, &previous_metadata_location, &metadata_location)
            .await?;

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
        Self::new_with_options(url, object_store, SqlCatalogOptions::default()).await
    }

    /// Open a catalog list with explicit connection options.
    pub async fn new_with_options(
        url: &str,
        object_store: ObjectStoreBuilder,
        options: SqlCatalogOptions,
    ) -> Result<Self, Error> {
        install_default_drivers();

        let pool_options = options.pool.unwrap_or_else(|| {
            let pool_options = PoolOptions::new();
            if url.starts_with("sqlite") {
                pool_options.max_connections(1)
            } else {
                pool_options
            }
        });

        let pool = pool_options_with_setup(
            pool_options,
            url.starts_with("sqlite"),
            options.session_statements,
        )
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
    use object_store::ObjectStoreExt;
    use testcontainers::{core::ExecCommand, runners::AsyncRunner, ImageExt};
    use testcontainers_modules::{localstack::LocalStack, postgres::Postgres};
    use tokio::time::sleep;

    use crate::{SqlCatalog, SqlCatalogOptions};
    use iceberg_rust::object_store::store::version_hint_content;
    use sqlx::pool::PoolOptions;
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

        // Wait for bucket to be ready
        iceberg_rust::test_utils::wait_for_s3_bucket(&object_store, "s3://warehouse", None).await;

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

    /// With `write.metadata.delete-after-commit.enabled` + a small
    /// `write.metadata.previous-versions-max`, superseded metadata files are
    /// reclaimed on commit instead of accumulating forever. Uses an in-memory
    /// sqlite catalog + in-memory object store (no containers).
    #[tokio::test]
    async fn delete_after_commit_prunes_old_metadata_files() {
        use futures::TryStreamExt;
        use iceberg_rust::object_store::Bucket;
        use iceberg_rust::spec::schema::Schema;
        use iceberg_rust::spec::types::{PrimitiveType, StructField, Type};
        use iceberg_rust::table::Table;

        let catalog = Arc::new(
            SqlCatalog::new("sqlite://", "warehouse", ObjectStoreBuilder::memory())
                .await
                .unwrap(),
        );

        catalog
            .create_namespace(&Namespace::try_new(&["ns".to_string()]).unwrap(), None)
            .await
            .unwrap();

        let schema = Schema::builder()
            .with_struct_field(StructField {
                id: 1,
                name: "id".to_string(),
                required: true,
                field_type: Type::Primitive(PrimitiveType::Long),
                doc: None,
                initial_default: None,
                write_default: None,
            })
            .build()
            .unwrap();

        let mut table = Table::builder()
            .with_name("t")
            .with_location("/warehouse/ns/t")
            .with_schema(schema)
            .build(&["ns".to_string()], catalog.clone())
            .await
            .unwrap();

        // Keep only one previous metadata file and reclaim the rest.
        table
            .new_transaction(None)
            .update_properties(vec![
                (
                    "write.metadata.delete-after-commit.enabled".to_string(),
                    "true".to_string(),
                ),
                (
                    "write.metadata.previous-versions-max".to_string(),
                    "1".to_string(),
                ),
            ])
            .commit()
            .await
            .unwrap();

        // Several more commits, each writing a fresh metadata file.
        for i in 0..5 {
            table
                .new_transaction(None)
                .update_properties(vec![("marker".to_string(), i.to_string())])
                .commit()
                .await
                .unwrap();
        }

        let object_store = catalog.default_object_store(Bucket::Local);
        let metadata_files = object_store
            .list(Some(&strip_prefix("/warehouse/ns/t/metadata").into()))
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .into_iter()
            .filter(|meta| meta.location.as_ref().ends_with(".metadata.json"))
            .count();

        // Without pruning this would be one file per version (7); with
        // previous-versions-max = 1 only the current + one previous survive.
        assert!(
            metadata_files <= 2,
            "expected metadata files pruned to <= 2, found {metadata_files}"
        );
    }

    /// Two catalog instances over the same database — the shape of any
    /// multi-process deployment — must not both report success for commits
    /// built on the same base metadata. The second commit's compare-and-swap
    /// matches no rows, so its snapshot is never recorded; reporting `Ok`
    /// there silently loses the write.
    #[tokio::test]
    async fn concurrent_commit_from_a_second_catalog_reports_conflict() {
        use iceberg_rust::catalog::identifier::Identifier;
        use iceberg_rust::catalog::tabular::Tabular;
        use iceberg_rust::error::Error as IcebergError;
        use iceberg_rust::spec::schema::Schema;
        use iceberg_rust::spec::types::{PrimitiveType, StructField, Type};
        use iceberg_rust::table::Table;

        // A shared-cache in-memory database stands in for a shared catalog
        // database; the cloned builder keeps both catalogs on one object store.
        let url = "sqlite:file:conflict_test?mode=memory&cache=shared";
        let object_store = ObjectStoreBuilder::memory();

        let catalog_a = Arc::new(
            SqlCatalog::new(url, "warehouse", object_store.clone())
                .await
                .unwrap(),
        );
        let catalog_b = Arc::new(
            SqlCatalog::new(url, "warehouse", object_store)
                .await
                .unwrap(),
        );

        catalog_a
            .create_namespace(&Namespace::try_new(&["ns".to_string()]).unwrap(), None)
            .await
            .unwrap();

        let schema = Schema::builder()
            .with_struct_field(StructField {
                id: 1,
                name: "id".to_string(),
                required: true,
                field_type: Type::Primitive(PrimitiveType::Long),
                doc: None,
                initial_default: None,
                write_default: None,
            })
            .build()
            .unwrap();

        let mut table_a = Table::builder()
            .with_name("t")
            .with_location("/warehouse/ns/t")
            .with_schema(schema)
            .build(&["ns".to_string()], catalog_a.clone())
            .await
            .unwrap();

        // Catalog B loads the table, caching the same base metadata location
        // that catalog A is about to supersede.
        let identifier = Identifier::new(&["ns".to_string()], "t");
        let Tabular::Table(mut table_b) =
            catalog_b.clone().load_tabular(&identifier).await.unwrap()
        else {
            panic!("expected a table");
        };

        table_a
            .new_transaction(None)
            .update_properties(vec![("owner".to_string(), "a".to_string())])
            .commit()
            .await
            .unwrap();

        // B's commit is built on metadata A already superseded: it must fail
        // rather than report a success the catalog never recorded.
        let result = table_b
            .new_transaction(None)
            .update_properties(vec![("owner".to_string(), "b".to_string())])
            .commit()
            .await;

        assert!(
            matches!(result, Err(IcebergError::CommitConflict(_))),
            "expected a commit conflict, got {result:?}"
        );

        // And the losing commit must not have overwritten the winner.
        let Tabular::Table(reloaded) = catalog_a.clone().load_tabular(&identifier).await.unwrap()
        else {
            panic!("expected a table");
        };
        assert_eq!(
            reloaded.metadata().properties.get("owner"),
            Some(&"a".to_string())
        );
    }

    /// Per-session settings cannot be carried on the URL — sqlx's SQLite parser
    /// rejects `busy_timeout` as a query parameter — so the only way to set one
    /// is on the connection itself.
    #[tokio::test]
    async fn session_statements_are_applied_to_pooled_connections() {
        use sqlx::Row;

        let options = SqlCatalogOptions::new()
            .with_session_statements(["pragma busy_timeout = 12345".to_string()]);

        let catalog = SqlCatalog::new_with_options(
            "sqlite:file:session_statements?mode=memory&cache=shared",
            "warehouse",
            ObjectStoreBuilder::memory(),
            options,
        )
        .await
        .unwrap();

        let timeout: i64 = sqlx::query("pragma busy_timeout")
            .fetch_one(&catalog.pool)
            .await
            .unwrap()
            .get(0);
        assert_eq!(timeout, 12345);
    }

    /// The SQLite defaults must survive: a caller supplying options is adding
    /// to them, not replacing them.
    #[tokio::test]
    async fn the_default_sqlite_pragmas_survive_caller_options() {
        use sqlx::Row;

        let catalog = SqlCatalog::new_with_options(
            "sqlite:file:defaults_survive?mode=memory&cache=shared",
            "warehouse",
            ObjectStoreBuilder::memory(),
            SqlCatalogOptions::new().with_session_statements(["pragma synchronous = normal"]),
        )
        .await
        .unwrap();

        let timeout: i64 = sqlx::query("pragma busy_timeout")
            .fetch_one(&catalog.pool)
            .await
            .unwrap()
            .get(0);
        assert_eq!(timeout, 30000, "the default busy timeout must still apply");

        let synchronous: i64 = sqlx::query("pragma synchronous")
            .fetch_one(&catalog.pool)
            .await
            .unwrap()
            .get(0);
        assert_eq!(synchronous, 1, "the caller's statement must have run too");
    }

    /// The pool's size is the caller's to choose; the default leaves several
    /// writers contending on a database that may well serialize them.
    #[tokio::test]
    async fn pool_options_are_honored() {
        let options =
            SqlCatalogOptions::new().with_pool_options(PoolOptions::new().max_connections(3));

        let catalog = SqlCatalog::new_with_options(
            "sqlite:file:pool_options?mode=memory&cache=shared",
            "warehouse",
            ObjectStoreBuilder::memory(),
            options,
        )
        .await
        .unwrap();

        // Force the lazy pool to connect before asking about its size.
        catalog
            .create_namespace(&Namespace::try_new(&["ns".to_string()]).unwrap(), None)
            .await
            .unwrap();

        assert_eq!(catalog.pool.options().get_max_connections(), 3);
    }

    /// A private in-memory SQLite database (`sqlite://`) is only reachable
    /// through the connection that created it, so the single-connection cap
    /// must hold even when the caller supplies their own pool options —
    /// otherwise a second connection silently opens a distinct, empty
    /// database.
    #[tokio::test]
    async fn sqlite_private_memory_caps_connections_even_with_explicit_pool_options() {
        let options =
            SqlCatalogOptions::new().with_pool_options(PoolOptions::new().max_connections(5));

        let catalog = SqlCatalog::new_with_options(
            "sqlite://",
            "warehouse",
            ObjectStoreBuilder::memory(),
            options,
        )
        .await
        .unwrap();

        assert_eq!(catalog.pool.options().get_max_connections(), 1);

        catalog
            .create_namespace(&Namespace::try_new(&["ns".to_string()]).unwrap(), None)
            .await
            .unwrap();
        assert!(catalog
            .namespace_exists(&Namespace::try_new(&["ns".to_string()]).unwrap())
            .await
            .unwrap());
    }

    /// A catalog opened the old way must behave exactly as before, including
    /// the single-connection cap that keeps a private in-memory SQLite
    /// database from being opened twice.
    #[tokio::test]
    async fn the_default_constructor_keeps_its_previous_behavior() {
        let catalog = SqlCatalog::new("sqlite://", "warehouse", ObjectStoreBuilder::memory())
            .await
            .unwrap();

        assert_eq!(catalog.pool.options().get_max_connections(), 1);

        // And it still works end to end.
        catalog
            .create_namespace(&Namespace::try_new(&["ns".to_string()]).unwrap(), None)
            .await
            .unwrap();
        assert!(catalog
            .namespace_exists(&Namespace::try_new(&["ns".to_string()]).unwrap())
            .await
            .unwrap());
    }
}

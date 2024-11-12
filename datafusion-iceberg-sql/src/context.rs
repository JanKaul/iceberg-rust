use std::{collections::HashMap, sync::Arc};

use arrow_schema::DataType;
use datafusion_common::{config::ConfigOptions, DataFusionError, TableReference};
use datafusion_execution::FunctionRegistry;
use datafusion_expr::{
    registry::MemoryFunctionRegistry, AggregateUDF, ScalarUDF, TableSource, WindowUDF,
};
use datafusion_sql::planner::ContextProvider;
use iceberg_rust::catalog::{identifier::Identifier, CatalogList};

use crate::IcebergTableSource;

pub struct IcebergContext {
    sources: HashMap<String, Arc<dyn TableSource>>,
    config_options: ConfigOptions,
    function_registry: MemoryFunctionRegistry,
}

impl IcebergContext {
    pub async fn new(
        tables: &[(String, String, String)],
        catalogs: Arc<dyn CatalogList>,
        branch: Option<&str>,
    ) -> Result<IcebergContext, DataFusionError> {
        let mut sources = HashMap::new();

        for (catalog_name, namespace, name) in tables {
            let catalog = catalogs
                .catalog(catalog_name)
                .ok_or(DataFusionError::Internal(format!(
                    "Catalog {} was not provided",
                    &catalog_name
                )))?;

            let tabular = catalog
                .clone()
                .load_tabular(
                    &Identifier::try_new(&[namespace.to_owned(), name.to_owned()], None)
                        .map_err(|err| DataFusionError::Internal(err.to_string()))?,
                )
                .await
                .map_err(|err| DataFusionError::Internal(err.to_string()))?;

            let table_source = IcebergTableSource::new(tabular, branch);

            sources.insert(
                catalog_name.to_owned() + "." + namespace + "." + name,
                Arc::new(table_source) as Arc<dyn TableSource>,
            );
        }

        let config_options = ConfigOptions::default();

        let mut function_registry = MemoryFunctionRegistry::new();

        datafusion_functions::register_all(&mut function_registry)?;
        datafusion_functions_aggregate::register_all(&mut function_registry)?;

        Ok(IcebergContext {
            sources,
            config_options,
            function_registry,
        })
    }
}

impl ContextProvider for IcebergContext {
    fn get_table_source(
        &self,
        name: TableReference,
    ) -> Result<Arc<dyn TableSource>, DataFusionError> {
        match name {
            TableReference::Full {
                catalog,
                schema,
                table,
            } => self
                .sources
                .get(&(catalog.to_string() + "." + &schema + "." + &table))
                .cloned()
                .ok_or(DataFusionError::Internal(format!(
                    "Couldn't resolve table reference {}.{}",
                    &schema, &table
                ))),
            _ => Err(DataFusionError::Internal(
                "Only partial table refence supported".to_string(),
            )),
        }
    }
    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
        self.function_registry.udf(name).ok()
    }
    fn get_variable_type(&self, _variable_names: &[String]) -> Option<DataType> {
        None
    }
    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>> {
        self.function_registry.udaf(name).ok()
    }
    fn get_window_meta(&self, name: &str) -> Option<Arc<WindowUDF>> {
        self.function_registry.udwf(name).ok()
    }
    fn options(&self) -> &ConfigOptions {
        &self.config_options
    }

    fn udf_names(&self) -> Vec<String> {
        self.function_registry.udfs().into_iter().collect()
    }

    fn udaf_names(&self) -> Vec<String> {
        Vec::new()
    }

    fn udwf_names(&self) -> Vec<String> {
        Vec::new()
    }
}

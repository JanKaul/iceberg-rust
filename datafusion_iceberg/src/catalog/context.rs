use std::{collections::HashMap, sync::Arc};

use datafusion::{
    config::ConfigOptions,
    error::DataFusionError,
    logical_expr::TableSource,
    sql::{planner::ContextProvider, TableReference},
};
use iceberg_rust::catalog::{identifier::Identifier, Catalog};

use crate::{error::Error, DataFusionTable};

pub struct IcebergContext {
    sources: HashMap<String, Arc<dyn TableSource>>,
    config_options: ConfigOptions,
}

impl IcebergContext {
    pub async fn new(
        tables: Vec<String>,
        catalog: Arc<dyn Catalog>,
    ) -> Result<IcebergContext, Error> {
        let mut sources = HashMap::new();
        for table in tables {
            let identifier = Identifier::parse(&table)?;
            let relation: DataFusionTable = catalog.clone().load_table(&identifier).await?.into();
            sources.insert(
                table,
                Arc::new(relation.into_table_source()) as Arc<dyn TableSource>,
            );
        }
        let config_options = ConfigOptions::default();
        Ok(IcebergContext {
            sources,
            config_options,
        })
    }
}

impl ContextProvider for IcebergContext {
    fn get_table_source(
        &self,
        name: datafusion::sql::TableReference,
    ) -> datafusion::error::Result<Arc<dyn TableSource>> {
        match name {
            TableReference::Partial { schema, table } => self
                .sources
                .get(&(schema.to_string() + "." + &table))
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
    fn get_function_meta(&self, _name: &str) -> Option<Arc<datafusion::logical_expr::ScalarUDF>> {
        None
    }
    fn get_variable_type(
        &self,
        _variable_names: &[String],
    ) -> Option<datafusion::arrow::datatypes::DataType> {
        None
    }
    fn get_aggregate_meta(
        &self,
        _name: &str,
    ) -> Option<Arc<datafusion::logical_expr::AggregateUDF>> {
        None
    }
    fn get_window_meta(&self, _name: &str) -> Option<Arc<datafusion::logical_expr::WindowUDF>> {
        None
    }
    fn options(&self) -> &ConfigOptions {
        &self.config_options
    }
}

use std::{any::Any, sync::Arc};

use arrow_schema::SchemaRef;
use datafusion_expr::TableSource;
use iceberg_rust::catalog::tabular::Tabular;

pub mod context;
pub mod schema;

pub struct IcebergTableSource {
    tabular: Tabular,
    branch: Option<String>,
}

impl IcebergTableSource {
    pub fn new(tabular: Tabular, branch: Option<&str>) -> Self {
        IcebergTableSource {
            tabular,
            branch: branch.map(ToOwned::to_owned),
        }
    }
}

impl TableSource for IcebergTableSource {
    fn as_any(&self) -> &dyn Any {
        &self.tabular
    }
    fn schema(&self) -> SchemaRef {
        match &self.tabular {
            Tabular::Table(table) => {
                let schema = table.current_schema(self.branch.as_deref()).unwrap();
                Arc::new((&schema.fields).try_into().unwrap())
            }
            Tabular::View(view) => {
                let schema = view.current_schema(self.branch.as_deref()).unwrap();
                Arc::new((&schema.fields).try_into().unwrap())
            }
            Tabular::MaterializedView(matview) => {
                let schema = matview.current_schema(self.branch.as_deref()).unwrap();
                Arc::new((&schema.fields).try_into().unwrap())
            }
        }
    }
}

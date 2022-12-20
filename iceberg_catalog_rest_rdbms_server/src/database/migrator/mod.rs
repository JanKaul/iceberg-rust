use sea_orm_migration::prelude::*;

mod m20221026_000001_create_catalog_table;
mod m20221026_000002_create_namespace_table;
mod m20221026_000003_create_table_table;
pub struct Migrator;

#[async_trait::async_trait]
impl MigratorTrait for Migrator {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        vec![
            Box::new(m20221026_000001_create_catalog_table::Migration),
            Box::new(m20221026_000002_create_namespace_table::Migration),
            Box::new(m20221026_000003_create_table_table::Migration),
        ]
    }
}

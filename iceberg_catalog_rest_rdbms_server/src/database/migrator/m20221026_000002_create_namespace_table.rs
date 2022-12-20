use sea_orm_migration::prelude::*;

use super::m20221026_000001_create_catalog_table::Catalog;

pub struct Migration;

impl MigrationName for Migration {
    fn name(&self) -> &str {
        "m20221026_000002_create_namespace_table"
    }
}

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    // Define how to apply this migration: Create the Chef table.
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(Namespace::Table)
                    .col(
                        ColumnDef::new(Namespace::Id)
                            .integer()
                            .not_null()
                            .auto_increment()
                            .primary_key(),
                    )
                    .col(
                        ColumnDef::new(Namespace::Name)
                            .string()
                            .unique_key()
                            .not_null(),
                    )
                    .col(ColumnDef::new(Namespace::CatalogId).integer())
                    .foreign_key(
                        ForeignKey::create()
                            .name("fk-namespace-catalog_id")
                            .from(Namespace::Table, Namespace::CatalogId)
                            .to(Catalog::Table, Catalog::Id),
                    )
                    .to_owned(),
            )
            .await
    }

    // Define how to rollback this migration: Drop the Chef table.
    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table(Namespace::Table).to_owned())
            .await
    }
}

#[derive(Iden)]
pub enum Namespace {
    Table,
    Id,
    Name,
    CatalogId,
}

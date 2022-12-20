use sea_orm_migration::prelude::*;

use super::m20221026_000002_create_namespace_table::Namespace;

pub struct Migration;

impl MigrationName for Migration {
    fn name(&self) -> &str {
        "m20221026_000003_create_table_table"
    }
}

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    // Define how to apply this migration: Create the Chef table.
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(IcebergTable::Table)
                    .col(
                        ColumnDef::new(IcebergTable::Id)
                            .integer()
                            .not_null()
                            .auto_increment()
                            .primary_key(),
                    )
                    .col(
                        ColumnDef::new(IcebergTable::Name)
                            .string()
                            .unique_key()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(IcebergTable::MetadataLocation)
                            .string()
                            .not_null(),
                    )
                    .col(ColumnDef::new(IcebergTable::PreviousMetadataLocation).string())
                    .col(
                        ColumnDef::new(IcebergTable::NamespaceId)
                            .integer()
                            .not_null(),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .name("fk-table-namespace_id")
                            .from(IcebergTable::Table, IcebergTable::NamespaceId)
                            .to(Namespace::Table, Namespace::Id),
                    )
                    .to_owned(),
            )
            .await
    }

    // Define how to rollback this migration: Drop the Chef table.
    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table(IcebergTable::Table).to_owned())
            .await
    }
}

#[derive(Iden)]
pub enum IcebergTable {
    Table,
    Id,
    Name,
    NamespaceId,
    MetadataLocation,
    PreviousMetadataLocation,
}

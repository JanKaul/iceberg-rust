use sea_orm::{ConnectionTrait, DatabaseConnection};
use sea_orm::{Database, DbBackend, DbErr, Statement};
use sea_orm_migration::prelude::*;

pub mod entities;
mod migrator;

pub(crate) async fn run(connection: &str, db_name: &str) -> Result<DatabaseConnection, DbErr> {
    let db = Database::connect(connection.to_owned() + "/postgres").await?;

    let db = match db.get_database_backend() {
        DbBackend::MySql => {
            db.execute(Statement::from_string(
                db.get_database_backend(),
                format!("CREATE DATABASE IF NOT EXISTS `{}`;", db_name),
            ))
            .await?;

            db.execute(Statement::from_string(
                db.get_database_backend(),
                format!("SET GLOBAL TRANSACTION ISOLATION LEVEL SERIALIZABLE"),
            ))
            .await?;

            let url = format!("{}/{}", connection, db_name);
            Database::connect(&url).await?
        }
        DbBackend::Postgres => {
            let _ = db
                .execute(Statement::from_string(
                    db.get_database_backend(),
                    format!("CREATE DATABASE {}", db_name),
                ))
                .await;

            db.execute(Statement::from_string(
                db.get_database_backend(),
                format!(
                    "ALTER DATABASE {} SET DEFAULT_TRANSACTION_ISOLATION TO SERIALIZABLE",
                    db_name
                ),
            ))
            .await?;

            let url = format!("{}/{}", connection, db_name);
            Database::connect(&url).await?
        }
        DbBackend::Sqlite => db,
    };

    let schema_manager = SchemaManager::new(&db); // To investigate the schema

    if !schema_manager.has_table("catalog").await? {
        migrator::Migrator::refresh(&db).await?;
    }

    Ok(db)
}

use aws_sdk_glue::{
    config::http::HttpResponse,
    operation::{
        create_database::CreateDatabaseError, create_table::CreateTableError,
        delete_table::DeleteTableError, get_databases::GetDatabasesError, get_table::GetTableError,
        get_tables::GetTablesError, update_table::UpdateTableError,
    },
};
use iceberg_rust::error::Error as IcebergError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    ParseError(#[from] url::ParseError),
    #[error(transparent)]
    GetDatabaseError(#[from] aws_sdk_glue::error::SdkError<GetDatabasesError, HttpResponse>),
    #[error(transparent)]
    CreateDatabaseError(#[from] aws_sdk_glue::error::SdkError<CreateDatabaseError, HttpResponse>),
    #[error(transparent)]
    CreateTableError(#[from] aws_sdk_glue::error::SdkError<CreateTableError, HttpResponse>),
    #[error(transparent)]
    UpdateTableError(#[from] aws_sdk_glue::error::SdkError<UpdateTableError, HttpResponse>),
    #[error(transparent)]
    GetTableError(#[from] aws_sdk_glue::error::SdkError<GetTableError, HttpResponse>),
    #[error(transparent)]
    DeleteTableError(#[from] aws_sdk_glue::error::SdkError<DeleteTableError, HttpResponse>),
    #[error(transparent)]
    GetTablesError(#[from] aws_sdk_glue::error::SdkError<GetTablesError, HttpResponse>),
    #[error(transparent)]
    BuildError(#[from] aws_sdk_glue::error::BuildError),
}

impl From<Error> for IcebergError {
    fn from(value: Error) -> Self {
        IcebergError::External(Box::new(value))
    }
}

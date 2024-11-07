use aws_sdk_glue::{
    config::http::HttpResponse,
    operation::{create_table::CreateTableError, get_databases::GetDatabasesError},
};
use iceberg_rust::error::Error as IcebergError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("{0}")]
    Text(String),
    #[error(transparent)]
    ParseError(#[from] url::ParseError),
    #[error(transparent)]
    GetDatabaseError(#[from] aws_sdk_glue::error::SdkError<GetDatabasesError, HttpResponse>),
    #[error(transparent)]
    CreateTableError(#[from] aws_sdk_glue::error::SdkError<CreateTableError, HttpResponse>),
    #[error(transparent)]
    BuildError(#[from] aws_sdk_glue::error::BuildError),
}

impl From<Error> for IcebergError {
    fn from(value: Error) -> Self {
        IcebergError::InvalidFormat(value.to_string())
    }
}

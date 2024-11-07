use aws_sdk_glue::{
    config::http::HttpResponse, error::SdkError, operation::get_databases::GetDatabasesError,
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
    GetDatabaseError(#[from] SdkError<GetDatabasesError, HttpResponse>),
}

impl From<Error> for IcebergError {
    fn from(value: Error) -> Self {
        IcebergError::InvalidFormat(value.to_string())
    }
}

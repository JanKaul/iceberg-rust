use aws_sdk_s3tables::{
    config::http::HttpResponse,
    error::SdkError,
    operation::{
        create_namespace::CreateNamespaceError, create_table::CreateTableError,
        delete_table::DeleteTableError, get_table::GetTableError,
        list_namespaces::ListNamespacesError, list_tables::ListTablesError,
        update_table_metadata_location::UpdateTableMetadataLocationError,
    },
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
    CreateNamespace(#[from] SdkError<CreateNamespaceError, HttpResponse>),
    #[error(transparent)]
    ListTables(#[from] SdkError<ListTablesError, HttpResponse>),
    #[error(transparent)]
    ListNamespaces(#[from] SdkError<ListNamespacesError, HttpResponse>),
    #[error(transparent)]
    GetTable(#[from] SdkError<GetTableError, HttpResponse>),
    #[error(transparent)]
    DeletaTable(#[from] SdkError<DeleteTableError, HttpResponse>),
    #[error(transparent)]
    CreateTable(#[from] SdkError<CreateTableError, HttpResponse>),
    #[error(transparent)]
    UpdateTableMetadataLocation(#[from] SdkError<UpdateTableMetadataLocationError, HttpResponse>),
}

impl From<Error> for IcebergError {
    fn from(value: Error) -> Self {
        IcebergError::External(Box::new(value))
    }
}

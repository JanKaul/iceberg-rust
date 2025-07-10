use aws_sdk_s3tables::{
    config::http::HttpResponse,
    error::SdkError,
    operation::{
        create_namespace::CreateNamespaceError, create_table::CreateTableError,
        delete_namespace::DeleteNamespaceError, delete_table::DeleteTableError,
        get_namespace::GetNamespaceError, get_table::GetTableError,
        get_table_metadata_location::GetTableMetadataLocationError,
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
    DeleteNamespace(#[from] SdkError<DeleteNamespaceError, HttpResponse>),
    #[error(transparent)]
    GetNamespace(#[from] SdkError<GetNamespaceError, HttpResponse>),
    #[error(transparent)]
    ListTables(#[from] SdkError<ListTablesError, HttpResponse>),
    #[error(transparent)]
    ListNamespaces(#[from] SdkError<ListNamespacesError, HttpResponse>),
    #[error(transparent)]
    GetTable(#[from] SdkError<GetTableError, HttpResponse>),
    #[error(transparent)]
    DeleteTable(#[from] SdkError<DeleteTableError, HttpResponse>),
    #[error(transparent)]
    SdkError(#[from] SdkError<GetTableMetadataLocationError, HttpResponse>),
    #[error(transparent)]
    GetTableMetadataLocation(#[from] GetTableMetadataLocationError),
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

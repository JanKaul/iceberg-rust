/*!
Error type for iceberg
*/

use thiserror::Error;

#[derive(Error, Debug)]
/// Iceberg error
pub enum Error {
    /// Invalid format
    #[error("{0} doesn't have the right format")]
    InvalidFormat(String),
    /// Type error
    #[error("Value {0} doesn't have the {1} type.")]
    Type(String, String),
    /// Schema error
    #[error("Column {0} not in schema {1}.")]
    Schema(String, String),
    /// Conversion error
    #[error("Failed to convert {0} to {1}.")]
    Conversion(String, String),
    /// Not found
    #[error("{0} {1} not found.")]
    NotFound(String, String),
    /// Not supported
    #[error("Feature {0} is not supported.")]
    NotSupported(String),
    /// Avro error
    #[error(transparent)]
    Avro(#[from] apache_avro::Error),
    /// Serde json
    #[error(transparent)]
    JSONSerde(#[from] serde_json::Error),
    /// Chrono parse
    #[error(transparent)]
    Chrono(#[from] chrono::ParseError),
    /// Chrono parse
    #[error(transparent)]
    Uuid(#[from] uuid::Error),
    /// Io error
    #[error(transparent)]
    IO(#[from] std::io::Error),
    /// Try from slice error
    #[error(transparent)]
    TryFromSlice(#[from] std::array::TryFromSliceError),
    /// Try from int error
    #[error(transparent)]
    TryFromInt(#[from] std::num::TryFromIntError),
    /// Utf8 error
    #[error(transparent)]
    UTF8(#[from] std::str::Utf8Error),
    /// from utf8 error
    #[error(transparent)]
    FromUTF8(#[from] std::string::FromUtf8Error),
    /// parse int error
    #[error(transparent)]
    ParseInt(#[from] std::num::ParseIntError),
    /// table metadata builder
    #[error(transparent)]
    TableMetadataBuilder(#[from] crate::spec::table_metadata::TableMetadataBuilderError),
    /// view metadata builder
    #[error(transparent)]
    ViewMetadataBuilder(#[from] crate::spec::view_metadata::GeneralViewMetadataBuilderError),
    /// version builder
    #[error(transparent)]
    VersionBuilder(#[from] crate::spec::view_metadata::VersionBuilderError),
    /// manifest builder
    #[error(transparent)]
    ManifestEntryBuilder(#[from] crate::spec::manifest::ManifestEntryBuilderError),
    /// datafile builder
    #[error(transparent)]
    DatafileBuilder(#[from] crate::spec::manifest::DataFileBuilderError),
    /// snapshot builder
    #[error(transparent)]
    SnapshotBuilder(#[from] crate::spec::snapshot::SnapshotBuilderError),
    /// schema builder
    #[error(transparent)]
    SchemaBuilder(#[from] crate::spec::schema::SchemaBuilderError),
    /// structype builder
    #[error(transparent)]
    StructTypeBuilder(#[from] crate::spec::types::StructTypeBuilderError),
    /// partition spec builder
    #[error(transparent)]
    PartitionSpec(#[from] crate::spec::partition::PartitionSpecBuilderError),
}

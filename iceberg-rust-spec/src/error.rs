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
    /// Conversion error
    #[error("Failed to convert {0} to {1}.")]
    Conversion(String, String),
    /// Not found
    #[error("{0} not found.")]
    NotFound(String),
    /// Not supported
    #[error("Feature {0} is not supported.")]
    NotSupported(String),
    /// Avro error
    #[error(transparent)]
    Avro(Box<apache_avro::Error>),
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
    /// derive builder
    #[error(transparent)]
    DeriveBuilder(#[from] derive_builder::UninitializedFieldError),
}

impl From<apache_avro::Error> for Error {
    fn from(err: apache_avro::Error) -> Self {
        Error::Avro(Box::new(err))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_invalid_format_display_includes_argument() {
        let err = Error::InvalidFormat("schema".to_string());
        assert_eq!(err.to_string(), "schema doesn't have the right format");
    }

    #[test]
    fn test_type_display_includes_both_arguments() {
        let err = Error::Type("\"foo\"".to_string(), "int".to_string());
        assert_eq!(err.to_string(), "Value \"foo\" doesn't have the int type.");
    }

    #[test]
    fn test_conversion_display_uses_to_from_phrasing() {
        let err = Error::Conversion("json number".to_string(), "long".to_string());
        assert_eq!(err.to_string(), "Failed to convert json number to long.");
    }

    #[test]
    fn test_not_found_display_includes_subject() {
        let err = Error::NotFound("schema field 12".to_string());
        assert_eq!(err.to_string(), "schema field 12 not found.");
    }

    #[test]
    fn test_not_supported_display_calls_out_feature_name() {
        let err = Error::NotSupported("Variant decoding".to_string());
        assert_eq!(
            err.to_string(),
            "Feature Variant decoding is not supported."
        );
    }

    #[test]
    fn test_from_apache_avro_boxes_into_avro_variant() {
        // Produce a real apache_avro::Error by handing the parser an
        // intentionally malformed schema.
        let avro_err = apache_avro::Schema::parse_str("{ not json }").unwrap_err();
        let err: Error = avro_err.into();
        assert!(matches!(err, Error::Avro(_)));
    }

    #[test]
    fn test_from_serde_json_lands_in_jsonserde_variant() {
        let serde_err = serde_json::from_str::<i32>("not-json").unwrap_err();
        let err: Error = serde_err.into();
        assert!(matches!(err, Error::JSONSerde(_)));
    }

    #[test]
    fn test_from_parse_int_lands_in_parseint_variant() {
        let parse_err = "12abc".parse::<i32>().unwrap_err();
        let err: Error = parse_err.into();
        assert!(matches!(err, Error::ParseInt(_)));
    }

    #[test]
    fn test_from_uuid_lands_in_uuid_variant() {
        let uuid_err = uuid::Uuid::parse_str("not-a-uuid").unwrap_err();
        let err: Error = uuid_err.into();
        assert!(matches!(err, Error::Uuid(_)));
    }

    #[test]
    fn test_transparent_variants_display_passes_through_inner_message() {
        // The transparent wrapper uses the inner error's Display directly,
        // so the message must equal the original serde_json error's render.
        let serde_err = serde_json::from_str::<i32>("not-json").unwrap_err();
        let inner_message = serde_err.to_string();
        let err: Error = serde_err.into();
        assert_eq!(err.to_string(), inner_message);
    }
}

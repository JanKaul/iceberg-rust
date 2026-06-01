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
    // -----------------------------------------------------------------------
    // Placeholders for `runSafely`-style suppressed-exception propagation.
    //
    // Rust has no analog: `?` short-circuits on the first error;
    // `std::error::Error::source()` exposes a single-link cause chain but no
    // *list* of cleanup-time exceptions; `Drop` can act as a finally analog
    // but panicking during unwind is UB. The 4 placeholders below cover the
    // four `runSafely` overloads (varying numbers of declared checked
    // exception types).
    // -----------------------------------------------------------------------

    #[test]
    #[ignore = "no exception_util::run_safely(block, catch, finally) suppressed-exception propagation"]
    fn test_run_safely_attaches_finally_exception_as_suppressed_when_primary_succeeds() {
        unimplemented!("run_safely 1-arg overload");
    }

    #[test]
    #[ignore = "no exception_util::run_safely 2-exception overload"]
    fn test_run_safely_attaches_catch_and_finally_exceptions_two_declared_types() {
        unimplemented!("run_safely 2-arg overload");
    }

    #[test]
    #[ignore = "no exception_util::run_safely 3-exception overload"]
    fn test_run_safely_attaches_catch_and_finally_exceptions_three_declared_types() {
        unimplemented!("run_safely 3-arg overload");
    }

    #[test]
    #[ignore = "no exception_util::run_safely runtime-exception variant"]
    fn test_run_safely_attaches_suppressed_exceptions_with_unchecked_runtime_errors() {
        unimplemented!("run_safely runtime overload");
    }
}

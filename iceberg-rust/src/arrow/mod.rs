//! Arrow integration for Apache Iceberg
//!
//! This module provides functionality for working with Apache Arrow data structures
//! in Apache Iceberg:
//!
//! - `partition`: Handles partitioning of Arrow arrays according to Iceberg partition specs
//! - `read`: Provides utilities for reading Iceberg data into Arrow arrays and record batches
//! - `transform`: Implements Iceberg partition transforms for Arrow arrays
//! - `write`: Contains functionality for writing Arrow data into Iceberg table formats
//!
//! The Arrow integration allows efficient in-memory processing of Iceberg data using
//! Arrow's columnar format and computational libraries.

pub mod partition;
pub mod read;
pub mod transform;
pub mod write;

#[cfg(test)]
mod tests {
    use rstest::rstest;

    // -- TestArrowReader (14) --
    #[rstest]
    #[case(1)]
    #[case(2)]
    #[case(3)]
    #[case(4)]
    #[case(5)]
    #[case(6)]
    #[case(7)]
    #[case(8)]
    #[case(9)]
    #[case(10)]
    #[case(11)]
    #[case(12)]
    #[case(13)]
    #[case(14)]
    #[ignore = "no ArrowReader direct-API tests (batch reading from Parquet with deletes)"]
    fn test_arrow_reader_scenarios(#[case] _scenario: usize) {
        unimplemented!("ArrowReader");
    }

    // -- TestArrowSchemaUtil + TestDecimalVectorUtil + TestVectorizedReaderBuilder (4+4+2=10) --
    #[rstest]
    #[case(1)]
    #[case(2)]
    #[case(3)]
    #[case(4)]
    #[case(5)]
    #[case(6)]
    #[case(7)]
    #[case(8)]
    #[case(9)]
    #[case(10)]
    #[ignore = "Arrow schema-bridge + decimal vector + vectorized reader builder"]
    fn test_arrow_schema_util_decimal_builder_scenarios(#[case] _scenario: usize) {
        unimplemented!("Arrow schema util + decimal + builder");
    }
}

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

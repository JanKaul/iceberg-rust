//! Apache Iceberg specification implementation in Rust
//!
//! This crate provides the core data structures and implementations for working with
//! Apache Iceberg tables in Rust. It includes:
//!
//! - Complete implementation of the Apache Iceberg table format specification
//! - Type-safe representations of schemas, partitioning, sorting and other metadata
//! - Serialization/deserialization of all Iceberg metadata formats
//! - Arrow integration for reading and writing Iceberg data
//! - Utility functions for working with Iceberg tables
//!
//! The crate is organized into several modules:
//!
//! - `spec`: Core specification types and implementations
//! - `arrow`: Integration with Apache Arrow
//! - `error`: Error types and handling
//! - `util`: Common utility functions
//!
pub mod arrow;
pub mod error;
pub mod spec;
pub mod util;

pub use spec::*;

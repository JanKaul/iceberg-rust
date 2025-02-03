//! Core Apache Iceberg specification types and implementations
//!
//! This module contains the core data structures and implementations that make up
//! the Apache Iceberg table format specification. Key components include:
//!
//! - Table metadata and schema definitions
//! - Manifest and snapshot management
//! - Partition specifications
//! - Sort orders
//! - Data types and values
//! - View and materialized view metadata
//!
//! Each submodule implements a specific part of the specification, providing
//! serialization/deserialization and validation logic.

pub mod identifier;
pub mod manifest;
pub mod manifest_list;
pub mod materialized_view_metadata;
pub mod namespace;
pub mod partition;
pub mod schema;
pub mod snapshot;
pub mod sort;
pub mod table_metadata;
pub mod tabular;
pub mod types;
pub mod values;
pub mod view_metadata;

#![deny(missing_docs)]
//! Apache Iceberg implementation in Rust
//!
//! This crate provides a native Rust implementation of [Apache Iceberg](https://iceberg.apache.org/),
//! a table format for large analytic datasets. Iceberg manages large collections of files as tables,
//! while providing atomic updates and concurrent writes.
//!
//! # Features
//!
//! * Table operations (create, read, update, delete)
//! * Schema evolution
//! * Hidden partitioning
//! * Time travel and snapshot isolation
//! * View and materialized view support
//! * Multiple catalog implementations (REST, AWS Glue, File-based)
//! * Table maintenance operations (snapshot expiration, orphan file cleanup)
//!
//! # Components
//!
//! The main components of this crate are:
//!
//! * [`table`] - Core table operations and management
//! * [`catalog`] - Catalog implementations for metadata storage
//! * [`arrow`] - Integration with Apache Arrow
//! * [`view`] - View and materialized view support
//! * [`error`] - Error types and handling
//!
//! # Example
//!
//! ```rust,no_run
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! use iceberg_rust::table::Table;
//! use iceberg_rust::catalog::Catalog;
//!
//! // Create a new table
//! let mut table = Table::builder()
//!     .with_name("example_table")
//!     .with_schema(schema)
//!     .build()
//!     .await?;
//!
//! // Start a transaction
//! table.new_transaction(None)
//!     .update_schema(new_schema)
//!     .commit()
//!     .await?;
//!
//! // Expire old snapshots for maintenance
//! let result = table.expire_snapshots()
//!     .expire_older_than(chrono::Utc::now().timestamp_millis() - 30 * 24 * 60 * 60 * 1000)
//!     .retain_last(10)
//!     .execute()
//!     .await?;
//!     
//! println!("Expired {} snapshots", result.expired_snapshot_ids.len());
//! # Ok(())
//! # }
//! ```

pub mod arrow;
pub mod catalog;
pub mod error;
pub mod file_format;
pub mod materialized_view;
pub mod object_store;
pub mod spec;
pub mod sql;
pub mod table;
pub(crate) mod util;
pub mod view;

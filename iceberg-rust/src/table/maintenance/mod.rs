//! Maintenance module for table cleanup and optimization operations
//!
//! This module provides maintenance operations for Iceberg tables including:
//! * Snapshot expiration - removing old snapshots and their associated files
//! * Orphaned file cleanup - removing data files no longer referenced by any snapshot
//! * Metadata compaction - optimizing table metadata size

pub mod expire_snapshots;

pub use expire_snapshots::{ExpireSnapshots, ExpireSnapshotsResult};

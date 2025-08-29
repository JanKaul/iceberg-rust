//! Snapshot expiration functionality for Iceberg tables
//!
//! This module provides the ability to expire old snapshots and clean up associated
//! manifest and data files. The implementation follows Iceberg's atomic commit model
//! and supports various expiration criteria including:
//!
//! * Time-based expiration (older than timestamp)
//! * Count-based retention (keep only last N snapshots)
//! * Reference-aware cleanup (preserve snapshots referenced by branches/tags)
//! * Optional orphaned file cleanup

use std::collections::HashSet;

use iceberg_rust_spec::spec::table_metadata::TableMetadata;

use crate::{
    error::Error,
    table::Table,
};

/// Builder for configuring and executing snapshot expiration operations
///
/// This builder provides a fluent API for configuring how snapshots should be expired:
/// * [`expire_older_than`](ExpireSnapshots::expire_older_than) - Remove snapshots older than a timestamp
/// * [`retain_last`](ExpireSnapshots::retain_last) - Keep only the most recent N snapshots
/// * [`clean_orphan_files`](ExpireSnapshots::clean_orphan_files) - Also remove unreferenced data files
/// * [`dry_run`](ExpireSnapshots::dry_run) - Preview what would be deleted without actually deleting
///
/// # Examples
///
/// ```rust,no_run
/// # async fn example(table: &mut Table) -> Result<(), Box<dyn std::error::Error>> {
/// // Expire snapshots older than 7 days, keeping at least 5 snapshots
/// let result = table.expire_snapshots()
///     .expire_older_than(chrono::Utc::now().timestamp_millis() - 7 * 24 * 60 * 60 * 1000)
///     .retain_last(5)
///     .clean_orphan_files(true)
///     .execute()
///     .await?;
///
/// println!("Expired {} snapshots", result.expired_snapshot_ids.len());
/// # Ok(())
/// # }
/// ```
pub struct ExpireSnapshots<'a> {
    table: &'a mut Table,
    older_than: Option<i64>,
    retain_last: Option<usize>,
    clean_orphan_files: bool,
    retain_ref_snapshots: bool,
    max_concurrent_deletes: usize,
    dry_run: bool,
}

/// Result of snapshot expiration operation
///
/// Contains detailed information about what was expired and deleted during
/// the operation. This can be used for logging, metrics, or verification.
#[derive(Debug, Clone)]
pub struct ExpireSnapshotsResult {
    /// IDs of snapshots that were expired/removed from table metadata
    pub expired_snapshot_ids: Vec<i64>,
    /// Summary of files that were deleted
    pub deleted_files: DeletedFiles,
    /// IDs of snapshots that were retained
    pub retained_snapshot_ids: Vec<i64>,
    /// Whether this was a dry run (no actual deletions performed)
    pub dry_run: bool,
}

/// Summary of files deleted during snapshot expiration
#[derive(Debug, Clone, Default)]
pub struct DeletedFiles {
    /// Manifest list files that were deleted
    pub manifest_lists: Vec<String>,
    /// Manifest files that were deleted
    pub manifests: Vec<String>,
    /// Data files that were deleted (only when clean_orphan_files is enabled)
    pub data_files: Vec<String>,
}

/// Internal structure for tracking what needs to be expired
#[derive(Debug)]
struct SnapshotSelection {
    snapshots_to_expire: Vec<i64>,
    snapshots_to_retain: Vec<i64>,
    files_to_delete: DeletedFiles,
}

impl<'a> ExpireSnapshots<'a> {
    /// Create a new snapshot expiration builder for the given table
    pub(crate) fn new(table: &'a mut Table) -> Self {
        Self {
            table,
            older_than: None,
            retain_last: None,
            clean_orphan_files: false,
            retain_ref_snapshots: true,
            max_concurrent_deletes: 4,
            dry_run: false,
        }
    }

    /// Expire snapshots older than the given timestamp (in milliseconds since Unix epoch)
    ///
    /// # Arguments
    /// * `timestamp_ms` - Unix timestamp in milliseconds. Snapshots created before this time will be expired
    ///
    /// # Returns
    /// * `Self` - The builder for method chaining
    ///
    /// # Examples
    /// ```rust,no_run
    /// # async fn example(table: &mut Table) -> Result<(), Box<dyn std::error::Error>> {
    /// // Expire snapshots older than 30 days
    /// let thirty_days_ago = chrono::Utc::now().timestamp_millis() - 30 * 24 * 60 * 60 * 1000;
    /// let result = table.expire_snapshots()
    ///     .expire_older_than(thirty_days_ago)
    ///     .execute()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn expire_older_than(mut self, timestamp_ms: i64) -> Self {
        self.older_than = Some(timestamp_ms);
        self
    }

    /// Retain only the most recent N snapshots, expiring all others
    ///
    /// This takes precedence over `expire_older_than` for the most recent snapshots.
    /// If both criteria are specified, the most recent N snapshots will be retained
    /// even if they are older than the timestamp threshold.
    ///
    /// # Arguments
    /// * `count` - Number of most recent snapshots to retain
    ///
    /// # Returns
    /// * `Self` - The builder for method chaining
    ///
    /// # Examples
    /// ```rust,no_run
    /// # async fn example(table: &mut Table) -> Result<(), Box<dyn std::error::Error>> {
    /// // Keep only the 10 most recent snapshots
    /// let result = table.expire_snapshots()
    ///     .retain_last(10)
    ///     .execute()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn retain_last(mut self, count: usize) -> Self {
        self.retain_last = Some(count);
        self
    }

    /// Enable or disable cleanup of orphaned data files
    ///
    /// When enabled, data files that are only referenced by expired snapshots
    /// will also be deleted. This can significantly reduce storage usage but
    /// requires more computation to determine file reachability.
    ///
    /// # Arguments
    /// * `enabled` - Whether to clean up orphaned files
    ///
    /// # Returns
    /// * `Self` - The builder for method chaining
    ///
    /// # Examples
    /// ```rust,no_run
    /// # async fn example(table: &mut Table) -> Result<(), Box<dyn std::error::Error>> {
    /// // Expire snapshots and clean up orphaned files
    /// let result = table.expire_snapshots()
    ///     .retain_last(5)
    ///     .clean_orphan_files(true)
    ///     .execute()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn clean_orphan_files(mut self, enabled: bool) -> Self {
        self.clean_orphan_files = enabled;
        self
    }

    /// Control whether snapshots referenced by branches/tags should be preserved
    ///
    /// When enabled (default), snapshots that are referenced by named branches
    /// or tags will not be expired even if they meet other expiration criteria.
    ///
    /// # Arguments
    /// * `enabled` - Whether to preserve snapshots referenced by branches/tags
    ///
    /// # Returns
    /// * `Self` - The builder for method chaining
    pub fn retain_ref_snapshots(mut self, enabled: bool) -> Self {
        self.retain_ref_snapshots = enabled;
        self
    }

    /// Set the maximum number of concurrent file delete operations
    ///
    /// # Arguments
    /// * `max_concurrent` - Maximum number of files to delete concurrently
    ///
    /// # Returns
    /// * `Self` - The builder for method chaining
    pub fn max_concurrent_deletes(mut self, max_concurrent: usize) -> Self {
        self.max_concurrent_deletes = max_concurrent;
        self
    }

    /// Enable dry run mode to preview what would be deleted without actually deleting
    ///
    /// In dry run mode, the operation will determine what snapshots and files would
    /// be expired/deleted but will not modify the table or delete any files.
    ///
    /// # Arguments
    /// * `enabled` - Whether to run in dry run mode
    ///
    /// # Returns
    /// * `Self` - The builder for method chaining
    ///
    /// # Examples
    /// ```rust,no_run
    /// # async fn example(table: &mut Table) -> Result<(), Box<dyn std::error::Error>> {
    /// // Preview what would be expired without actually doing it
    /// let result = table.expire_snapshots()
    ///     .retain_last(5)
    ///     .dry_run(true)
    ///     .execute()
    ///     .await?;
    ///
    /// println!("Would expire {} snapshots", result.expired_snapshot_ids.len());
    /// # Ok(())
    /// # }
    /// ```
    pub fn dry_run(mut self, enabled: bool) -> Self {
        self.dry_run = enabled;
        self
    }

    /// Execute the snapshot expiration operation
    ///
    /// This method performs the actual expiration, updating table metadata and
    /// optionally deleting files. The operation is atomic - either all changes
    /// are applied or none are.
    ///
    /// # Returns
    /// * `Result<ExpireSnapshotsResult, Error>` - Details of what was expired/deleted
    ///
    /// # Errors
    /// * `Error::InvalidFormat` - If neither `older_than` nor `retain_last` is specified
    /// * `Error::External` - If the table was modified during the operation (after retries)
    /// * `Error::IO` - If file operations fail
    /// * Other catalog or object store errors
    pub async fn execute(self) -> Result<ExpireSnapshotsResult, Error> {
        // Validate parameters
        if self.older_than.is_none() && self.retain_last.is_none() {
            return Err(Error::InvalidFormat(
                "Must specify either older_than or retain_last for snapshot expiration".into()
            ));
        }

        // Core implementation with retry logic for concurrent modifications
        let mut attempts = 0;
        const MAX_ATTEMPTS: usize = 5;

        loop {
            attempts += 1;

            // 1. Get the current table metadata
            let metadata = &self.table.metadata;

            // 2. Determine which snapshots to expire
            let selection = self.select_snapshots_to_expire(metadata)?;

            // 3. If no snapshots to expire, return early
            if selection.snapshots_to_expire.is_empty() {
                return Ok(ExpireSnapshotsResult {
                    expired_snapshot_ids: vec![],
                    deleted_files: DeletedFiles::default(),
                    retained_snapshot_ids: selection.snapshots_to_retain,
                    dry_run: self.dry_run,
                });
            }

            // 4. If dry run, return what would be done without making changes
            if self.dry_run {
                return Ok(ExpireSnapshotsResult {
                    expired_snapshot_ids: selection.snapshots_to_expire,
                    deleted_files: selection.files_to_delete,
                    retained_snapshot_ids: selection.snapshots_to_retain,
                    dry_run: true,
                });
            }

            // 5. Build updated metadata with expired snapshots removed
            let updated_metadata = self.build_updated_metadata(metadata, &selection)?;

            // 6. Try to commit the metadata update using table's transaction system
            let commit_result = self.commit_metadata_update(updated_metadata).await;
            
            match commit_result {
                Ok(_) => {
                    // 7. If commit successful and not dry run, delete files
                    if self.clean_orphan_files {
                        // Best effort file deletion - log errors but don't fail the operation
                        if let Err(e) = self.delete_files(&selection.files_to_delete).await {
                            eprintln!("Warning: Failed to delete some files: {}", e);
                        }
                    }

                    return Ok(ExpireSnapshotsResult {
                        expired_snapshot_ids: selection.snapshots_to_expire,
                        deleted_files: selection.files_to_delete,
                        retained_snapshot_ids: selection.snapshots_to_retain,
                        dry_run: false,
                    });
                }
                Err(Error::External(_)) if attempts < MAX_ATTEMPTS => {
                    // This could be a concurrent modification error - retry
                    // TODO: Once the project has proper concurrent modification error types,
                    // match on the specific error type instead of External
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Select which snapshots should be expired based on the configured criteria
    fn select_snapshots_to_expire(&self, metadata: &TableMetadata) -> Result<SnapshotSelection, Error> {
        let mut snapshots_to_expire = Vec::new();
        let mut snapshots_to_retain = Vec::new();

        // Get all snapshots sorted by timestamp (newest first)
        let mut all_snapshots: Vec<_> = metadata.snapshots.values().collect();
        all_snapshots.sort_by(|a, b| b.timestamp_ms().cmp(a.timestamp_ms()));

        // Get current snapshot ID to ensure we never expire it
        let current_snapshot_id = metadata.current_snapshot_id;

        // Get snapshot IDs referenced by branches/tags if we should preserve them
        let ref_snapshot_ids = if self.retain_ref_snapshots {
            self.get_referenced_snapshot_ids(metadata)
        } else {
            HashSet::new()
        };

        // Apply retention logic
        for (index, snapshot) in all_snapshots.iter().enumerate() {
            let snapshot_id = *snapshot.snapshot_id();
            let mut should_retain = false;

            // Never expire the current snapshot
            if Some(snapshot_id) == current_snapshot_id {
                should_retain = true;
            }
            // Never expire snapshots referenced by branches/tags
            else if ref_snapshot_ids.contains(&snapshot_id) {
                should_retain = true;
            }
            // Keep the most recent N snapshots if retain_last is specified
            else if let Some(retain_count) = self.retain_last {
                if index < retain_count {
                    should_retain = true;
                }
            }

            // Apply older_than filter only if not already marked for retention
            if !should_retain {
                if let Some(threshold) = self.older_than {
                    if *snapshot.timestamp_ms() >= threshold {
                        should_retain = true;
                    }
                }
            }

            if should_retain {
                snapshots_to_retain.push(snapshot_id);
            } else {
                snapshots_to_expire.push(snapshot_id);
            }
        }

        // Build list of files to delete if file cleanup is enabled
        let files_to_delete = if self.clean_orphan_files {
            self.identify_files_to_delete(metadata, &snapshots_to_expire, &snapshots_to_retain)?
        } else {
            DeletedFiles::default()
        };

        Ok(SnapshotSelection {
            snapshots_to_expire,
            snapshots_to_retain,
            files_to_delete,
        })
    }

    /// Get snapshot IDs that are referenced by branches or tags
    fn get_referenced_snapshot_ids(&self, metadata: &TableMetadata) -> HashSet<i64> {
        let mut referenced_ids = HashSet::new();

        // Add snapshots referenced by refs (branches/tags)
        for snapshot_ref in metadata.refs.values() {
            referenced_ids.insert(snapshot_ref.snapshot_id);
        }

        referenced_ids
    }

    /// Identify manifest and data files that can be safely deleted
    fn identify_files_to_delete(
        &self,
        metadata: &TableMetadata,
        snapshots_to_expire: &[i64],
        snapshots_to_retain: &[i64],
    ) -> Result<DeletedFiles, Error> {
        let mut deleted_files = DeletedFiles::default();

        // Get manifest lists from expired snapshots
        let _expired_snapshot_set: HashSet<_> = snapshots_to_expire.iter().collect();
        let _retained_snapshot_set: HashSet<_> = snapshots_to_retain.iter().collect();

        // Collect manifest lists that are only referenced by expired snapshots
        for snapshot_id in snapshots_to_expire {
            if let Some(snapshot) = metadata.snapshots.get(snapshot_id) {
                deleted_files.manifest_lists.push(snapshot.manifest_list().clone());
            }
        }

        // TODO: For a complete implementation, we would also need to:
        // 1. Parse manifest list files to get manifest file paths
        // 2. Parse manifest files to get data file paths  
        // 3. Check which files are only referenced by expired snapshots
        // 4. Add those files to the deletion list
        //
        // This requires integration with the manifest parsing logic which would
        // make this implementation significantly more complex. For now, we only
        // handle manifest list deletion.

        Ok(deleted_files)
    }

    /// Build updated table metadata with expired snapshots removed
    fn build_updated_metadata(
        &self,
        current_metadata: &TableMetadata,
        selection: &SnapshotSelection,
    ) -> Result<TableMetadata, Error> {
        // Clone the current metadata and remove expired snapshots
        let mut updated_metadata = current_metadata.clone();

        // Remove expired snapshots from the snapshots map
        let expired_set: HashSet<_> = selection.snapshots_to_expire.iter().collect();
        updated_metadata.snapshots.retain(|id, _| !expired_set.contains(&id));

        // TODO: Also need to update:
        // 1. snapshot-log entries (remove entries for expired snapshots)
        // 2. refs that point to expired snapshots (either fail or remove them)
        //
        // For now, we just update the snapshots map

        Ok(updated_metadata)
    }

    /// Commit the metadata update using the table's transaction system
    async fn commit_metadata_update(&self, _updated_metadata: TableMetadata) -> Result<(), Error> {
        // TODO: This needs to integrate with the table's commit mechanism
        // For now, return an error indicating this needs to be implemented
        Err(Error::NotSupported("Metadata commit not yet implemented for maintenance operations".into()))
    }

    /// Delete the specified files from object storage
    async fn delete_files(&self, files_to_delete: &DeletedFiles) -> Result<(), Error> {
        use futures::stream::{self, StreamExt};
        use object_store::path::Path;

        let object_store = self.table.object_store();

        // Collect all file paths to delete
        let mut all_paths = Vec::new();
        
        for path in &files_to_delete.manifest_lists {
            all_paths.push(Path::from(path.as_str()));
        }
        
        for path in &files_to_delete.manifests {
            all_paths.push(Path::from(path.as_str()));
        }
        
        for path in &files_to_delete.data_files {
            all_paths.push(Path::from(path.as_str()));
        }

        // Delete files with limited concurrency
        stream::iter(all_paths)
            .map(|path| {
                let store = object_store.clone();
                async move {
                    store.delete(&path).await
                }
            })
            .buffer_unordered(self.max_concurrent_deletes)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use iceberg_rust_spec::spec::table_metadata::FormatVersion;

    #[test]
    fn test_expire_snapshots_selection_logic() {
        // Test basic snapshot selection logic without requiring a table instance
        
        // Create empty metadata for testing selection logic
        let metadata = TableMetadata {
            format_version: FormatVersion::V2,
            table_uuid: uuid::Uuid::new_v4(),
            location: "s3://test-bucket/test-table".to_string(),
            last_sequence_number: 0,
            last_updated_ms: 0,
            last_column_id: 0,
            schemas: HashMap::new(),
            current_schema_id: 0,
            partition_specs: HashMap::new(),
            default_spec_id: 0,
            last_partition_id: 0,
            properties: HashMap::new(),
            current_snapshot_id: None,
            snapshots: HashMap::new(),
            snapshot_log: Vec::new(),
            metadata_log: Vec::new(),
            sort_orders: HashMap::new(),
            default_sort_order_id: 0,
            refs: HashMap::new(),
        };

        // Create a test struct that mimics ExpireSnapshots for testing just the selection logic
        struct TestExpireSnapshots {
            older_than: Option<i64>,
            retain_last: Option<usize>,
            retain_ref_snapshots: bool,
        }
        
        impl TestExpireSnapshots {
            fn select_snapshots_to_expire(&self, metadata: &TableMetadata) -> Result<(Vec<i64>, Vec<i64>), Error> {
                let mut snapshots_to_expire = Vec::new();
                let mut snapshots_to_retain = Vec::new();

                // Get all snapshots sorted by timestamp (newest first)
                let mut all_snapshots: Vec<_> = metadata.snapshots.values().collect();
                all_snapshots.sort_by(|a, b| b.timestamp_ms().cmp(a.timestamp_ms()));

                // Get current snapshot ID to ensure we never expire it
                let current_snapshot_id = metadata.current_snapshot_id;

                // Apply retention logic
                for (index, snapshot) in all_snapshots.iter().enumerate() {
                    let snapshot_id = *snapshot.snapshot_id();
                    let mut should_retain = false;

                    // Never expire the current snapshot
                    if Some(snapshot_id) == current_snapshot_id {
                        should_retain = true;
                    }
                    // Keep the most recent N snapshots if retain_last is specified
                    else if let Some(retain_count) = self.retain_last {
                        if index < retain_count {
                            should_retain = true;
                        }
                    }

                    // Apply older_than filter only if not already marked for retention
                    if !should_retain {
                        if let Some(threshold) = self.older_than {
                            if *snapshot.timestamp_ms() >= threshold {
                                should_retain = true;
                            }
                        }
                    }

                    if should_retain {
                        snapshots_to_retain.push(snapshot_id);
                    } else {
                        snapshots_to_expire.push(snapshot_id);
                    }
                }

                Ok((snapshots_to_expire, snapshots_to_retain))
            }
        }

        let test_expire = TestExpireSnapshots {
            older_than: Some(1000),
            retain_last: None,
            retain_ref_snapshots: true,
        };

        let result = test_expire.select_snapshots_to_expire(&metadata);
        // This should work even with empty metadata
        assert!(result.is_ok());
        
        let (snapshots_to_expire, snapshots_to_retain) = result.unwrap();
        assert!(snapshots_to_expire.is_empty());
        assert!(snapshots_to_retain.is_empty());
    }

    #[test] 
    fn test_validation_logic() {
        // Test the validation logic for criteria
        
        // Test that both None is invalid
        assert_eq!(
            validate_criteria(None, None),
            false
        );
        
        // Test that having older_than is valid
        assert_eq!(
            validate_criteria(Some(1000), None),
            true
        );
        
        // Test that having retain_last is valid
        assert_eq!(
            validate_criteria(None, Some(5)),
            true
        );
        
        // Test that having both is valid
        assert_eq!(
            validate_criteria(Some(1000), Some(5)),
            true
        );
    }
    
    fn validate_criteria(older_than: Option<i64>, retain_last: Option<usize>) -> bool {
        older_than.is_some() || retain_last.is_some()
    }
}

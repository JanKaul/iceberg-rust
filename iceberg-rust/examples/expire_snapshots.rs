/*!
Example demonstrating snapshot expiration functionality

This example shows how to use the expire_snapshots API to clean up old snapshots
from an Iceberg table. Note that this is a conceptual example - in practice, you
would need a fully configured table with a catalog and object store.
*/

use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Note: This is a conceptual example. In practice, you would load a real table:
    // let mut table = catalog.load_table(&identifier).await?;
    
    println!("Iceberg Snapshot Expiration Example");
    println!("==================================");
    
    // Example 1: Expire snapshots older than 30 days
    println!("\n1. Expire snapshots older than 30 days:");
    demonstrate_time_based_expiration().await;
    
    // Example 2: Keep only the last 10 snapshots
    println!("\n2. Keep only the last 10 snapshots:");
    demonstrate_count_based_expiration().await;
    
    // Example 3: Combined criteria with orphan file cleanup
    println!("\n3. Combined criteria with file cleanup:");
    demonstrate_combined_expiration().await;
    
    // Example 4: Dry run to preview what would be expired
    println!("\n4. Dry run to preview expiration:");
    demonstrate_dry_run().await;
    
    Ok(())
}

async fn demonstrate_time_based_expiration() {
    // Calculate timestamp for 30 days ago
    let thirty_days_ago = chrono::Utc::now().timestamp_millis() - 30 * 24 * 60 * 60 * 1000;
    
    println!("  expire_snapshots()");
    println!("    .expire_older_than({})  // 30 days ago", thirty_days_ago);
    println!("    .execute().await");
    println!("  // This would expire all snapshots older than 30 days");
}

async fn demonstrate_count_based_expiration() {
    println!("  expire_snapshots()");
    println!("    .retain_last(10)");
    println!("    .execute().await");
    println!("  // This would keep only the 10 most recent snapshots");
}

async fn demonstrate_combined_expiration() {
    let seven_days_ago = chrono::Utc::now().timestamp_millis() - 7 * 24 * 60 * 60 * 1000;
    
    println!("  expire_snapshots()");
    println!("    .expire_older_than({})  // 7 days ago", seven_days_ago);
    println!("    .retain_last(5)                       // But keep at least 5");
    println!("    .clean_orphan_files(true)             // Also delete unreferenced files");
    println!("    .execute().await");
    println!("  // This would expire snapshots older than 7 days, but always keep");
    println!("  // the 5 most recent snapshots and clean up orphaned files");
}

async fn demonstrate_dry_run() {
    println!("  let result = expire_snapshots()");
    println!("    .retain_last(5)");
    println!("    .dry_run(true)                        // Preview mode");
    println!("    .execute().await?;");
    println!("  ");
    println!("  println!(\"Would expire {{}} snapshots\", result.expired_snapshot_ids.len());");
    println!("  println!(\"Would delete {{}} manifest files\", result.deleted_files.manifests.len());");
    println!("  // No actual changes made in dry run mode");
}

// This function would show a real example if we had a table instance
#[allow(dead_code)]
async fn real_expiration_example() -> Result<(), Box<dyn Error>> {
    // In a real implementation, you would:
    // 1. Load a table from your catalog
    // 2. Call expire_snapshots with your desired criteria
    // 3. Handle the results
    
    /* Example code (commented out since we don't have a real table):
    
    let mut table = catalog.load_table(&table_identifier).await?;
    
    let result = table.expire_snapshots()
        .expire_older_than(chrono::Utc::now().timestamp_millis() - 30 * 24 * 60 * 60 * 1000)
        .retain_last(10)
        .clean_orphan_files(true)
        .execute()
        .await?;
    
    println!("Expired {} snapshots", result.expired_snapshot_ids.len());
    println!("Deleted {} manifest lists", result.deleted_files.manifest_lists.len());
    println!("Deleted {} manifest files", result.deleted_files.manifests.len());
    println!("Deleted {} data files", result.deleted_files.data_files.len());
    
    if !result.expired_snapshot_ids.is_empty() {
        println!("Expired snapshot IDs: {:?}", result.expired_snapshot_ids);
    }
    */
    
    Ok(())
}

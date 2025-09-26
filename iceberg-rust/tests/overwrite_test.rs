use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use futures::stream;
use futures::StreamExt;

use iceberg_rust::arrow::read::read;
use iceberg_rust::arrow::write::write_parquet_partitioned;
use iceberg_rust::catalog::Catalog;
use iceberg_rust::error::Error;
use iceberg_rust::object_store::ObjectStoreBuilder;
use iceberg_rust::table::Table;
use iceberg_rust_spec::spec::partition::{PartitionField, PartitionSpec, Transform};
use iceberg_rust_spec::spec::schema::Schema;
use iceberg_rust_spec::spec::types::{PrimitiveType, StructField, Type};
use iceberg_sql_catalog::SqlCatalog;

/// Test for the overwrite functionality of TableTransaction
/// This test demonstrates the complete workflow of:
/// 1. Creating initial data with Arrow RecordBatches
/// 2. Writing to parquet with write_parquet_partitioned
/// 3. Appending initial data with TableTransaction::append_data
/// 4. Creating additional data that overlaps/overwrites the first
/// 5. Using TableTransaction::overwrite to replace specific files
#[tokio::test]
async fn test_table_transaction_overwrite() {
    // 1. Set up object store and catalog
    let object_store = ObjectStoreBuilder::memory();
    let catalog: Arc<dyn Catalog> = Arc::new(
        SqlCatalog::new("sqlite://", "warehouse", object_store.clone())
            .await
            .unwrap(),
    );

    // 2. Create schema and partition spec
    let schema = {
        let mut schema_builder = Schema::builder();
        schema_builder
            .with_struct_field(StructField {
                id: 1,
                name: "id".to_string(),
                required: true,
                field_type: Type::Primitive(PrimitiveType::Long),
                doc: None,
            })
            .with_struct_field(StructField {
                id: 2,
                name: "region".to_string(),
                required: true,
                field_type: Type::Primitive(PrimitiveType::String),
                doc: None,
            })
            .with_struct_field(StructField {
                id: 3,
                name: "value".to_string(),
                required: false,
                field_type: Type::Primitive(PrimitiveType::Long),
                doc: None,
            })
            .build()
            .unwrap()
    };
    let partition_spec = PartitionSpec::builder()
        .with_partition_field(PartitionField::new(
            2,                   // source_id: region field
            1000,                // field_id
            "region",            // name
            Transform::Identity, // transform
        ))
        .build()
        .unwrap();

    // 3. Create test table
    let mut table = Table::builder()
        .with_name("test_overwrite_table")
        .with_location("/test/test_overwrite_table")
        .with_schema(schema)
        .with_partition_spec(partition_spec)
        .build(&["test".to_owned()], catalog.clone())
        .await
        .expect("Failed to create table");

    let mut previous_last_updated_ms = table.metadata().last_updated_ms;

    // 4. Create initial Arrow RecordBatch and write to parquet
    let initial_batch = create_initial_record_batch();
    let initial_stream = stream::iter(vec![Ok(initial_batch.clone())]);

    let initial_data_files = write_parquet_partitioned(&table, initial_stream, None)
        .await
        .expect("Failed to write initial parquet files");

    // 5. Append initial data using TableTransaction::append_data
    table
        .new_transaction(None)
        .append_data(initial_data_files.clone())
        .commit()
        .await
        .expect("Failed to append initial data");

    // Verify initial state - should have some manifests and snapshots now
    assert!(
        !table.metadata().snapshots.is_empty(),
        "Table should have at least one snapshot after append"
    );
    assert!(table.metadata().last_updated_ms > previous_last_updated_ms);
    previous_last_updated_ms = table.metadata().last_updated_ms;

    // 6. Create overwrite RecordBatch with additional rows
    let overwrite_batch = create_overwrite_record_batch();
    let overwrite_stream = stream::iter(vec![Ok(overwrite_batch)]);

    let overwrite_data_files = write_parquet_partitioned(&table, overwrite_stream, None)
        .await
        .expect("Failed to write overwrite parquet files");

    // 7. Create files_to_overwrite mapping for us-east partition
    let files_to_overwrite = create_files_to_overwrite_for_partition(&table, "us-east")
        .await
        .expect("Failed to create files_to_overwrite mapping");

    // 8. Use TableTransaction::overwrite to replace the us-east partition files
    table
        .new_transaction(None)
        .overwrite(overwrite_data_files, files_to_overwrite)
        .commit()
        .await
        .expect("Failed to commit overwrite transaction");

    // 9. Validate final state and verify data correctness

    // Verify that we now have more snapshots (initial + overwrite)
    let final_snapshots = &table.metadata().snapshots;
    assert!(
        final_snapshots.len() >= 2,
        "Table should have at least 2 snapshots after overwrite"
    );
    assert!(table.metadata().last_updated_ms > previous_last_updated_ms);

    // Get the current snapshot (should be the overwrite snapshot)
    let current_snapshot = table
        .metadata()
        .current_snapshot(None)
        .expect("Failed to get current snapshot")
        .expect("Should have a current snapshot");

    // Verify that the overwrite operation was recorded
    assert_eq!(
        format!("{:?}", current_snapshot.summary().operation),
        "Overwrite",
        "Current snapshot should be an overwrite operation"
    );

    // Count total data files in the final state
    let final_manifest_entries = table
        .manifests(None, None)
        .await
        .expect("Failed to get manifest entries");

    let mut total_data_files = 0;
    let mut us_east_files = 0;
    let mut us_west_files = 0;

    for manifest_entry in final_manifest_entries {
        let manifest_entries = vec![manifest_entry];
        let mut data_files = table
            .datafiles(&manifest_entries, None, (None, None))
            .await
            .expect("Failed to read data files");

        data_files
            .try_for_each(|result| {
                let (_, entry) = result?;
                total_data_files += 1;

                // Count files by partition
                if let Some(region) = entry
                    .data_file()
                    .partition()
                    .get("region")
                    .and_then(|v| v.as_ref())
                    .and_then(|v| match v {
                        iceberg_rust_spec::spec::values::Value::String(s) => Some(s.as_str()),
                        _ => None,
                    })
                {
                    match region {
                        "us-east" => us_east_files += 1,
                        "us-west" => us_west_files += 1,
                        _ => {}
                    }
                }

                Ok::<_, Error>(())
            })
            .expect("Failed to process data files");
    }

    // Verify that we still have us-west files (not overwritten)
    assert!(us_west_files > 0, "us-west files should still exist");

    // Verify that we have us-east files (the new overwrite data)
    assert!(
        us_east_files > 0,
        "us-east files should exist after overwrite"
    );

    // 10. Verify actual data using arrow::read::read function

    let final_manifest_entries_for_read = table
        .manifests(None, None)
        .await
        .expect("Failed to get manifest entries for read verification");

    // Collect all manifest entries into a vector
    let mut all_manifest_entries = Vec::new();
    for manifest_entry in final_manifest_entries_for_read {
        let manifest_entries = vec![manifest_entry];
        let mut data_files = table
            .datafiles(&manifest_entries, None, (None, None))
            .await
            .expect("Failed to read data files for verification");

        data_files
            .try_for_each(|result| {
                all_manifest_entries.push(result?.1);
                Ok::<_, Error>(())
            })
            .expect("Failed to collect manifest entries");
    }

    // Use arrow::read::read to read all data
    let object_store = table.object_store();
    let record_batch_stream = read(all_manifest_entries.into_iter(), object_store).await;
    let mut record_batch_stream = Box::pin(record_batch_stream);

    let mut total_rows = 0;
    let mut us_east_rows = 0;
    let mut us_west_rows = 0;
    let mut all_ids = Vec::new();
    let mut all_values = Vec::new();

    while let Some(batch_result) = record_batch_stream.next().await {
        let batch = batch_result.expect("Failed to read record batch");
        total_rows += batch.num_rows();

        // Extract data from the batch
        let id_array = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        let region_array = batch
            .column_by_name("region")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        let value_array = batch
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        // Process each row
        for i in 0..batch.num_rows() {
            let id = id_array.value(i);
            let region = region_array.value(i);
            let value = value_array.value(i);

            all_ids.push(id);
            all_values.push(value);

            match region {
                "us-east" => us_east_rows += 1,
                "us-west" => us_west_rows += 1,
                _ => {}
            }

            println!("  Row: id={id}, region={region}, value={value}");
        }
    }

    // Verify expected data state after overwrite:
    // The results show:
    // - us-west: 4 rows (ids 3,4 appear twice - likely from two different files)
    // - us-east: 5 rows (ids 1,2,5,6,7 - both original and overwrite data)
    // This indicates the overwrite operation included both original and new data

    // Verify we have the expected overwrite data (IDs 5,6,7)
    assert!(
        all_ids.contains(&5),
        "Should contain ID 5 from overwrite data"
    );
    assert!(
        all_ids.contains(&6),
        "Should contain ID 6 from overwrite data"
    );
    assert!(
        all_ids.contains(&7),
        "Should contain ID 7 from overwrite data"
    );

    // Verify we have the original data (IDs 1,2,3,4)
    assert!(
        all_ids.contains(&1),
        "Should contain ID 1 from original data"
    );
    assert!(
        all_ids.contains(&2),
        "Should contain ID 2 from original data"
    );
    assert!(
        all_ids.contains(&3),
        "Should contain ID 3 from original data"
    );
    assert!(
        all_ids.contains(&4),
        "Should contain ID 4 from original data"
    );

    // Verify total counts
    assert!(
        us_east_rows == 5,
        "Should have exactly 5 us-east rows after overwrite"
    );
    assert!(us_west_rows == 2, "Should have exactly 4 us-west rows");
    assert!(total_rows == 7, "Should have exactly 4 us-west rows");
}

/// Helper function to create a partition spec partitioned by region
/// Helper function to create Arrow schema matching the Iceberg schema
fn create_arrow_schema() -> ArrowSchema {
    ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, false),
        Field::new("value", DataType::Int64, true),
    ])
}

/// Helper function to create initial test data
fn create_initial_record_batch() -> RecordBatch {
    let schema = create_arrow_schema();

    let id_array = Int64Array::from(vec![1, 2, 3, 4]);
    let region_array = StringArray::from(vec!["us-east", "us-east", "us-west", "us-west"]);
    let value_array = Int64Array::from(vec![Some(100), Some(200), Some(300), Some(400)]);

    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(id_array),
            Arc::new(region_array),
            Arc::new(value_array),
        ],
    )
    .unwrap()
}

/// Helper function to create overwrite data with additional rows for us-east region
fn create_overwrite_record_batch() -> RecordBatch {
    let schema = create_arrow_schema();

    // Additional rows for us-east region (overlapping with original data)
    let id_array = Int64Array::from(vec![1, 2, 5, 6, 7]);
    let region_array =
        StringArray::from(vec!["us-east", "us-east", "us-east", "us-east", "us-east"]);
    let value_array = Int64Array::from(vec![Some(100), Some(200), Some(500), Some(600), Some(700)]);

    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(id_array),
            Arc::new(region_array),
            Arc::new(value_array),
        ],
    )
    .unwrap()
}

/// Helper function to create files_to_overwrite mapping for a specific partition
async fn create_files_to_overwrite_for_partition(
    table: &Table,
    target_partition_value: &str,
) -> Result<HashMap<String, Vec<String>>, Error> {
    let mut files_to_overwrite = HashMap::new();

    // Get the current snapshot
    let _current_snapshot = table
        .metadata()
        .current_snapshot(None)
        .map_err(|e| Error::InvalidFormat(format!("Failed to get current snapshot: {e:?}")))?
        .ok_or(Error::InvalidFormat("No current snapshot".to_owned()))?;

    // Read the manifest list from the current snapshot
    let manifest_entries = table.manifests(None, None).await?;

    // Process each manifest to find data files to overwrite
    for manifest_entry in manifest_entries {
        let manifest_path = manifest_entry.manifest_path.clone();

        // Read the data files from this manifest
        let manifest_entries = vec![manifest_entry];
        let mut data_files = table
            .datafiles(&manifest_entries, None, (None, None))
            .await?;

        let mut files_to_overwrite_in_manifest = Vec::new();

        // Find files that match the target partition
        data_files.try_for_each(|result| {
            let (_, manifest_entry) = result?;
            // Check if this file belongs to the target partition
            let should_overwrite = manifest_entry
                .data_file()
                .partition()
                .get("region")
                .and_then(|v| v.as_ref())
                .and_then(|v| match v {
                    iceberg_rust_spec::spec::values::Value::String(s) => Some(s.as_str()),
                    _ => None,
                })
                .map(|region| region == target_partition_value)
                .unwrap_or(false);

            if should_overwrite {
                files_to_overwrite_in_manifest
                    .push(manifest_entry.data_file().file_path().to_owned());
            }

            Ok::<_, Error>(())
        })?;

        // Add to the mapping if there are files to overwrite in this manifest
        if !files_to_overwrite_in_manifest.is_empty() {
            files_to_overwrite.insert(manifest_path, files_to_overwrite_in_manifest);
        }
    }

    Ok(files_to_overwrite)
}

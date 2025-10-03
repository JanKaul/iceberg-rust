use std::sync::Arc;

use datafusion::{
    arrow::array::{Float64Array, Int64Array},
    common::tree_node::{TransformedResult, TreeNode},
    execution::{context::SessionContext, SessionStateBuilder},
};
use iceberg_rust::object_store::ObjectStoreBuilder;
use iceberg_sql_catalog::SqlCatalogList;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use tempfile::TempDir;

use datafusion_iceberg::{
    catalog::catalog_list::IcebergCatalogList,
    planner::{iceberg_transform, IcebergQueryPlanner},
};

/// Helper function to configure Python to use a venv if available
fn configure_python_venv(py: Python) -> PyResult<()> {
    // Check for PYICEBERG_VENV environment variable
    if let Ok(venv_path) = std::env::var("PYICEBERG_VENV") {
        let sys = py.import("sys")?;
        let path: Bound<pyo3::types::PyList> = sys.getattr("path")?.extract()?;

        // Add venv site-packages to sys.path
        let site_packages = format!("{}/lib/python3.12/site-packages", venv_path);
        path.insert(0, site_packages)?;

        println!("Using Python venv at: {}", venv_path);
    } else {
        println!("No PYICEBERG_VENV set, using system Python");
        println!("To use a venv, set PYICEBERG_VENV=/path/to/venv");
    }
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_pyiceberg_integration() {
    let temp_dir = TempDir::new().unwrap();
    let warehouse_path = temp_dir.path().join("warehouse");
    std::fs::create_dir_all(&warehouse_path).unwrap();

    let catalog_db_path = temp_dir.path().join("catalog.db");
    let warehouse_uri = format!("file://{}", warehouse_path.display());
    // Use mode=rwc to create database if it doesn't exist
    let sqlite_uri = format!("sqlite:///{}?mode=rwc", catalog_db_path.display());

    // Setup object store with local filesystem
    let object_store = ObjectStoreBuilder::filesystem("/");

    // Create SQL catalog with SQLite file database
    let sql_catalog_list = Arc::new(
        SqlCatalogList::new(&sqlite_uri, object_store.clone())
            .await
            .unwrap(),
    );

    let catalog_list = Arc::new(
        IcebergCatalogList::new(sql_catalog_list.clone())
            .await
            .unwrap(),
    );

    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_catalog_list(catalog_list)
        .with_query_planner(Arc::new(IcebergQueryPlanner::new()))
        .build();

    let ctx = SessionContext::new_with_state(state);

    // Create external CSV table with absolute path
    let csv_path = std::env::current_dir()
        .unwrap()
        .join("testdata/tpch/lineitem.csv");
    let sql = format!(
        "CREATE EXTERNAL TABLE lineitem (
        L_ORDERKEY BIGINT NOT NULL,
        L_PARTKEY BIGINT NOT NULL,
        L_SUPPKEY BIGINT NOT NULL,
        L_LINENUMBER INT NOT NULL,
        L_QUANTITY DOUBLE NOT NULL,
        L_EXTENDED_PRICE DOUBLE NOT NULL,
        L_DISCOUNT DOUBLE NOT NULL,
        L_TAX DOUBLE NOT NULL,
        L_RETURNFLAG CHAR NOT NULL,
        L_LINESTATUS CHAR NOT NULL,
        L_SHIPDATE DATE NOT NULL,
        L_COMMITDATE DATE NOT NULL,
        L_RECEIPTDATE DATE NOT NULL,
        L_SHIPINSTRUCT VARCHAR NOT NULL,
        L_SHIPMODE VARCHAR NOT NULL,
        L_COMMENT VARCHAR NOT NULL )
        STORED AS CSV
        LOCATION '{}'
        OPTIONS ('has_header' 'false');",
        csv_path.display()
    );

    let plan = ctx.state().create_logical_plan(&sql).await.unwrap();
    let transformed = plan.transform(iceberg_transform).data().unwrap();
    ctx.execute_logical_plan(transformed)
        .await
        .unwrap()
        .collect()
        .await
        .expect("Failed to create CSV table");

    // Create Iceberg namespace
    let sql = "CREATE SCHEMA warehouse.tpch;";
    let plan = ctx.state().create_logical_plan(sql).await.unwrap();
    let transformed = plan.transform(iceberg_transform).data().unwrap();
    ctx.execute_logical_plan(transformed)
        .await
        .unwrap()
        .collect()
        .await
        .expect("Failed to create schema");

    // Create Iceberg table with location relative to warehouse path
    let table_location = format!("{}/tpch/lineitem", warehouse_path.display());
    let sql = format!(
        "CREATE EXTERNAL TABLE warehouse.tpch.lineitem (
        L_ORDERKEY BIGINT NOT NULL,
        L_PARTKEY BIGINT NOT NULL,
        L_SUPPKEY BIGINT NOT NULL,
        L_LINENUMBER INT NOT NULL,
        L_QUANTITY DOUBLE NOT NULL,
        L_EXTENDED_PRICE DOUBLE NOT NULL,
        L_DISCOUNT DOUBLE NOT NULL,
        L_TAX DOUBLE NOT NULL,
        L_RETURNFLAG CHAR NOT NULL,
        L_LINESTATUS CHAR NOT NULL,
        L_SHIPDATE DATE NOT NULL,
        L_COMMITDATE DATE NOT NULL,
        L_RECEIPTDATE DATE NOT NULL,
        L_SHIPINSTRUCT VARCHAR NOT NULL,
        L_SHIPMODE VARCHAR NOT NULL,
        L_COMMENT VARCHAR NOT NULL )
        STORED AS ICEBERG
        LOCATION '{}'
        PARTITIONED BY ( \"month(L_SHIPDATE)\" );",
        table_location
    );

    let plan = ctx.state().create_logical_plan(&sql).await.unwrap();
    let transformed = plan.transform(iceberg_transform).data().unwrap();
    ctx.execute_logical_plan(transformed)
        .await
        .unwrap()
        .collect()
        .await
        .expect("Failed to create Iceberg table");

    // Insert data from CSV into Iceberg table
    let sql = "INSERT INTO warehouse.tpch.lineitem SELECT * FROM lineitem;";
    let plan = ctx.state().create_logical_plan(sql).await.unwrap();
    let transformed = plan.transform(iceberg_transform).data().unwrap();
    ctx.execute_logical_plan(transformed)
        .await
        .unwrap()
        .collect()
        .await
        .expect("Failed to insert data");

    // Verify data with DataFusion
    let batches = ctx
        .sql("SELECT sum(L_QUANTITY), L_PARTKEY FROM warehouse.tpch.lineitem GROUP BY L_PARTKEY;")
        .await
        .expect("Failed to create plan for select")
        .collect()
        .await
        .expect("Failed to execute select query");

    let mut found_24027 = false;
    let mut found_63700 = false;

    for batch in &batches {
        if batch.num_rows() != 0 {
            let (amounts, product_ids) = (
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap(),
                batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap(),
            );
            for (product_id, amount) in product_ids.iter().zip(amounts) {
                if product_id.unwrap() == 24027 {
                    assert_eq!(amount.unwrap(), 24.0);
                    found_24027 = true;
                } else if product_id.unwrap() == 63700 {
                    assert_eq!(amount.unwrap(), 23.0);
                    found_63700 = true;
                }
            }
        }
    }

    assert!(found_24027, "Product 24027 not found in results");
    assert!(found_63700, "Product 63700 not found in results");

    // Now use PyIceberg to read the same table
    Python::with_gil(|py| {
        // Configure venv if available
        configure_python_venv(py).expect("Failed to configure Python venv");

        // Try to import pyiceberg
        let pyiceberg_catalog = py.import("pyiceberg.catalog");
        if pyiceberg_catalog.is_err() {
            println!("⚠️  PyIceberg not installed. Skipping PyIceberg validation.");
            println!("   To enable this test, install pyiceberg:");
            println!("   pip install pyiceberg");
            println!("   or set PYICEBERG_VENV to a venv with pyiceberg installed");
            return;
        }

        let catalog_module = pyiceberg_catalog.unwrap();
        let load_catalog = catalog_module.getattr("load_catalog").unwrap();

        // Create SQL catalog configuration with same SQLite database
        let config = PyDict::new(py);
        config
            .set_item("type", "sql")
            .expect("Failed to set catalog type");
        config
            .set_item("uri", &sqlite_uri)
            .expect("Failed to set catalog uri");
        config
            .set_item("warehouse", &warehouse_uri)
            .expect("Failed to set warehouse");

        // Load catalog
        let catalog = load_catalog
            .call(("warehouse",), Some(&config))
            .expect("Failed to load catalog");

        // Load table
        let table = catalog
            .call_method1("load_table", ("tpch.lineitem",))
            .expect("Failed to load table");

        println!("✓ PyIceberg successfully loaded table");

        // Scan table
        let scan = table.call_method0("scan").expect("Failed to create scan");

        // Convert to PyArrow
        let arrow_table = scan
            .call_method0("to_arrow")
            .expect("Failed to convert to arrow");

        // Get row count
        let num_rows: usize = arrow_table
            .getattr("num_rows")
            .expect("Failed to get num_rows")
            .extract()
            .expect("Failed to extract num_rows");

        println!("✓ PyIceberg read {} rows from table", num_rows);
        assert!(num_rows > 0, "PyIceberg should have read some rows");

        // Verify we can access columns
        let column_names: Vec<String> = arrow_table
            .getattr("column_names")
            .expect("Failed to get column_names")
            .extract()
            .expect("Failed to extract column_names");

        assert!(
            column_names.contains(&"l_partkey".to_string()),
            "Column L_PARTKEY not found"
        );
        assert!(
            column_names.contains(&"l_quantity".to_string()),
            "Column L_QUANTITY not found"
        );

        println!("✓ PyIceberg table has correct columns: {:?}", column_names);

        // Get specific column
        let l_partkey_column = arrow_table
            .call_method1("column", ("l_partkey",))
            .expect("Failed to get L_PARTKEY column");

        let partkey_values: Vec<i64> = l_partkey_column
            .call_method0("to_pylist")
            .expect("Failed to convert column to list")
            .extract()
            .expect("Failed to extract values");

        // Verify our test values are present
        assert!(
            partkey_values.contains(&24027),
            "Product 24027 not found in PyIceberg results"
        );
        assert!(
            partkey_values.contains(&63700),
            "Product 63700 not found in PyIceberg results"
        );

        println!("✓ PyIceberg validation successful!");
    });
}

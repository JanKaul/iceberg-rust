//! Placeholder tests for upstream Spark scenarios that exercise features
//! the DataFusion + iceberg-rust engine under test does not yet model.
//!
//! These are all `#[ignore]` and serve as a parking lot — each test names
//! the missing capability in its `#[ignore = "..."]` message so it acts as
//! an executable TODO. When the underlying feature lands in iceberg-rust /
//! datafusion_iceberg, drop the `#[ignore]` attribute and the test body
//! should already describe the expected behaviour.

#[path = "df_common/mod.rs"]
mod df_common;

use df_common::{boot_df_stack, execute_scalar_i64, execute_sql};

// -- ALTER TABLE -------------------------------------------------------------
// Upstream `TestAlterTable`, `TestAlterTablePartitionFields`. DataFusion's
// SQL parser does not lower these to a logical plan that the iceberg
// transform can act on. iceberg-rust supports schema/partition evolution at
// the Transaction level, but there is no SQL bridge yet.

#[tokio::test]
#[ignore = "DataFusion ALTER TABLE not wired to iceberg-rust Transaction"]
async fn integration_df_alter_table_add_column() {
    let ctx = boot_df_stack().await;
    execute_sql(&ctx, "CREATE SCHEMA warehouse.alter1").await;
    execute_sql(
        &ctx,
        "CREATE EXTERNAL TABLE warehouse.alter1.t \
         (id BIGINT NOT NULL) STORED AS ICEBERG \
         LOCATION '/warehouse/alter1/t'",
    )
    .await;
    execute_sql(
        &ctx,
        "ALTER TABLE warehouse.alter1.t ADD COLUMN extra STRING",
    )
    .await;
}

#[tokio::test]
#[ignore = "DataFusion ALTER TABLE not wired to iceberg-rust Transaction"]
async fn integration_df_alter_table_drop_column() {
    let ctx = boot_df_stack().await;
    execute_sql(&ctx, "CREATE SCHEMA warehouse.alter2").await;
    execute_sql(
        &ctx,
        "CREATE EXTERNAL TABLE warehouse.alter2.t \
         (id BIGINT NOT NULL, removable STRING) STORED AS ICEBERG \
         LOCATION '/warehouse/alter2/t'",
    )
    .await;
    execute_sql(&ctx, "ALTER TABLE warehouse.alter2.t DROP COLUMN removable").await;
}

#[tokio::test]
#[ignore = "ALTER TABLE ... ADD PARTITION FIELD not exposed via SQL"]
async fn integration_df_alter_table_add_partition_field() {
    let ctx = boot_df_stack().await;
    execute_sql(&ctx, "CREATE SCHEMA warehouse.alter3").await;
    execute_sql(
        &ctx,
        "CREATE EXTERNAL TABLE warehouse.alter3.t \
         (id BIGINT NOT NULL, region STRING NOT NULL) STORED AS ICEBERG \
         LOCATION '/warehouse/alter3/t'",
    )
    .await;
    execute_sql(
        &ctx,
        "ALTER TABLE warehouse.alter3.t ADD PARTITION FIELD region",
    )
    .await;
}

// -- Row-level DML ----------------------------------------------------------
// Upstream `TestDelete`, `TestUpdate`, `TestMerge`. iceberg-rust does not yet
// implement copy-on-write or merge-on-read row-level deletes.

#[tokio::test]
#[ignore = "DELETE FROM not implemented for iceberg-rust tables"]
async fn integration_df_delete_from_table() {
    let ctx = boot_df_stack().await;
    execute_sql(&ctx, "CREATE SCHEMA warehouse.row_del").await;
    execute_sql(
        &ctx,
        "CREATE EXTERNAL TABLE warehouse.row_del.t \
         (id BIGINT NOT NULL) STORED AS ICEBERG \
         LOCATION '/warehouse/row_del/t'",
    )
    .await;
    execute_sql(&ctx, "INSERT INTO warehouse.row_del.t VALUES (1), (2), (3)").await;
    execute_sql(&ctx, "DELETE FROM warehouse.row_del.t WHERE id = 2").await;
    let n = execute_scalar_i64(&ctx, "SELECT COUNT(*) FROM warehouse.row_del.t").await;
    assert_eq!(n, 2);
}

#[tokio::test]
#[ignore = "UPDATE not implemented for iceberg-rust tables"]
async fn integration_df_update_table() {
    let ctx = boot_df_stack().await;
    execute_sql(&ctx, "CREATE SCHEMA warehouse.row_upd").await;
    execute_sql(
        &ctx,
        "CREATE EXTERNAL TABLE warehouse.row_upd.t \
         (id BIGINT NOT NULL, status STRING) STORED AS ICEBERG \
         LOCATION '/warehouse/row_upd/t'",
    )
    .await;
    execute_sql(
        &ctx,
        "INSERT INTO warehouse.row_upd.t VALUES (1, 'open'), (2, 'open')",
    )
    .await;
    execute_sql(
        &ctx,
        "UPDATE warehouse.row_upd.t SET status = 'closed' WHERE id = 1",
    )
    .await;
}

#[tokio::test]
#[ignore = "MERGE INTO not implemented for iceberg-rust tables"]
async fn integration_df_merge_into_table() {
    let ctx = boot_df_stack().await;
    execute_sql(&ctx, "CREATE SCHEMA warehouse.row_merge").await;
    execute_sql(
        &ctx,
        "CREATE EXTERNAL TABLE warehouse.row_merge.target \
         (id BIGINT NOT NULL, v STRING) STORED AS ICEBERG \
         LOCATION '/warehouse/row_merge/target'",
    )
    .await;
    execute_sql(
        &ctx,
        "CREATE EXTERNAL TABLE warehouse.row_merge.source \
         (id BIGINT NOT NULL, v STRING) STORED AS ICEBERG \
         LOCATION '/warehouse/row_merge/source'",
    )
    .await;
    execute_sql(
        &ctx,
        "MERGE INTO warehouse.row_merge.target t \
         USING warehouse.row_merge.source s ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET v = s.v \
         WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)",
    )
    .await;
}

// -- Time travel ------------------------------------------------------------
// Upstream `TestTimeTravel`. DataFusion's parser does not currently route
// `FOR VERSION AS OF` / `FOR TIMESTAMP AS OF` to a per-snapshot scan against
// an Iceberg table.

#[tokio::test]
#[ignore = "FOR VERSION AS OF not exposed by datafusion_iceberg planner"]
async fn integration_df_time_travel_by_snapshot_id() {
    let ctx = boot_df_stack().await;
    execute_sql(&ctx, "CREATE SCHEMA warehouse.tt").await;
    execute_sql(
        &ctx,
        "CREATE EXTERNAL TABLE warehouse.tt.t (id BIGINT NOT NULL) STORED AS ICEBERG \
         LOCATION '/warehouse/tt/t'",
    )
    .await;
    execute_sql(&ctx, "INSERT INTO warehouse.tt.t VALUES (1)").await;
    execute_sql(&ctx, "INSERT INTO warehouse.tt.t VALUES (2)").await;
    // The placeholder snapshot id `0` is intentional — wire-up will replace
    // it with the captured first-snapshot id once the feature lands.
    let n = execute_scalar_i64(
        &ctx,
        "SELECT COUNT(*) FROM warehouse.tt.t FOR VERSION AS OF 0",
    )
    .await;
    assert_eq!(n, 1);
}

// -- Branch / tag DDL -------------------------------------------------------
// Upstream `TestBranchDDL`, `TestTagDDL`. iceberg-rust does support branches
// at the metadata level but DataFusion's parser does not surface
// `ALTER TABLE ... CREATE BRANCH` / `CREATE TAG`.

#[tokio::test]
#[ignore = "CREATE BRANCH not exposed via DataFusion SQL"]
async fn integration_df_create_branch() {
    let ctx = boot_df_stack().await;
    execute_sql(&ctx, "CREATE SCHEMA warehouse.br").await;
    execute_sql(
        &ctx,
        "CREATE EXTERNAL TABLE warehouse.br.t (id BIGINT NOT NULL) STORED AS ICEBERG \
         LOCATION '/warehouse/br/t'",
    )
    .await;
    execute_sql(&ctx, "INSERT INTO warehouse.br.t VALUES (1)").await;
    execute_sql(&ctx, "ALTER TABLE warehouse.br.t CREATE BRANCH dev").await;
}

#[tokio::test]
#[ignore = "CREATE TAG not exposed via DataFusion SQL"]
async fn integration_df_create_tag() {
    let ctx = boot_df_stack().await;
    execute_sql(&ctx, "CREATE SCHEMA warehouse.tg").await;
    execute_sql(
        &ctx,
        "CREATE EXTERNAL TABLE warehouse.tg.t (id BIGINT NOT NULL) STORED AS ICEBERG \
         LOCATION '/warehouse/tg/t'",
    )
    .await;
    execute_sql(&ctx, "INSERT INTO warehouse.tg.t VALUES (1)").await;
    execute_sql(&ctx, "ALTER TABLE warehouse.tg.t CREATE TAG v1").await;
}

// -- Stored procedures (CALL) ----------------------------------------------
// Upstream `TestRollbackToSnapshotProcedure`, `TestExpireSnapshotsProcedure`
// etc. DataFusion does not expose a procedure-call syntax, and iceberg-rust
// does not yet have procedure analogues.

#[tokio::test]
#[ignore = "CALL <procedure> syntax not supported by DataFusion"]
async fn integration_df_call_rollback_to_snapshot_procedure() {
    let ctx = boot_df_stack().await;
    execute_sql(&ctx, "CREATE SCHEMA warehouse.proc").await;
    execute_sql(
        &ctx,
        "CREATE EXTERNAL TABLE warehouse.proc.t (id BIGINT NOT NULL) STORED AS ICEBERG \
         LOCATION '/warehouse/proc/t'",
    )
    .await;
    execute_sql(
        &ctx,
        "CALL warehouse.system.rollback_to_snapshot('warehouse.proc.t', 0)",
    )
    .await;
}

// -- Iceberg transform functions in SELECT ----------------------------------
// Upstream `TestSparkFunctions` / `TestIcebergFunctions`. iceberg-rust uses
// year/month/day/hour/bucket/truncate UDFs internally for pruning but does
// not register them as user-callable SQL functions in the DataFusion
// `SessionContext`. Wiring them up would let SELECT/PARTITIONED BY share a
// vocabulary.

#[tokio::test]
#[ignore = "year() Iceberg transform UDF not registered with DataFusion SessionContext"]
async fn integration_df_year_function() {
    let ctx = boot_df_stack().await;
    execute_sql(&ctx, "CREATE SCHEMA warehouse.tfn").await;
    execute_sql(
        &ctx,
        "CREATE EXTERNAL TABLE warehouse.tfn.t \
         (id BIGINT NOT NULL, d DATE NOT NULL) STORED AS ICEBERG \
         LOCATION '/warehouse/tfn/t'",
    )
    .await;
    execute_sql(
        &ctx,
        "INSERT INTO warehouse.tfn.t VALUES (1, DATE '2024-03-15'), (2, DATE '2025-01-01')",
    )
    .await;
    let n = execute_scalar_i64(
        &ctx,
        "SELECT COUNT(*) FROM warehouse.tfn.t WHERE year(d) = 2024",
    )
    .await;
    assert_eq!(n, 1);
}

#[tokio::test]
#[ignore = "bucket() Iceberg transform UDF not registered with DataFusion SessionContext"]
async fn integration_df_bucket_function() {
    let ctx = boot_df_stack().await;
    execute_sql(&ctx, "CREATE SCHEMA warehouse.tfn_b").await;
    execute_sql(
        &ctx,
        "CREATE EXTERNAL TABLE warehouse.tfn_b.t \
         (id BIGINT NOT NULL) STORED AS ICEBERG \
         LOCATION '/warehouse/tfn_b/t'",
    )
    .await;
    execute_sql(
        &ctx,
        "INSERT INTO warehouse.tfn_b.t VALUES (1), (2), (3), (4)",
    )
    .await;
    let n = execute_scalar_i64(
        &ctx,
        "SELECT COUNT(DISTINCT bucket(8, id)) FROM warehouse.tfn_b.t",
    )
    .await;
    assert!((1..=4).contains(&n));
}

#[tokio::test]
#[ignore = "truncate() Iceberg transform UDF not registered with DataFusion SessionContext"]
async fn integration_df_truncate_function() {
    let ctx = boot_df_stack().await;
    execute_sql(&ctx, "CREATE SCHEMA warehouse.tfn_tr").await;
    execute_sql(
        &ctx,
        "CREATE EXTERNAL TABLE warehouse.tfn_tr.t \
         (id BIGINT NOT NULL, value INT NOT NULL) STORED AS ICEBERG \
         LOCATION '/warehouse/tfn_tr/t'",
    )
    .await;
    execute_sql(
        &ctx,
        "INSERT INTO warehouse.tfn_tr.t VALUES (1, 17), (2, 23), (3, 105)",
    )
    .await;
    let n = execute_scalar_i64(
        &ctx,
        "SELECT COUNT(*) FROM warehouse.tfn_tr.t WHERE truncate(10, value) = 20",
    )
    .await;
    assert_eq!(n, 1, "value 23 truncated to 20");
}

// -- Metadata tables --------------------------------------------------------
// Upstream `TestMetadataTables`. iceberg-rust does not expose `<tbl>.history`
// / `<tbl>.snapshots` / `<tbl>.files` / etc. as queryable virtual tables in
// DataFusion's catalog.

#[tokio::test]
#[ignore = "Iceberg metadata tables not registered with DataFusion catalog"]
async fn integration_df_metadata_table_snapshots() {
    let ctx = boot_df_stack().await;
    execute_sql(&ctx, "CREATE SCHEMA warehouse.meta").await;
    execute_sql(
        &ctx,
        "CREATE EXTERNAL TABLE warehouse.meta.t (id BIGINT NOT NULL) STORED AS ICEBERG \
         LOCATION '/warehouse/meta/t'",
    )
    .await;
    execute_sql(&ctx, "INSERT INTO warehouse.meta.t VALUES (1)").await;
    let n = execute_scalar_i64(&ctx, "SELECT COUNT(*) FROM warehouse.meta.t.snapshots").await;
    assert_eq!(n, 1);
}

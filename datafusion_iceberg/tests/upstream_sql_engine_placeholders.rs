//! Placeholders for upstream SQL-engine integration tests.
//!
//! The upstream Apache Iceberg test suite includes scan-correctness +
//! predicate-pushdown + partition-pruning + write/delete/update/merge +
//! view + alter + statistics tests that exercise the Spark integration.
//! The duckdb-iceberg suite mirrors the same surface via sqllogictest
//! files in `test/sql/local/`. Both bodies of tests are SQL-engine-
//! oriented and have no direct Rust analog in iceberg-rust today.
//!
//! Each `#[rstest]` here carries one `#[case]` per upstream @Test
//! method or per .test file, so cargo test reports the same scenario
//! count the upstream suites enumerate.

use rstest::rstest;

// =============================================================================
// Section 1: End-to-end scan correctness
// =============================================================================

// Spark scan-related Java tests with > 0 methods. Most other rows in this
// section duplicate iceberg-rust-crate.md and are already pinned there.

// TestSparkV2Filters (25) + TestSparkFilters (6) = 31 --
#[rstest]
#[case(1)]
#[case(2)]
#[case(3)]
#[case(4)]
#[case(5)]
#[case(6)]
#[case(7)]
#[case(8)]
#[case(9)]
#[case(10)]
#[case(11)]
#[case(12)]
#[case(13)]
#[case(14)]
#[case(15)]
#[case(16)]
#[case(17)]
#[case(18)]
#[case(19)]
#[case(20)]
#[case(21)]
#[case(22)]
#[case(23)]
#[case(24)]
#[case(25)]
#[case(26)]
#[case(27)]
#[case(28)]
#[case(29)]
#[case(30)]
#[case(31)]
#[ignore = "no Spark SQL filter translation analog: DataFusion logical-expression -> Iceberg predicate (31 upstream scenarios)"]
fn test_spark_filter_translation_scenarios(#[case] _scenario: usize) {
    unimplemented!("Spark filter translation");
}

// TestSparkExecutorCache (19) --
#[rstest]
#[case(1)]
#[case(2)]
#[case(3)]
#[case(4)]
#[case(5)]
#[case(6)]
#[case(7)]
#[case(8)]
#[case(9)]
#[case(10)]
#[case(11)]
#[case(12)]
#[case(13)]
#[case(14)]
#[case(15)]
#[case(16)]
#[case(17)]
#[case(18)]
#[case(19)]
#[ignore = "no per-executor manifest cache"]
fn test_spark_executor_cache_scenarios(#[case] _scenario: usize) {
    unimplemented!("SparkExecutorCache");
}

// TestComputeTableStatsAction (19) + TestComputeTableStatsProcedure (6) = 25 --
#[rstest]
#[case(1)]
#[case(2)]
#[case(3)]
#[case(4)]
#[case(5)]
#[case(6)]
#[case(7)]
#[case(8)]
#[case(9)]
#[case(10)]
#[case(11)]
#[case(12)]
#[case(13)]
#[case(14)]
#[case(15)]
#[case(16)]
#[case(17)]
#[case(18)]
#[case(19)]
#[case(20)]
#[case(21)]
#[case(22)]
#[case(23)]
#[case(24)]
#[case(25)]
#[ignore = "no compute-table-stats action + procedure (Spark `CALL`)"]
fn test_compute_table_stats_scenarios(#[case] _scenario: usize) {
    unimplemented!("ComputeTableStats");
}

// TestSparkReadConf (4) + TestSparkWriteConf (33) = 37 --
#[rstest]
#[case(1)]
#[case(2)]
#[case(3)]
#[case(4)]
#[case(5)]
#[case(6)]
#[case(7)]
#[case(8)]
#[case(9)]
#[case(10)]
#[case(11)]
#[case(12)]
#[case(13)]
#[case(14)]
#[case(15)]
#[case(16)]
#[case(17)]
#[case(18)]
#[case(19)]
#[case(20)]
#[case(21)]
#[case(22)]
#[case(23)]
#[case(24)]
#[case(25)]
#[case(26)]
#[case(27)]
#[case(28)]
#[case(29)]
#[case(30)]
#[case(31)]
#[case(32)]
#[case(33)]
#[case(34)]
#[case(35)]
#[case(36)]
#[case(37)]
#[ignore = "no SparkReadConf / SparkWriteConf parsing (read/write conf merge from session + table)"]
fn test_spark_read_write_conf_scenarios(#[case] _scenario: usize) {
    unimplemented!("SparkReadConf + SparkWriteConf");
}

// =============================================================================
// Section 2: Predicate pushdown + residuals (Spark-side extensions)
// =============================================================================

// TestInclusiveMetricsEvaluatorWithExtract (24) + TestInclusiveMetricsEvaluatorWithTransforms (22) = 46 --
#[rstest]
#[case(1)]
#[case(2)]
#[case(3)]
#[case(4)]
#[case(5)]
#[case(6)]
#[case(7)]
#[case(8)]
#[case(9)]
#[case(10)]
#[case(11)]
#[case(12)]
#[case(13)]
#[case(14)]
#[case(15)]
#[case(16)]
#[case(17)]
#[case(18)]
#[case(19)]
#[case(20)]
#[case(21)]
#[case(22)]
#[case(23)]
#[case(24)]
#[case(25)]
#[case(26)]
#[case(27)]
#[case(28)]
#[case(29)]
#[case(30)]
#[case(31)]
#[case(32)]
#[case(33)]
#[case(34)]
#[case(35)]
#[case(36)]
#[case(37)]
#[case(38)]
#[case(39)]
#[case(40)]
#[case(41)]
#[case(42)]
#[case(43)]
#[case(44)]
#[case(45)]
#[case(46)]
#[ignore = "no InclusiveMetricsEvaluator extensions (extract on struct/map, predicate on transformed column)"]
fn test_inclusive_metrics_evaluator_extensions_scenarios(#[case] _scenario: usize) {
    unimplemented!("InclusiveMetricsEvaluator extensions");
}

// =============================================================================
// Section 5: Writes — Spark DML extensions
// =============================================================================

// TestDelete (41) + TestMergeOnReadMerge (4) + TestCopyOnWriteMerge (2) +
// TestCopyOnWriteDelete (4) + TestCopyOnWriteUpdate (3) = 54 --
#[rstest]
#[case(1)]
#[case(2)]
#[case(3)]
#[case(4)]
#[case(5)]
#[case(6)]
#[case(7)]
#[case(8)]
#[case(9)]
#[case(10)]
#[case(11)]
#[case(12)]
#[case(13)]
#[case(14)]
#[case(15)]
#[case(16)]
#[case(17)]
#[case(18)]
#[case(19)]
#[case(20)]
#[case(21)]
#[case(22)]
#[case(23)]
#[case(24)]
#[case(25)]
#[case(26)]
#[case(27)]
#[case(28)]
#[case(29)]
#[case(30)]
#[case(31)]
#[case(32)]
#[case(33)]
#[case(34)]
#[case(35)]
#[case(36)]
#[case(37)]
#[case(38)]
#[case(39)]
#[case(40)]
#[case(41)]
#[case(42)]
#[case(43)]
#[case(44)]
#[case(45)]
#[case(46)]
#[case(47)]
#[case(48)]
#[case(49)]
#[case(50)]
#[case(51)]
#[case(52)]
#[case(53)]
#[case(54)]
#[ignore = "no Spark DML extension semantics (merge-on-read vs copy-on-write Delete/Update/Merge with V3 row-lineage)"]
fn test_spark_dml_extensions_scenarios(#[case] _scenario: usize) {
    unimplemented!("Spark DML extensions");
}

// =============================================================================
// duckdb-iceberg sqllogictest suite
// =============================================================================
//
// The duckdb-iceberg suite has ~115 .test sqllogictest files. We pin each
// file as one rstest case so the test count matches the upstream file count.
// File names are kept inside the comment block since case labels are positional.

// Section 1 read-side: 22 files (test_v1_manifest_entry, case_sensitive_names,
// null_stats, missing_map_bounds, nested_types, test_basic_deletion_vectors,
// moved_positional_delete_path, test_iceberg_deletes_scan_does_not_exist,
// test_reading_partitioned_table_with_bad_stats, equality_deletes,
// generated_bounds, unknown_puffin, version_name_format_error,
// iceberg_scans/* (whole suite ≈8 files), catalog_test_config_setup/reads/* (~1 file))
#[rstest]
#[case(1)]
#[case(2)]
#[case(3)]
#[case(4)]
#[case(5)]
#[case(6)]
#[case(7)]
#[case(8)]
#[case(9)]
#[case(10)]
#[case(11)]
#[case(12)]
#[case(13)]
#[case(14)]
#[case(15)]
#[case(16)]
#[case(17)]
#[case(18)]
#[case(19)]
#[case(20)]
#[case(21)]
#[case(22)]
#[ignore = "duckdb-iceberg read-side suite: v1 manifest, case sensitivity, null/missing stats, nested types, deletion vectors, equality deletes, generated bounds, unknown puffin, version-hint formatting, iceberg_scans/* + cross-catalog reads"]
fn test_duckdb_iceberg_read_scenarios(#[case] _scenario: usize) {
    unimplemented!("duckdb-iceberg read suite");
}

// Section 2 predicate pushdown: 8 files
#[rstest]
#[case(1)]
#[case(2)]
#[case(3)]
#[case(4)]
#[case(5)]
#[case(6)]
#[case(7)]
#[case(8)]
#[ignore = "duckdb-iceberg predicate pushdown: bucket pushdown, IN-list partition pruning, day/month-partitioned reads, truncate-partition pruning per type, struct-filter, bucket/truncate SQL functions"]
fn test_duckdb_iceberg_predicate_pushdown_scenarios(#[case] _scenario: usize) {
    unimplemented!("duckdb-iceberg predicate pushdown");
}

// Section 3 partition pruning + partition writes: ~50 files
#[rstest]
#[case(1)]
#[case(2)]
#[case(3)]
#[case(4)]
#[case(5)]
#[case(6)]
#[case(7)]
#[case(8)]
#[case(9)]
#[case(10)]
#[case(11)]
#[case(12)]
#[case(13)]
#[case(14)]
#[case(15)]
#[case(16)]
#[case(17)]
#[case(18)]
#[case(19)]
#[case(20)]
#[case(21)]
#[case(22)]
#[case(23)]
#[case(24)]
#[case(25)]
#[case(26)]
#[case(27)]
#[case(28)]
#[case(29)]
#[case(30)]
#[case(31)]
#[case(32)]
#[case(33)]
#[case(34)]
#[case(35)]
#[case(36)]
#[case(37)]
#[case(38)]
#[case(39)]
#[case(40)]
#[case(41)]
#[case(42)]
#[case(43)]
#[case(44)]
#[case(45)]
#[case(46)]
#[case(47)]
#[case(48)]
#[case(49)]
#[case(50)]
#[ignore = "duckdb-iceberg partition pruning + writes: bucket (11 type variants), truncate (5), identity (4), temporal (5 + pre-epoch), multi-insert (6), null partition values (4), multi-field combined (3), large-batch stress, schema-evolution partition writes, unsupported rejection, partition UPDATE"]
fn test_duckdb_iceberg_partition_write_scenarios(#[case] _scenario: usize) {
    unimplemented!("duckdb-iceberg partition writes");
}

// Section 4 time travel + snapshots: 2 files
#[rstest]
#[case(1)]
#[case(2)]
#[ignore = "duckdb-iceberg time travel: pre-delete snapshot, pre-add-column schema"]
fn test_duckdb_iceberg_time_travel_scenarios(#[case] _scenario: usize) {
    unimplemented!("duckdb-iceberg time travel");
}

// Section 5 writes (insert/delete/update/merge): ~30 files
#[rstest]
#[case(1)]
#[case(2)]
#[case(3)]
#[case(4)]
#[case(5)]
#[case(6)]
#[case(7)]
#[case(8)]
#[case(9)]
#[case(10)]
#[case(11)]
#[case(12)]
#[case(13)]
#[case(14)]
#[case(15)]
#[case(16)]
#[case(17)]
#[case(18)]
#[case(19)]
#[case(20)]
#[case(21)]
#[case(22)]
#[case(23)]
#[case(24)]
#[case(25)]
#[case(26)]
#[case(27)]
#[case(28)]
#[case(29)]
#[case(30)]
#[ignore = "duckdb-iceberg writes: geometry INSERT (V3), DELETE shapes including consolidation/multi-file/rowid/transactional/empty/V3, MERGE INTO variants, partition UPDATE, transactional multi-statement"]
fn test_duckdb_iceberg_write_scenarios(#[case] _scenario: usize) {
    unimplemented!("duckdb-iceberg writes");
}

// Section 6 CREATE/DROP/ALTER: ~30 files
#[rstest]
#[case(1)]
#[case(2)]
#[case(3)]
#[case(4)]
#[case(5)]
#[case(6)]
#[case(7)]
#[case(8)]
#[case(9)]
#[case(10)]
#[case(11)]
#[case(12)]
#[case(13)]
#[case(14)]
#[case(15)]
#[case(16)]
#[case(17)]
#[case(18)]
#[case(19)]
#[case(20)]
#[case(21)]
#[case(22)]
#[case(23)]
#[case(24)]
#[case(25)]
#[case(26)]
#[case(27)]
#[case(28)]
#[case(29)]
#[case(30)]
#[ignore = "duckdb-iceberg DDL: CREATE TABLE shapes + properties + CTAS + REST requests, default values per primitive, schema/namespace lifecycle, DROP TABLE, ALTER add/drop column + nested field + type evolution + default-value evolution + property update + format-version upgrade + PARTITION BY ALTER + rename table + rename column + add/drop field + alter type/default/rename"]
fn test_duckdb_iceberg_ddl_scenarios(#[case] _scenario: usize) {
    unimplemented!("duckdb-iceberg DDL");
}

// Section 9 catalog-specific SQL: ~20 files
#[rstest]
#[case(1)]
#[case(2)]
#[case(3)]
#[case(4)]
#[case(5)]
#[case(6)]
#[case(7)]
#[case(8)]
#[case(9)]
#[case(10)]
#[case(11)]
#[case(12)]
#[case(13)]
#[case(14)]
#[case(15)]
#[case(16)]
#[case(17)]
#[case(18)]
#[case(19)]
#[case(20)]
#[ignore = "duckdb-iceberg catalog-specific SQL: eager refresh, REST loadTable shape, minimal HEAD requests, USE schema, SHOW TABLES dedup, drop+create transactional, parquet COPY options, CASCADE, TPC-H, empty table SELECT, target-file-size, IRC-specific, V3 row lineage + variant, custom catalog (OAuth2, setup errors, HTTP logging, proxy, attach options, nested namespaces, max-table-staleness, nessie / lakekeeper / sigv4 mocks)"]
fn test_duckdb_iceberg_catalog_specific_scenarios(#[case] _scenario: usize) {
    unimplemented!("duckdb-iceberg catalog-specific");
}

//! Placeholder home for an eventual `expressions` module.
//!
//! Predicate / binding / projection / evaluation logic lives partly in
//! `iceberg-rust/src/arrow/` and partly in callers today. iceberg-rust-spec
//! has no dedicated `expressions` module: no `Expression` AST, no
//! `BoundPredicate`, no `Accessor` / `Evaluator` / `InclusiveManifestEvaluator`
//! / `InclusiveMetricsEvaluator` / `StrictMetricsEvaluator` /
//! `AggregateEvaluator`, no `Projections::inclusive` / `Projections::strict`,
//! no `Residuals`, no `BoundingBox` / `GeospatialBound` / geospatial predicate
//! evaluators, no MetricsConfig.
//!
//! The placeholders below pin the upstream test contract one cluster at a
//! time. Each `#[rstest]` exposes one `#[ignore]`d case per upstream `@Test`
//! method, so cargo test reports the same count of expected scenarios that
//! the upstream test class enumerates.

#[cfg(test)]
mod tests {
    use rstest::rstest;

    // -- TestExpressionBinding (36 scenarios) --
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
    #[ignore = "no expressions::Expression binding pipeline (missing ref, bound-fails, case-sensitivity, AND/OR/NOT, startsWith, transforms, is_null/not_null with required+optional+transformed-order-preserving+non-preserving+variant, extract by path, nested struct paths)"]
    fn test_expression_binding_scenarios(#[case] _scenario: usize) {
        unimplemented!("expressions::Expression binding");
    }

    // -- TestPredicateBinding (24 scenarios) --
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
    #[ignore = "no expressions::Predicate binding (multi-field, missing-field, comparison + prefix, literal conversion + invalid + numeric narrowing, is_null/not_null/is_nan/not_nan, In/NotIn binding + conversion + dedup + to-Eq/NotEq/expression)"]
    fn test_predicate_binding_scenarios(#[case] _scenario: usize) {
        unimplemented!("expressions::Predicate binding");
    }

    // -- TestExpressionParser (12 scenarios) --
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
    #[ignore = "no expressions::Expression JSON parser (simple, transform, predicate variants, AND/OR/NOT, nested, fixed/decimal literals)"]
    fn test_expression_parser_scenarios(#[case] _scenario: usize) {
        unimplemented!("expressions::Expression JSON parser");
    }

    // -- TestExpressionSerialization (1 scenario) --
    #[test]
    #[ignore = "no Expression serialization round-trip"]
    fn test_expression_serialization_round_trip() {
        // Java analog is Java-Serialization; Rust analog is serde round-trip of a typed
        // Expression tree. Whole pipeline absent.
        unimplemented!("expressions serialization round-trip");
    }

    // -- TestExpressionHelpers (13 scenarios) --
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
    #[ignore = "no expressions::Expressions helpers (millis/micros/nanos timestamp literals, simplify And/Or/Not, rewrite Not, transform expressions, null name + value expr, multi-and, NaN-input validation)"]
    fn test_expression_helpers_scenarios(#[case] _scenario: usize) {
        unimplemented!("expressions::Expressions helpers");
    }

    // -- TestExpressionUtil (47 scenarios) --
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
    #[ignore = "no expressions::ExpressionUtil (sanitize predicates, sanitize all primitive literals including timestamps about now/past/last-week/future, extract by id inclusive, equivalence checks, selects-partitions, variant sanitization)"]
    fn test_expression_util_scenarios(#[case] _scenario: usize) {
        unimplemented!("expressions::ExpressionUtil");
    }

    // -- TestAccessors (18 scenarios) --
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
    #[ignore = "no expressions::Accessor (per-primitive accessor for nested fields by id-path: 13 primitives + list/map + struct-as-object + empty-struct/schema)"]
    fn test_accessor_scenarios(#[case] _scenario: usize) {
        unimplemented!("expressions::Accessor");
    }

    // -- TestEvaluator (24 scenarios) --
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
    #[ignore = "no expressions::Evaluator over a struct row (all binary comparisons, startsWith, alwaysTrue/False, is_null/not_null, isNan/notNaN, AND/OR/NOT, case-sensitivity, CharSeq, In/NotIn + exception paths)"]
    fn test_evaluator_scenarios(#[case] _scenario: usize) {
        unimplemented!("expressions::Evaluator");
    }

    // -- TestProjection (9 scenarios) --
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
    #[ignore = "no expressions::Projections::inclusive / Projections::strict (identity, timestamp-nanos identity, case sensitivity, strict variants, bad spark filter rejection, projection field-name output)"]
    fn test_projection_scenarios(#[case] _scenario: usize) {
        unimplemented!("expressions::Projections");
    }

    // -- TestResiduals (10 scenarios) --
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
    #[ignore = "no expressions::Residuals (identity transform + case sensitivity, unpartitioned, In/InTimestamp, NotIn/NotInTimestamp, IsNaN/NotNaN)"]
    fn test_residuals_scenarios(#[case] _scenario: usize) {
        unimplemented!("expressions::Residuals");
    }

    // -- TestInclusiveManifestEvaluator (24 scenarios) --
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
    #[ignore = "no expressions::InclusiveManifestEvaluator (all-nulls / no-nulls, NaN handling, missing column / stats, AND/OR/NOT, integer Lt/LtEq/Gt/GtEq/Eq/NotEq + rewritten + case sensitivity, string startsWith/notStartsWith, In/NotIn, NotEq + NotIn single value)"]
    fn test_inclusive_manifest_evaluator_scenarios(#[case] _scenario: usize) {
        unimplemented!("expressions::InclusiveManifestEvaluator");
    }

    // -- TestInclusiveMetricsEvaluator (28 scenarios) --
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
    #[ignore = "no expressions::InclusiveMetricsEvaluator (all-nulls / no-nulls, NaN handling, required + missing column / missing stats, zero-record file, AND/OR/NOT, integer Lt/LtEq/Gt/GtEq/Eq/NotEq + rewritten + case sensitivity, string startsWith/notStartsWith, In/NotIn, is_null/not_null in nested struct, NotEq + NotIn single value)"]
    fn test_inclusive_metrics_evaluator_scenarios(#[case] _scenario: usize) {
        unimplemented!("expressions::InclusiveMetricsEvaluator");
    }

    // -- TestStrictMetricsEvaluator (45 scenarios) --
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
    #[ignore = "no expressions::StrictMetricsEvaluator (all-nulls / no-nulls / some-nulls, NaN handling, required/missing column + missing stats, zero-record file, AND/OR/NOT, integer Lt/LtEq/Gt/GtEq/Eq/NotEq + rewritten, In/NotIn, nested-column evaluation, extensive startsWith / notStartsWith bound matching covering equal/longer/shorter prefix vs bounds + nested column)"]
    fn test_strict_metrics_evaluator_scenarios(#[case] _scenario: usize) {
        unimplemented!("expressions::StrictMetricsEvaluator");
    }

    // -- TestMetricsEvaluatorsNaNHandling (12 scenarios) --
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
    #[ignore = "no expressions::*MetricsEvaluator NaN handling (Lt/LtEq, Gt/GtEq, Eq, NotEq, In, NotIn across inclusive + strict variants)"]
    fn test_metrics_evaluators_nan_handling_scenarios(#[case] _scenario: usize) {
        unimplemented!("expressions metrics NaN handling");
    }

    // -- TestMetricsConfig (3 scenarios) --
    #[rstest]
    #[case(1)]
    #[case(2)]
    #[case(3)]
    #[ignore = "no MetricsConfig (per-column metrics mode none/counts/truncate(N)/full + nested struct/map/list-of-maps inheritance)"]
    fn test_metrics_config_scenarios(#[case] _scenario: usize) {
        unimplemented!("MetricsConfig");
    }

    // -- TestStartsWith (2 scenarios) --
    #[rstest]
    #[case(1)]
    #[case(2)]
    #[ignore = "no expressions startsWith + truncate projection interaction"]
    fn test_starts_with_scenarios(#[case] _scenario: usize) {
        unimplemented!("expressions startsWith");
    }

    // -- TestNotStartsWith (6 scenarios) --
    #[rstest]
    #[case(1)]
    #[case(2)]
    #[case(3)]
    #[case(4)]
    #[case(5)]
    #[case(6)]
    #[ignore = "no expressions notStartsWith + truncate projection + strict/inclusive metrics evaluators"]
    fn test_not_starts_with_scenarios(#[case] _scenario: usize) {
        unimplemented!("expressions notStartsWith");
    }

    // -- TestAggregateBinding (6 scenarios) --
    #[rstest]
    #[case(1)]
    #[case(2)]
    #[case(3)]
    #[case(4)]
    #[case(5)]
    #[case(6)]
    #[ignore = "no expressions::Aggregate binding (count/min/max binding, count-star, bound-aggregate-fails, case sensitivity, missing field)"]
    fn test_aggregate_binding_scenarios(#[case] _scenario: usize) {
        unimplemented!("expressions::Aggregate binding");
    }

    // -- TestAggregateEvaluator (7 scenarios) --
    #[rstest]
    #[case(1)]
    #[case(2)]
    #[case(3)]
    #[case(4)]
    #[case(5)]
    #[case(6)]
    #[case(7)]
    #[ignore = "no expressions::AggregateEvaluator (int aggregate, all-nulls / some-nulls / no-stats, all-missing-stats, optional column all-missing-stats, missing-some-stats)"]
    fn test_aggregate_evaluator_scenarios(#[case] _scenario: usize) {
        unimplemented!("expressions::AggregateEvaluator");
    }

    // -- TestBoundingBox (7 scenarios) --
    #[rstest]
    #[case(1)]
    #[case(2)]
    #[case(3)]
    #[case(4)]
    #[case(5)]
    #[case(6)]
    #[case(7)]
    #[ignore = "no V3 BoundingBox value (constructor + accessors, ByteBuffer construct + parse + non-zero position, equality + hash, Display, serde round-trip)"]
    fn test_bounding_box_scenarios(#[case] _scenario: usize) {
        unimplemented!("BoundingBox");
    }

    // -- TestGeospatialBound (10 scenarios) --
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
    #[ignore = "no V3 GeospatialBound (XY / XYZ / XYM / XYZM constructors, equality + hash, Display + simple string, serde, ByteBuffer parse, round-trip serde)"]
    fn test_geospatial_bound_scenarios(#[case] _scenario: usize) {
        unimplemented!("GeospatialBound");
    }

    // -- TestPathUtil (6 scenarios) --
    #[rstest]
    #[case(1)]
    #[case(2)]
    #[case(3)]
    #[case(4)]
    #[case(5)]
    #[case(6)]
    #[ignore = "no V3 PathUtil (restricted JSONPath parser: $ root + .name segments, rejects brackets/wildcards/recursive-descent/positions/digit-leading/lone-surrogates; toNormalizedPath emits RFC9535 brackets; rfc9535escape per special chars)"]
    fn test_path_util_scenarios(#[case] _scenario: usize) {
        unimplemented!("V3 PathUtil");
    }

    // -- TestGeospatialPredicateEvaluators (21 scenarios) --
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
    #[ignore = "no V3 geospatial predicate evaluators (geometry / geography / unsupported type, intersect / touch-at-corner / touch-at-edge / contained, Z + M coordinates, invalid ranges, geography wrap-around + boundaries + invalid lat/long/ZM, mixed dimensions XY vs XYZ vs XYM vs XYZM)"]
    fn test_geospatial_predicate_evaluator_scenarios(#[case] _scenario: usize) {
        unimplemented!("geospatial predicate evaluators");
    }
}

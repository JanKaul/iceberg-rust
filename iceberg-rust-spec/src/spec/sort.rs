/*!
 * Sort order specification for Iceberg tables
 *
 * This module defines the sort ordering capabilities of Iceberg tables, including:
 * - Sort direction (ascending/descending)
 * - Null ordering (nulls first/last)
 * - Sort fields that specify which columns to sort by
 * - Sort order specifications that combine multiple sort fields
 *
 * Sort orders are used to:
 * - Optimize data layout for efficient querying
 * - Support range predicates and partition pruning
 * - Enable merge-on-read operations
 */

use std::{fmt, str};

use derive_builder::Builder;
use serde::{Deserialize, Serialize};

use crate::error::Error;

use super::partition::Transform;

pub static DEFAULT_SORT_ORDER_ID: i32 = 0;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
/// Sort direction in a partition, either ascending or descending
pub enum SortDirection {
    /// Ascending
    #[serde(rename = "asc")]
    Ascending,
    /// Descending
    #[serde(rename = "desc")]
    Descending,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
/// Describes the order of null values when sorted.
pub enum NullOrder {
    #[serde(rename = "nulls-first")]
    /// Nulls are stored first
    First,
    #[serde(rename = "nulls-last")]
    /// Nulls are stored last
    Last,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "kebab-case")]
/// Entry for every column that is to be sorted
pub struct SortField {
    /// A source column id from the table’s schema
    pub source_id: i32,
    /// A transform that is used to produce values to be sorted on from the source column.
    pub transform: Transform,
    /// A sort direction, that can only be either asc or desc
    pub direction: SortDirection,
    /// A null order that describes the order of null values when sorted.
    pub null_order: NullOrder,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone, Default, Builder)]
#[serde(rename_all = "kebab-case")]
#[builder(setter(prefix = "with"))]
/// A sort order is defined by a sort order id and a list of sort fields.
/// The order of the sort fields within the list defines the order in which the sort is applied to the data.
pub struct SortOrder {
    /// Identifier for SortOrder, order_id `0` is no sort order.
    #[builder(default = "DEFAULT_SORT_ORDER_ID")]
    pub order_id: i32,
    #[builder(setter(each(name = "with_sort_field")))]
    /// Details of the sort
    pub fields: Vec<SortField>,
}

impl fmt::Display for SortOrder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            &serde_json::to_string(self).map_err(|_| fmt::Error)?,
        )
    }
}

impl str::FromStr for SortOrder {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        serde_json::from_str(s).map_err(Error::from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sort_field() {
        let sort_field = r#"
        {
            "transform": "bucket[4]",
            "source-id": 3,
            "direction": "desc",
            "null-order": "nulls-last"
         }
        "#;

        let field: SortField = serde_json::from_str(sort_field).unwrap();
        assert_eq!(Transform::Bucket(4), field.transform);
        assert_eq!(3, field.source_id);
        assert_eq!(SortDirection::Descending, field.direction);
        assert_eq!(NullOrder::Last, field.null_order);
    }

    #[test]
    fn sort_order() {
        let sort_order = r#"
        {
        "order-id": 1,
        "fields": [ {
            "transform": "identity",
            "source-id": 2,
            "direction": "asc",
            "null-order": "nulls-first"
         }, {
            "transform": "bucket[4]",
            "source-id": 3,
            "direction": "desc",
            "null-order": "nulls-last"
         } ]
        }
        "#;

        let order: SortOrder = serde_json::from_str(sort_order).unwrap();
        assert_eq!(Transform::Identity, order.fields[0].transform);
        assert_eq!(2, order.fields[0].source_id);
        assert_eq!(SortDirection::Ascending, order.fields[0].direction);
        assert_eq!(NullOrder::First, order.fields[0].null_order);

        assert_eq!(Transform::Bucket(4), order.fields[1].transform);
        assert_eq!(3, order.fields[1].source_id);
        assert_eq!(SortDirection::Descending, order.fields[1].direction);
        assert_eq!(NullOrder::Last, order.fields[1].null_order);
    }

    // --- JSON round-trip and rejection -----------------------------------

    #[test]
    fn test_default_sort_order_is_empty_with_id_zero() {
        // Iceberg reserves order-id 0 for the unsorted order; in Rust this is
        // also what `SortOrder::default()` produces.
        let order = SortOrder::default();
        assert_eq!(order.order_id, DEFAULT_SORT_ORDER_ID);
        assert!(order.fields.is_empty());

        let empty_json = r#"{ "order-id": 0, "fields": [] }"#;
        let parsed: SortOrder = serde_json::from_str(empty_json).unwrap();
        assert_eq!(parsed, order);
    }

    #[test]
    fn test_sort_order_json_round_trip_covers_every_transform_and_modifier() {
        // One SortOrder that exercises every Transform variant and both
        // SortDirection / NullOrder values. Re-serializing then re-parsing
        // must yield the same struct.
        let json = r#"{
            "order-id": 7,
            "fields": [
                { "source-id": 11, "transform": "identity",     "direction": "asc",  "null-order": "nulls-first" },
                { "source-id": 12, "transform": "bucket[16]",   "direction": "desc", "null-order": "nulls-last"  },
                { "source-id": 13, "transform": "truncate[8]",  "direction": "asc",  "null-order": "nulls-last"  },
                { "source-id": 14, "transform": "year",         "direction": "desc", "null-order": "nulls-first" },
                { "source-id": 14, "transform": "month",        "direction": "asc",  "null-order": "nulls-first" },
                { "source-id": 14, "transform": "day",          "direction": "desc", "null-order": "nulls-last"  },
                { "source-id": 14, "transform": "hour",         "direction": "asc",  "null-order": "nulls-last"  },
                { "source-id": 15, "transform": "void",         "direction": "desc", "null-order": "nulls-first" }
            ]
        }"#;

        let order: SortOrder = serde_json::from_str(json).unwrap();
        assert_eq!(order.order_id, 7);
        assert_eq!(
            order
                .fields
                .iter()
                .map(|f| f.transform.clone())
                .collect::<Vec<_>>(),
            vec![
                Transform::Identity,
                Transform::Bucket(16),
                Transform::Truncate(8),
                Transform::Year,
                Transform::Month,
                Transform::Day,
                Transform::Hour,
                Transform::Void,
            ],
        );
        // source-ids must come out in input order (Vec preserves position).
        assert_eq!(
            order.fields.iter().map(|f| f.source_id).collect::<Vec<_>>(),
            vec![11, 12, 13, 14, 14, 14, 14, 15],
        );

        let re_serialized = serde_json::to_string(&order).unwrap();
        let order_again: SortOrder = serde_json::from_str(&re_serialized).unwrap();
        assert_eq!(order_again, order);
    }

    #[test]
    fn test_sort_order_rejects_invalid_direction_value() {
        let json = r#"{
            "order-id": 1,
            "fields": [
                { "source-id": 1, "transform": "identity", "direction": "sideways", "null-order": "nulls-first" }
            ]
        }"#;
        assert!(serde_json::from_str::<SortOrder>(json).is_err());
    }

    #[test]
    fn test_sort_order_rejects_invalid_null_order_value() {
        let json = r#"{
            "order-id": 1,
            "fields": [
                { "source-id": 1, "transform": "identity", "direction": "asc", "null-order": "nulls-middle" }
            ]
        }"#;
        assert!(serde_json::from_str::<SortOrder>(json).is_err());
    }

    #[test]
    fn test_sort_order_rejects_unknown_transform_string() {
        // Java accepts unknown transforms via an `UnknownTransform` carrier
        // variant; Rust enumerates known variants and rejects anything else.
        // This test pins the current Rust behavior so a future `Transform`
        // extension is an opt-in decision rather than a silent change.
        let json = r#"{
            "order-id": 10,
            "fields": [
                { "source-id": 2, "transform": "custom_transform", "direction": "desc", "null-order": "nulls-first" }
            ]
        }"#;
        assert!(serde_json::from_str::<SortOrder>(json).is_err());
    }

    #[test]
    fn test_sort_order_display_and_fromstr_round_trip() {
        let order = SortOrder {
            order_id: 5,
            fields: vec![SortField {
                source_id: 9,
                transform: Transform::Bucket(32),
                direction: SortDirection::Descending,
                null_order: NullOrder::Last,
            }],
        };
        let rendered = order.to_string();
        let parsed: SortOrder = rendered.parse().unwrap();
        assert_eq!(parsed, order);
    }

    #[test]
    fn test_sort_order_builder_produces_expected_struct() {
        let order = SortOrderBuilder::default()
            .with_order_id(3)
            .with_sort_field(SortField {
                source_id: 1,
                transform: Transform::Identity,
                direction: SortDirection::Ascending,
                null_order: NullOrder::First,
            })
            .with_sort_field(SortField {
                source_id: 2,
                transform: Transform::Truncate(4),
                direction: SortDirection::Descending,
                null_order: NullOrder::Last,
            })
            .build()
            .unwrap();

        assert_eq!(order.order_id, 3);
        assert_eq!(order.fields.len(), 2);
        assert_eq!(order.fields[0].transform, Transform::Identity);
        assert_eq!(order.fields[1].transform, Transform::Truncate(4));
        assert_eq!(order.fields[1].direction, SortDirection::Descending);
    }

    // --- TestSortOrderUtil port --------------------------------------------
    //
    // Java's `org.apache.iceberg.util.SortOrderUtil` exposes two helpers:
    //
    //   1. `buildSortOrder(schema, spec, order) -> SortOrder`
    //      Prepends each partition-spec field to `order` as an ascending
    //      clustering column, unless the existing `order` already
    //      "satisfies" that partition field's transform (e.g. a sort on
    //      `ts` satisfies `days(ts)` because days is monotonic in ts).
    //
    //   2. `findTableSortOrder(table, userOrder) -> SortOrder`
    //      Walks the table's historical `sortOrders` map and returns the
    //      first entry whose fields match `userOrder`'s fields; falls
    //      back to `SortOrder.unsorted()` if none match.
    //
    // Rust has no SortOrderUtil module. The data structures (SortOrder,
    // SortField, PartitionSpec, TableMetadata.sort_orders / default_sort_order_id)
    // all exist, but no reducer combines them. All 15 Java scenarios are
    // pinned `#[ignore]` here so the eventual
    // `sort_order_util::build_sort_order(&Schema, &PartitionSpec, &SortOrder) -> SortOrder`
    // and `sort_order_util::find_table_sort_order(&TableMetadata, &SortOrder) -> SortOrder`
    // helpers have a ready spec.

    #[test]
    #[ignore = "feature gap: no sort_order_util::build_sort_order(); reducer that prepends partition spec as clustering columns is unimplemented"]
    fn test_build_sort_order_empty_spec_v1_preserves_existing_order() {
        // Spec: unpartitioned; Order: asc(id, NULLS_LAST).
        // Expected: order returned unchanged (no spec fields to prepend).
    }

    #[test]
    #[ignore = "feature gap: no sort_order_util::build_sort_order(); same behavior at format version 2"]
    fn test_build_sort_order_empty_spec_v2_preserves_existing_order() {
        // Spec: unpartitioned; Order: asc(id, NULLS_LAST), v2.
        // Expected: order returned unchanged.
    }

    #[test]
    #[ignore = "feature gap: no sort_order_util::build_sort_order(); should prepend day(ts) + identity(category) before desc(id)"]
    fn test_build_sort_order_prepends_partition_fields_when_sort_has_none() {
        // Spec:  day(ts), identity(category).
        // Order: desc(id).
        // Expected fields in order:
        //   asc(day(ts), nulls-first),
        //   asc(identity(category), nulls-first),
        //   desc(id, nulls-last).
    }

    #[test]
    #[ignore = "feature gap: no sort_order_util::build_sort_order(); should leave order unchanged when it already covers every spec field in spec order"]
    fn test_build_sort_order_when_sort_already_covers_all_partition_fields_in_order() {
        // Spec:  day(ts), identity(category).
        // Order: asc(day(ts)), asc(category), desc(id).
        // Expected: order returned unchanged.
    }

    #[test]
    #[ignore = "feature gap: no sort_order_util::build_sort_order(); should leave order unchanged even when spec fields appear in a different order"]
    fn test_build_sort_order_when_sort_covers_all_partition_fields_reordered() {
        // Spec:  identity(category), day(ts).
        // Order: asc(day(ts)), asc(category), desc(id).
        // Expected: order returned unchanged (only set membership matters).
    }

    #[test]
    #[ignore = "feature gap: no sort_order_util::build_sort_order(); should prepend missing spec field(s)"]
    fn test_build_sort_order_prepends_missing_partition_fields() {
        // Spec:  identity(category), day(ts).
        // Order: asc(category), desc(id) — day(ts) missing.
        // Expected fields in order:
        //   asc(day(ts)), asc(category), desc(id).
    }

    #[test]
    #[ignore = "feature gap: no sort_order_util::build_sort_order(); bare ts at any position satisfies days(ts) — order unchanged"]
    fn test_build_sort_order_satisfied_partition_at_tail_returns_unchanged() {
        // Spec:  identity(category), day(ts).
        // Order: asc(category), asc(ts), desc(id) — asc(ts) satisfies day(ts).
        // Expected: order returned unchanged.
    }

    #[test]
    #[ignore = "feature gap: no sort_order_util::build_sort_order(); bare ts as leading field satisfies day(ts) — prepend remaining spec field only"]
    fn test_build_sort_order_satisfied_partition_at_head_only_prepends_remaining() {
        // Spec:  day(ts), identity(category).
        // Order: asc(ts), asc(category), desc(id) — asc(ts) satisfies day(ts).
        // Expected fields in order:
        //   asc(category), asc(ts), asc(category), desc(id)
        // (Java keeps the original sort intact and only prepends the
        // partition fields whose transform isn't yet satisfied.)
    }

    #[test]
    #[ignore = "feature gap: no sort_order_util::build_sort_order(); duplicate of head-satisfied scenario, kept to mirror Java's coverage"]
    fn test_build_sort_order_satisfied_partition_fields_idempotent() {
        // Same fixture and expectation as the head-satisfied case.
    }

    #[test]
    #[ignore = "feature gap: no sort_order_util::build_sort_order(); spec with redundant time fields (day+hour on same ts) — bare ts satisfies both"]
    fn test_build_sort_order_with_redundant_partition_fields_returns_unchanged() {
        // Spec evolves: day(ts) + identity(category), then add hour(ts).
        // Order: asc(category), asc(ts), desc(id).
        // Expected: order returned unchanged — asc(ts) satisfies both
        // day(ts) and hour(ts).
    }

    #[test]
    #[ignore = "feature gap: no sort_order_util::build_sort_order(); void-transform partition fields are skipped; active fields prepended"]
    fn test_build_sort_order_with_redundant_partition_fields_missing_skips_void() {
        // Spec evolves: day(ts) + identity(category), then drop day(ts)
        // (void transform left in place) and add hour(ts).
        // Order: desc(id).
        // Expected fields in order:
        //   asc(category), asc(hour(ts)), desc(id)
        // — the void-transform entry must not be re-emitted.
    }

    #[test]
    #[ignore = "feature gap: no sort_order_util::find_table_sort_order(); should return the current sort order when user-supplied matches it"]
    fn test_find_sort_order_returns_current_order_when_matching() {
        // Table has a single non-unsorted sort order. find_table_sort_order
        // with that exact order should return it back.
    }

    #[test]
    #[ignore = "feature gap: no sort_order_util::find_table_sort_order(); should match by field shape even when caller omits order-id"]
    fn test_find_sort_order_matches_when_user_omits_order_id() {
        // User-supplied SortOrder has order-id 0 (default) but identical
        // fields to the table's current order. find_table_sort_order
        // should return the table's current order.
    }

    #[test]
    #[ignore = "feature gap: no sort_order_util::find_table_sort_order(); should match a historical sort order, not just the current one"]
    fn test_find_sort_order_matches_historical_order_not_just_current() {
        // Table has two sort orders: original asc(id, NULLS_LAST) and a
        // newer asc(data), desc(ts). find_table_sort_order with the
        // original's shape returns the original (historical) entry, not
        // the current one.
    }

    #[test]
    #[ignore = "feature gap: no sort_order_util::find_table_sort_order(); returns SortOrder::unsorted() when no historical order matches"]
    fn test_find_sort_order_returns_unsorted_when_no_match() {
        // User supplies desc(id, NULLS_LAST); table only has
        // asc(id, NULLS_LAST) and asc(data), desc(ts).
        // find_table_sort_order returns SortOrder::default() (order-id 0,
        // empty fields).
    }

    // --- TestSortOrderComparators port -------------------------------------
    //
    // Java's `SortOrderComparators.forSchema(schema, sortOrder) ->
    // Comparator<StructLike>` produces a row comparator that walks the
    // sort fields, applies the configured transform to each row's value
    // for that field, then compares pairwise honouring direction (asc/desc)
    // and null ordering (NULLS_FIRST/NULLS_LAST).
    //
    // Rust has no `SortOrderComparators` analog (`grep` across the
    // workspace finds zero matches) and no `StructLike` row abstraction
    // — rows live in arrow batches in the implementation layer. All 20
    // Java @Test scenarios are pinned `#[ignore]` here so an eventual
    // `sort_order_comparator::for_schema(&Schema, &SortOrder)` reducer
    // has a ready spec.
    //
    // Expected eventual Rust API (mirroring Java's surface):
    //   fn for_schema(&Schema, &SortOrder) -> impl Fn(&Struct, &Struct) -> std::cmp::Ordering;
    //
    // Shared contract pinned by every Java test:
    //   compare(less, less)           == Equal
    //   compare(greater, greater)     == Equal
    //   compare(less, lessCopy)       == Equal  (same sort-key bucket)
    //   ASC + NULLS_FIRST: less < greater; nullValue < less
    //   DESC + NULLS_LAST: less > greater; nullValue > greater
    //
    // Each Java test exercises the contract on a different value type
    // and/or transform.

    #[test]
    #[ignore = "feature gap: no sort_order_comparator; Boolean asc/desc + null-first / null-last sort"]
    fn test_sort_order_comparator_boolean_per_java() {
        // less = (id3, false); greater = (id2, true); lessCopy = (id1, false).
    }

    #[test]
    #[ignore = "feature gap: no sort_order_comparator; Integer asc/desc"]
    fn test_sort_order_comparator_int_per_java() {
        // less = (id3, 111); greater = (id2, 222); lessCopy = (id1, 111).
    }

    #[test]
    #[ignore = "feature gap: no sort_order_comparator; Long asc/desc"]
    fn test_sort_order_comparator_long_per_java() {
        // less = (id3, 111L); greater = (id2, 222L); lessCopy = (id1, 111L).
    }

    #[test]
    #[ignore = "feature gap: no sort_order_comparator; Float asc/desc"]
    fn test_sort_order_comparator_float_per_java() {
        // less = (id3, 1.11f); greater = (id1, 2.22f); lessCopy = (id1, 1.11f).
    }

    #[test]
    #[ignore = "feature gap: no sort_order_comparator; Double asc/desc"]
    fn test_sort_order_comparator_double_per_java() {
        // less = (id3, 1.11d); greater = (id2, 2.22d); lessCopy = (id1, 1.11d).
    }

    #[test]
    #[ignore = "feature gap: no sort_order_comparator; Date (i32 days) asc/desc"]
    fn test_sort_order_comparator_date_per_java() {
        // less = (id3, Date(111)); greater = (id2, Date(222)); lessCopy = (id1, Date(111)).
    }

    #[test]
    #[ignore = "feature gap: no sort_order_comparator; Time (i64 micros) asc/desc"]
    fn test_sort_order_comparator_time_per_java() {
        // less = (id3, Time(111L)); greater = (id2, Time(222L)); lessCopy = (id1, Time(111L)).
    }

    #[test]
    #[ignore = "feature gap: no sort_order_comparator; Timestamp (with/without zone, i64 micros) asc/desc"]
    fn test_sort_order_comparator_timestamp_per_java() {
        // less = (id3, ts(2022-01-10T00:00:00)); greater = (id2, ts(2022-01-10T01:00:00));
        // lessCopy = (id1, ts(2022-01-10T00:00:00)). Both withZone and withoutZone schemas.
    }

    #[test]
    #[ignore = "feature gap: no sort_order_comparator; sort by day(timestamp) transform — values at 00:00 and 01:00 same day compare equal"]
    fn test_sort_order_comparator_timestamp_day_transform_per_java() {
        // less = (id3, ts(2022-01-10T00:00:00)); greater = (id2, ts(2022-01-11T00:00:00));
        // lessCopy = (id1, ts(2022-01-10T01:00:00)) — same day as less under day() transform.
        // Sort: sortBy(day('field'), ASC, NULLS_FIRST).
    }

    #[test]
    #[ignore = "feature gap: no sort_order_comparator; String asc/desc"]
    fn test_sort_order_comparator_string_per_java() {
        // less = (id3, 'aaa'); greater = (id2, 'bbb'); lessCopy = (id1, 'aaa').
    }

    #[test]
    #[ignore = "feature gap: no sort_order_comparator; sort by bucket('field', 4) — bucket('bbb')<bucket('aaa'), bucket('bbb')==bucket('cca')"]
    fn test_sort_order_comparator_string_bucket_transform_per_java() {
        // less = (id3, 'bbb'); greater = (id2, 'aaa'); lessCopy = (id1, 'cca').
        // Sort: sortBy(bucket('field', 4), ASC, NULLS_FIRST).
    }

    #[test]
    #[ignore = "feature gap: no sort_order_comparator; UUID asc/desc (lexicographic on byte representation)"]
    fn test_sort_order_comparator_uuid_per_java() {
        // less = (id3, uuid '81873e7d-...'); greater = (id2, uuid 'fd02441d-...');
        // lessCopy = (id1, same as less).
    }

    #[test]
    #[ignore = "feature gap: no sort_order_comparator; sort by bucket(uuid, 4) — bucket('fd...')<bucket('86...'), bucket('fd...')==bucket('81...')"]
    fn test_sort_order_comparator_uuid_bucket_transform_per_java() {
        // less = (id3, uuid 'fd02441d-...'); greater = (id2, uuid '86873e7d-...');
        // lessCopy = (id1, uuid '81873e7d-...') (same bucket as less).
        // Sort: sortBy(bucket('field', 4), ASC, NULLS_FIRST).
    }

    #[test]
    #[ignore = "feature gap: no sort_order_comparator; Fixed-length byte array asc/desc"]
    fn test_sort_order_comparator_fixed_per_java() {
        // less = (id3, Fixed([1,2,3])); greater = (id2, Fixed([3,2,1]));
        // lessCopy = (id1, Fixed([1,2,3])).
    }

    #[test]
    #[ignore = "feature gap: no sort_order_comparator; Binary asc/desc — shorter prefix is less than longer prefix-extended"]
    fn test_sort_order_comparator_binary_per_java() {
        // less = (id3, Binary([1,1])); greater = (id2, Binary([1,1,1]));
        // lessCopy = (id1, Binary([1,1])).
    }

    #[test]
    #[ignore = "feature gap: no sort_order_comparator; sort by truncate(binary, 2) — first 2 bytes [1,2] < [1,3], and [1,2,3] equals [1,2,5,6] under truncate"]
    fn test_sort_order_comparator_binary_truncate_transform_per_java() {
        // less = (id3, Binary([1,2,3])); greater = (id2, Binary([1,3,1]));
        // lessCopy = (id1, Binary([1,2,5,6])).
        // Sort: sortBy(truncate('field', 2), ASC, NULLS_FIRST).
    }

    #[test]
    #[ignore = "feature gap: no sort_order_comparator; Decimal asc/desc"]
    fn test_sort_order_comparator_decimal_per_java() {
        // less = (id3, 0.1); greater = (id2, 0.2); lessCopy = (id1, 0.1).
        // Sort schema: Decimal(9, 5).
    }

    #[test]
    #[ignore = "feature gap: no sort_order_comparator; sort on nested struct field 'location.lat' — null lat sorts first under ASC, last under DESC"]
    fn test_sort_order_comparator_struct_per_java() {
        // Schema with location: {lat, long}. Tests 3 sort orders against
        // a fixture that varies which field is null:
        //   asc('location.lat')  — null lat fixture
        //   desc('location.long', NULLS_LAST) — null long fixture
        //   asc('location.lat').asc('location.long') — null lat / null
        //     long / both null fixtures, each compared against a different
        //     'greater' row that differs by the relevant field(s).
    }

    #[test]
    #[ignore = "feature gap: no sort_order_comparator; multi-field sort with truncate transform on each inner field of a nested struct"]
    fn test_sort_order_comparator_struct_with_transforms_per_java() {
        // Schema with struct: {left: binary, right: binary}.
        // Sort: sortBy(truncate('struct.left', 2), ASC, NULLS_FIRST)
        //        .sortBy(truncate('struct.right', 2), ASC, NULLS_FIRST).
        // less.right = [2,3,4]; greater.right = [9,3,4]; lessCopy.right = [2,3,9];
        // nullRight = right is null.
    }

    #[test]
    #[ignore = "feature gap: no sort_order_comparator; sort on 3-level nested user.location.lat/long with multiple null-field fixtures"]
    fn test_sort_order_comparator_nested_struct_per_java() {
        // Schema with user: {name, location: {lat, long}}.
        // Sort: asc('user.location.lat').asc('user.location.long').
        // Fixtures cover greater-lat, greater-long, greater-both,
        // and null-lat, null-long, null-both.
    }
}

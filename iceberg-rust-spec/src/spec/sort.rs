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

    // -----------------------------------------------------------------------
    // Placeholders for `sort_order_util` and `sort_order_comparator`.
    //
    // Eventual API surface:
    //   sort_order_util::build_sort_order(&Schema, &PartitionSpec, &SortOrder) -> SortOrder
    //       Prepends partition-spec fields as clustering columns, skipping any that the
    //       existing sort order already satisfies.
    //   sort_order_util::find_table_sort_order(&TableMetadata, &SortOrder) -> SortOrder
    //       Matches a user-supplied sort order against the table's historical sort_orders;
    //       returns SortOrder::default() (id=0, empty fields) when no match.
    //   sort_order_comparator::for_schema(&Schema, &SortOrder)
    //       -> impl Fn(&Struct, &Struct) -> std::cmp::Ordering
    //       Row comparator honouring asc/desc direction and NULLS_FIRST/NULLS_LAST.
    // -----------------------------------------------------------------------

    // --- TestSortOrderUtil (15) ---

    #[test]
    #[ignore = "no sort_order_util::build_sort_order"]
    fn test_build_sort_order_unpartitioned_v1_returns_order_unchanged() {
        unimplemented!("build_sort_order");
    }
    #[test]
    #[ignore = "no sort_order_util::build_sort_order"]
    fn test_build_sort_order_unpartitioned_v2_returns_order_unchanged() {
        unimplemented!("build_sort_order");
    }
    #[test]
    #[ignore = "no sort_order_util::build_sort_order"]
    fn test_build_sort_order_prepends_partition_fields_when_none_overlap() {
        unimplemented!("build_sort_order");
    }
    #[test]
    #[ignore = "no sort_order_util::build_sort_order"]
    fn test_build_sort_order_keeps_order_when_existing_sort_covers_spec_in_spec_order() {
        unimplemented!("build_sort_order");
    }
    #[test]
    #[ignore = "no sort_order_util::build_sort_order"]
    fn test_build_sort_order_keeps_order_when_existing_sort_covers_spec_in_different_order() {
        unimplemented!("build_sort_order");
    }
    #[test]
    #[ignore = "no sort_order_util::build_sort_order"]
    fn test_build_sort_order_prepends_only_missing_partition_fields() {
        unimplemented!("build_sort_order");
    }
    #[test]
    #[ignore = "no sort_order_util::build_sort_order"]
    fn test_build_sort_order_recognises_bare_source_at_tail_as_satisfying_transformed_partition() {
        unimplemented!("build_sort_order");
    }
    #[test]
    #[ignore = "no sort_order_util::build_sort_order"]
    fn test_build_sort_order_recognises_bare_source_at_head_as_satisfying_transformed_partition() {
        unimplemented!("build_sort_order");
    }
    #[test]
    #[ignore = "no sort_order_util::build_sort_order"]
    fn test_build_sort_order_recognises_bare_source_as_satisfying_multiple_transformed_partitions()
    {
        unimplemented!("build_sort_order");
    }
    #[test]
    #[ignore = "no sort_order_util::build_sort_order"]
    fn test_build_sort_order_with_redundant_partition_fields_keeps_order_unchanged() {
        unimplemented!("build_sort_order");
    }
    #[test]
    #[ignore = "no sort_order_util::build_sort_order"]
    fn test_build_sort_order_with_void_transform_skips_inactive_partition_fields() {
        unimplemented!("build_sort_order");
    }

    #[test]
    #[ignore = "no sort_order_util::find_table_sort_order"]
    fn test_find_table_sort_order_returns_current_when_user_supplied_matches() {
        unimplemented!("find_table_sort_order");
    }
    #[test]
    #[ignore = "no sort_order_util::find_table_sort_order"]
    fn test_find_table_sort_order_matches_by_shape_when_order_id_is_missing() {
        unimplemented!("find_table_sort_order");
    }
    #[test]
    #[ignore = "no sort_order_util::find_table_sort_order"]
    fn test_find_table_sort_order_matches_historical_order_not_just_current() {
        unimplemented!("find_table_sort_order");
    }
    #[test]
    #[ignore = "no sort_order_util::find_table_sort_order"]
    fn test_find_table_sort_order_returns_unsorted_default_when_no_match() {
        unimplemented!("find_table_sort_order");
    }

    // --- TestSortOrderComparators (20) ---

    #[test]
    #[ignore = "no sort_order_comparator"]
    fn test_sort_order_comparator_for_boolean_identity_sort() {
        unimplemented!("sort_order_comparator");
    }
    #[test]
    #[ignore = "no sort_order_comparator"]
    fn test_sort_order_comparator_for_int_identity_sort() {
        unimplemented!("sort_order_comparator");
    }
    #[test]
    #[ignore = "no sort_order_comparator"]
    fn test_sort_order_comparator_for_long_identity_sort() {
        unimplemented!("sort_order_comparator");
    }
    #[test]
    #[ignore = "no sort_order_comparator"]
    fn test_sort_order_comparator_for_float_identity_sort() {
        unimplemented!("sort_order_comparator");
    }
    #[test]
    #[ignore = "no sort_order_comparator"]
    fn test_sort_order_comparator_for_double_identity_sort() {
        unimplemented!("sort_order_comparator");
    }
    #[test]
    #[ignore = "no sort_order_comparator"]
    fn test_sort_order_comparator_for_date_identity_sort() {
        unimplemented!("sort_order_comparator");
    }
    #[test]
    #[ignore = "no sort_order_comparator"]
    fn test_sort_order_comparator_for_time_identity_sort() {
        unimplemented!("sort_order_comparator");
    }
    #[test]
    #[ignore = "no sort_order_comparator"]
    fn test_sort_order_comparator_for_timestamp_identity_sort() {
        unimplemented!("sort_order_comparator");
    }
    #[test]
    #[ignore = "no sort_order_comparator with day(timestamp) transform"]
    fn test_sort_order_comparator_for_day_of_timestamp_transform() {
        unimplemented!("sort_order_comparator transform");
    }
    #[test]
    #[ignore = "no sort_order_comparator"]
    fn test_sort_order_comparator_for_string_identity_sort() {
        unimplemented!("sort_order_comparator");
    }
    #[test]
    #[ignore = "no sort_order_comparator with bucket(string) transform"]
    fn test_sort_order_comparator_for_bucket_of_string_transform() {
        unimplemented!("sort_order_comparator transform");
    }
    #[test]
    #[ignore = "no sort_order_comparator"]
    fn test_sort_order_comparator_for_uuid_identity_sort() {
        unimplemented!("sort_order_comparator");
    }
    #[test]
    #[ignore = "no sort_order_comparator with bucket(uuid) transform"]
    fn test_sort_order_comparator_for_bucket_of_uuid_transform() {
        unimplemented!("sort_order_comparator transform");
    }
    #[test]
    #[ignore = "no sort_order_comparator"]
    fn test_sort_order_comparator_for_fixed_identity_sort() {
        unimplemented!("sort_order_comparator");
    }
    #[test]
    #[ignore = "no sort_order_comparator"]
    fn test_sort_order_comparator_for_binary_identity_sort() {
        unimplemented!("sort_order_comparator");
    }
    #[test]
    #[ignore = "no sort_order_comparator with truncate(binary, 2) transform"]
    fn test_sort_order_comparator_for_truncate_of_binary_transform() {
        unimplemented!("sort_order_comparator transform");
    }
    #[test]
    #[ignore = "no sort_order_comparator"]
    fn test_sort_order_comparator_for_decimal_identity_sort() {
        unimplemented!("sort_order_comparator");
    }
    #[test]
    #[ignore = "no sort_order_comparator over struct row"]
    fn test_sort_order_comparator_for_nested_struct_single_field_sort() {
        unimplemented!("sort_order_comparator struct");
    }
    #[test]
    #[ignore = "no sort_order_comparator over struct row with multi-field transform"]
    fn test_sort_order_comparator_for_nested_struct_with_truncate_transform_on_both_fields() {
        unimplemented!("sort_order_comparator struct transform");
    }
    #[test]
    #[ignore = "no sort_order_comparator over deeply nested struct row"]
    fn test_sort_order_comparator_for_three_level_nested_struct_with_two_fields() {
        unimplemented!("sort_order_comparator deeply nested");
    }
}

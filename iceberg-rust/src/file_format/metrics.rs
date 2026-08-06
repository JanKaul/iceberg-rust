/*!
 * Metrics modes for data-file statistics.
 *
 * Iceberg lets a table decide how much per-column statistics a data file
 * carries, through the `write.metadata.metrics.*` table properties. Statistics
 * are stored inline in the manifest entry for every data file, so collecting
 * full bounds for every column of a wide table — or for a column holding large
 * strings, such as a JSON blob or a log body — makes manifests grow without
 * making planning any better: bounds on such a column prune nothing, but they
 * are carried on every entry forever.
 *
 * The supported modes, matching the spec:
 *
 * - `none` — no counts and no bounds
 * - `counts` — value/null/NaN counts, no bounds
 * - `truncate(n)` — counts plus bounds shortened to `n` units
 * - `full` — counts plus untruncated bounds
 *
 * Resolution order for a column is `write.metadata.metrics.column.<name>`, then
 * `write.metadata.metrics.default`, then `truncate(16)`. The inferred default
 * only reaches the first `write.metadata.metrics.max-inferred-column-defaults`
 * (100) top-level columns; past that a column must be named explicitly to be
 * measured at all.
 */

use std::collections::HashMap;

use iceberg_rust_spec::spec::values::Value;
use iceberg_rust_spec::table_metadata::{
    WRITE_METADATA_METRICS_COLUMN_PREFIX, WRITE_METADATA_METRICS_DEFAULT,
    WRITE_METADATA_METRICS_MAX_INFERRED_COLUMN_DEFAULTS,
};

/// Bound length used when a table says nothing about metrics.
pub const DEFAULT_TRUNCATE_LENGTH: usize = 16;

/// Number of top-level columns the inferred default reaches.
pub const DEFAULT_MAX_INFERRED_COLUMN_DEFAULTS: usize = 100;

/// How much statistics to collect for one column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricsMode {
    /// Collect nothing.
    None,
    /// Collect counts but no bounds.
    Counts,
    /// Collect counts and bounds shortened to the given number of units.
    Truncate(usize),
    /// Collect counts and untruncated bounds.
    Full,
}

impl MetricsMode {
    /// Parse a mode from a table-property value, case-insensitively.
    ///
    /// Returns `None` for anything unrecognized, so an unparsable property
    /// falls back to the default rather than failing the write.
    pub fn parse(value: &str) -> Option<MetricsMode> {
        let value = value.trim().to_ascii_lowercase();
        match value.as_str() {
            "none" => Some(MetricsMode::None),
            "counts" => Some(MetricsMode::Counts),
            "full" => Some(MetricsMode::Full),
            other => {
                let length = other.strip_prefix("truncate(")?.strip_suffix(')')?;
                let length: usize = length.trim().parse().ok()?;
                // truncate(0) would leave a bound that carries no information.
                (length > 0).then_some(MetricsMode::Truncate(length))
            }
        }
    }

    /// Whether value, null and NaN counts are collected under this mode.
    pub fn records_counts(self) -> bool {
        !matches!(self, MetricsMode::None)
    }

    /// Whether lower and upper bounds are collected under this mode.
    pub fn records_bounds(self) -> bool {
        matches!(self, MetricsMode::Truncate(_) | MetricsMode::Full)
    }
}

/// The `write.metadata.metrics.*` configuration of a table.
#[derive(Debug, Clone)]
pub struct MetricsConfig {
    default_mode: MetricsMode,
    column_modes: HashMap<String, MetricsMode>,
    max_inferred_column_defaults: usize,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        MetricsConfig {
            default_mode: MetricsMode::Truncate(DEFAULT_TRUNCATE_LENGTH),
            column_modes: HashMap::new(),
            max_inferred_column_defaults: DEFAULT_MAX_INFERRED_COLUMN_DEFAULTS,
        }
    }
}

impl MetricsConfig {
    /// Read the configuration from a table's properties.
    pub fn from_table_properties(properties: &HashMap<String, String>) -> Self {
        let default_mode = properties
            .get(WRITE_METADATA_METRICS_DEFAULT)
            .and_then(|value| MetricsMode::parse(value))
            .unwrap_or(MetricsMode::Truncate(DEFAULT_TRUNCATE_LENGTH));

        let column_modes = properties
            .iter()
            .filter_map(|(key, value)| {
                let column = key.strip_prefix(WRITE_METADATA_METRICS_COLUMN_PREFIX)?;
                Some((column.to_owned(), MetricsMode::parse(value)?))
            })
            .collect();

        let max_inferred_column_defaults = properties
            .get(WRITE_METADATA_METRICS_MAX_INFERRED_COLUMN_DEFAULTS)
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(DEFAULT_MAX_INFERRED_COLUMN_DEFAULTS);

        MetricsConfig {
            default_mode,
            column_modes,
            max_inferred_column_defaults,
        }
    }

    /// The mode for a column, addressed by its full dotted name.
    ///
    /// `top_level_index` is the position of the column's root field in the
    /// schema; a column past `max-inferred-column-defaults` only gets metrics
    /// if it was named explicitly. Pass `None` when the position is unknown, in
    /// which case the limit does not apply.
    pub fn mode_for(&self, column_name: &str, top_level_index: Option<usize>) -> MetricsMode {
        if let Some(mode) = self.column_modes.get(column_name) {
            return *mode;
        }
        match top_level_index {
            Some(index) if index >= self.max_inferred_column_defaults => MetricsMode::None,
            _ => self.default_mode,
        }
    }
}

/// Shorten a lower bound to `length` units.
///
/// Truncating a lower bound can only make it smaller, so the result still
/// bounds every value the original bounded. Types with no meaningful notion of
/// truncation — numbers, fixed-width binary, UUIDs — pass through untouched.
pub fn truncate_lower_bound(value: Value, length: usize) -> Value {
    match value {
        Value::String(string) => {
            let truncated: String = string.chars().take(length).collect();
            Value::String(truncated)
        }
        Value::Binary(bytes) => {
            let mut truncated = bytes;
            truncated.truncate(length);
            Value::Binary(truncated)
        }
        other => other,
    }
}

/// Shorten an upper bound to `length` units.
///
/// An upper bound must stay at or above every value it covers, so the truncated
/// prefix is incremented. Returns `None` when the prefix cannot be incremented
/// — every unit is already at its maximum — in which case the bound has to be
/// dropped rather than understated.
pub fn truncate_upper_bound(value: Value, length: usize) -> Option<Value> {
    match value {
        Value::String(string) => {
            if string.chars().count() <= length {
                return Some(Value::String(string));
            }
            let mut chars: Vec<char> = string.chars().take(length).collect();
            while let Some(last) = chars.pop() {
                if let Some(next) = next_char(last) {
                    chars.push(next);
                    return Some(Value::String(chars.into_iter().collect()));
                }
            }
            None
        }
        Value::Binary(bytes) => {
            if bytes.len() <= length {
                return Some(Value::Binary(bytes));
            }
            let mut truncated = bytes;
            truncated.truncate(length);
            while let Some(last) = truncated.pop() {
                if last != u8::MAX {
                    truncated.push(last + 1);
                    return Some(Value::Binary(truncated));
                }
            }
            None
        }
        other => Some(other),
    }
}

/// The next Unicode scalar value after `c`, skipping the surrogate range.
fn next_char(c: char) -> Option<char> {
    let mut code = c as u32 + 1;
    // 0xD800..=0xDFFF are surrogates and never valid scalar values.
    if (0xD800..=0xDFFF).contains(&code) {
        code = 0xE000;
    }
    char::from_u32(code)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_accepts_every_spec_mode_case_insensitively() {
        assert_eq!(MetricsMode::parse("none"), Some(MetricsMode::None));
        assert_eq!(MetricsMode::parse("NONE"), Some(MetricsMode::None));
        assert_eq!(MetricsMode::parse("counts"), Some(MetricsMode::Counts));
        assert_eq!(MetricsMode::parse("full"), Some(MetricsMode::Full));
        assert_eq!(
            MetricsMode::parse("truncate(24)"),
            Some(MetricsMode::Truncate(24))
        );
        assert_eq!(
            MetricsMode::parse(" Truncate( 8 ) "),
            Some(MetricsMode::Truncate(8))
        );
    }

    #[test]
    fn parse_rejects_unusable_values() {
        assert_eq!(MetricsMode::parse("truncate(0)"), None);
        assert_eq!(MetricsMode::parse("truncate(-1)"), None);
        assert_eq!(MetricsMode::parse("truncate()"), None);
        assert_eq!(MetricsMode::parse("truncate"), None);
        assert_eq!(MetricsMode::parse("everything"), None);
        assert_eq!(MetricsMode::parse(""), None);
    }

    #[test]
    fn modes_gate_counts_and_bounds() {
        assert!(!MetricsMode::None.records_counts());
        assert!(!MetricsMode::None.records_bounds());
        assert!(MetricsMode::Counts.records_counts());
        assert!(!MetricsMode::Counts.records_bounds());
        assert!(MetricsMode::Truncate(4).records_counts());
        assert!(MetricsMode::Truncate(4).records_bounds());
        assert!(MetricsMode::Full.records_counts());
        assert!(MetricsMode::Full.records_bounds());
    }

    #[test]
    fn a_table_without_properties_truncates_to_sixteen() {
        let config = MetricsConfig::from_table_properties(&HashMap::new());
        assert_eq!(
            config.mode_for("anything", Some(0)),
            MetricsMode::Truncate(DEFAULT_TRUNCATE_LENGTH)
        );
    }

    #[test]
    fn a_per_column_mode_overrides_the_default() {
        let properties = HashMap::from([
            (
                WRITE_METADATA_METRICS_DEFAULT.to_string(),
                "counts".to_string(),
            ),
            (
                format!("{WRITE_METADATA_METRICS_COLUMN_PREFIX}trace_id"),
                "full".to_string(),
            ),
            (
                format!("{WRITE_METADATA_METRICS_COLUMN_PREFIX}body"),
                "none".to_string(),
            ),
        ]);
        let config = MetricsConfig::from_table_properties(&properties);

        assert_eq!(config.mode_for("trace_id", Some(0)), MetricsMode::Full);
        assert_eq!(config.mode_for("body", Some(1)), MetricsMode::None);
        assert_eq!(config.mode_for("other", Some(2)), MetricsMode::Counts);
    }

    #[test]
    fn columns_past_the_inferred_limit_are_not_measured() {
        let properties = HashMap::from([(
            WRITE_METADATA_METRICS_MAX_INFERRED_COLUMN_DEFAULTS.to_string(),
            "2".to_string(),
        )]);
        let config = MetricsConfig::from_table_properties(&properties);

        assert!(config.mode_for("a", Some(0)).records_bounds());
        assert!(config.mode_for("b", Some(1)).records_bounds());
        assert_eq!(config.mode_for("c", Some(2)), MetricsMode::None);
    }

    #[test]
    fn an_explicit_column_is_measured_past_the_inferred_limit() {
        let properties = HashMap::from([
            (
                WRITE_METADATA_METRICS_MAX_INFERRED_COLUMN_DEFAULTS.to_string(),
                "1".to_string(),
            ),
            (
                format!("{WRITE_METADATA_METRICS_COLUMN_PREFIX}late"),
                "full".to_string(),
            ),
        ]);
        let config = MetricsConfig::from_table_properties(&properties);

        assert_eq!(config.mode_for("late", Some(99)), MetricsMode::Full);
        assert_eq!(config.mode_for("other", Some(99)), MetricsMode::None);
    }

    #[test]
    fn an_unparsable_property_falls_back_instead_of_failing() {
        let properties = HashMap::from([(
            WRITE_METADATA_METRICS_DEFAULT.to_string(),
            "sometimes".to_string(),
        )]);
        let config = MetricsConfig::from_table_properties(&properties);
        assert_eq!(
            config.mode_for("a", Some(0)),
            MetricsMode::Truncate(DEFAULT_TRUNCATE_LENGTH)
        );
    }

    #[test]
    fn a_truncated_lower_bound_stays_below_the_original() {
        let original = "abcdefghij".to_string();
        let truncated = truncate_lower_bound(Value::String(original.clone()), 4);
        let Value::String(truncated) = truncated else {
            panic!("expected a string");
        };
        assert_eq!(truncated, "abcd");
        assert!(truncated <= original);
    }

    #[test]
    fn a_truncated_upper_bound_stays_above_the_original() {
        let original = "abcdefghij".to_string();
        let truncated = truncate_upper_bound(Value::String(original.clone()), 4).unwrap();
        let Value::String(truncated) = truncated else {
            panic!("expected a string");
        };
        assert_eq!(truncated, "abce");
        assert!(truncated >= original);
    }

    #[test]
    fn bounds_shorter_than_the_limit_are_left_alone() {
        assert_eq!(
            truncate_lower_bound(Value::String("ab".to_string()), 4),
            Value::String("ab".to_string())
        );
        assert_eq!(
            truncate_upper_bound(Value::String("ab".to_string()), 4),
            Some(Value::String("ab".to_string()))
        );
    }

    #[test]
    fn truncation_counts_characters_not_bytes() {
        // Four characters, ten bytes: truncating to 4 must keep all of them
        // and must never split a character.
        let original = "日本語だ".to_string();
        assert_eq!(original.len(), 12);
        let truncated = truncate_lower_bound(Value::String(original.clone()), 4);
        assert_eq!(truncated, Value::String(original));

        let Value::String(truncated) =
            truncate_lower_bound(Value::String("日本語だより".to_string()), 3)
        else {
            panic!("expected a string");
        };
        assert_eq!(truncated, "日本語");
    }

    #[test]
    fn an_upper_bound_carries_past_maximal_characters() {
        // The last character cannot be incremented, so the carry moves left.
        let original = format!("a{}{}z", char::MAX, char::MAX);
        let Value::String(truncated) = truncate_upper_bound(Value::String(original.clone()), 3)
            .expect("a bound is still representable")
        else {
            panic!("expected a string");
        };
        assert_eq!(truncated, "b");
        assert!(truncated >= original);
    }

    #[test]
    fn an_upper_bound_that_cannot_be_incremented_is_dropped() {
        let original: String = std::iter::repeat_n(char::MAX, 4).collect();
        assert_eq!(truncate_upper_bound(Value::String(original), 2), None);
    }

    #[test]
    fn an_incremented_character_never_lands_on_a_surrogate() {
        // U+D7FF is the last scalar before the surrogate range.
        let original = format!("{}x", '\u{D7FF}');
        let Value::String(truncated) =
            truncate_upper_bound(Value::String(original.clone()), 1).unwrap()
        else {
            panic!("expected a string");
        };
        assert_eq!(truncated, "\u{E000}");
        assert!(truncated >= original);
    }

    #[test]
    fn binary_bounds_truncate_by_byte() {
        assert_eq!(
            truncate_lower_bound(Value::Binary(vec![1, 2, 3, 4]), 2),
            Value::Binary(vec![1, 2])
        );
        assert_eq!(
            truncate_upper_bound(Value::Binary(vec![1, 2, 3, 4]), 2),
            Some(Value::Binary(vec![1, 3]))
        );
        assert_eq!(
            truncate_upper_bound(Value::Binary(vec![1, 255, 255, 4]), 3),
            Some(Value::Binary(vec![2]))
        );
        assert_eq!(
            truncate_upper_bound(Value::Binary(vec![255, 255, 255]), 2),
            None
        );
    }

    #[test]
    fn non_truncatable_types_pass_through() {
        assert_eq!(
            truncate_lower_bound(Value::LongInt(42), 2),
            Value::LongInt(42)
        );
        assert_eq!(
            truncate_upper_bound(Value::LongInt(42), 2),
            Some(Value::LongInt(42))
        );
    }
}

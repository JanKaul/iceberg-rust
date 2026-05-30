/*!
This module provides utility functions.
*/
use url::Url;

/// Strips URL scheme and authority components from a path string
///
/// # Arguments
/// * `path` - A string that may be a URL or plain path
///
/// # Returns
/// The path component of the URL, or the original string if it's not a valid URL
///
/// # Examples
/// ```
/// use iceberg_rust_spec::util::strip_prefix;
/// assert_eq!(strip_prefix("s3://bucket/path"), "/path");
/// assert_eq!(strip_prefix("/plain/path"), "/plain/path");
/// ```
pub fn strip_prefix(path: &str) -> String {
    match Url::parse(path) {
        Ok(url) => String::from(url.path()),
        Err(_) => String::from(path),
    }
}

#[cfg(test)]
mod tests {
    use crate::util::strip_prefix;
    #[test]
    fn strip_prefix_behaves_as_expected() {
        assert_eq!(strip_prefix("/a/b"), "/a/b");
        assert_eq!(strip_prefix("memory:///a/b"), "/a/b");
        assert_eq!(strip_prefix("file:///a/b"), "/a/b");
        assert_eq!(strip_prefix("s3://bucket/a/b"), "/a/b");
        assert_eq!(strip_prefix("gs://bucket/a/b"), "/a/b");
        assert_eq!(strip_prefix("az://bucket/a/b"), "/a/b");
        assert_eq!(
            strip_prefix("abfss://container@account.dfs.core.windows.net/a/b"),
            "/a/b"
        );
        assert_eq!(
            strip_prefix("abfs://container@account.dfs.core.windows.net/a/b"),
            "/a/b"
        );
    }

    // --- TestJsonUtil port -------------------------------------------------
    //
    // Java's `org.apache.iceberg.util.JsonUtil` is a Jackson-based facade
    // exposing typed field accessors over a `JsonNode` tree:
    //
    //   - `get(name, node)` / `getInt/getLong/getString/getBool(name, node)`
    //     throw IllegalArgumentException with a specific message when the
    //     field is missing, null, or has the wrong type.
    //   - `getXxxOrNull(name, node)` return null for missing/null fields
    //     but still throw for wrong-type values.
    //   - Container variants: `getIntArrayOrNull / getIntegerList /
    //     getIntegerSet / getLongList / getStringList / getStringSet /
    //     getStringMap / getObjectList` — each pins error messages for
    //     missing field, non-array input, or wrong element type.
    //   - `getByteBufferOrNull(name, node)` parses BASE16 hex into bytes.
    //   - `getDurationStringOrNull(name, node)` validates ISO-8601
    //     duration syntax (`PT30M`).
    //
    // Rust has NO `JsonUtil` facade. Code uses `serde_json::Value` and
    // `value.get("x").and_then(|v| v.as_i64())` style access directly,
    // with `serde_json::Error` surfaced via the workspace `Error` enum.
    // The Java helpers' error messages have no Rust counterpart.
    //
    // All 27 Java @Test scenarios are pinned `#[ignore]` here: they
    // exercise a facade that Rust deliberately doesn't replicate (the
    // catalogue calls this out — "Rust uses serde directly"). Future
    // work would either be:
    //   - Add a `json_util` module with matching error messages so the
    //     Java tests can be ported 1:1.
    //   - Keep using serde directly and accept the divergent error text.

    #[test]
    #[ignore = "feature gap: no util::json_util::get(name, &Value); should throw 'Cannot parse missing field: <name>' on missing or null field"]
    fn test_json_util_get_per_java() {
        // Java: get. Missing field + null field both error with
        // "Cannot parse missing field: x"; "{\"x\": \"23\"}" -> "23".
    }

    #[test]
    #[ignore = "feature gap: no get_int; messages 'Cannot parse missing int: x' for missing, 'Cannot parse to an integer value: x: <node>' for wrong type"]
    fn test_json_util_get_int_per_java() {
        // Java: getInt. Missing -> "missing int"; null/string/decimal ->
        // "to an integer value". Integer JSON value -> i32.
    }

    #[test]
    #[ignore = "feature gap: no get_int_or_null; null/missing return None; wrong type still errors"]
    fn test_json_util_get_int_or_null_per_java() {
        // Java: getIntOrNull. Missing/null -> null; int -> i32; string/
        // decimal -> error.
    }

    #[test]
    #[ignore = "feature gap: no get_long; messages 'Cannot parse missing long: x' / 'Cannot parse to a long value: x: <node>'"]
    fn test_json_util_get_long_per_java() {}

    #[test]
    #[ignore = "feature gap: no get_long_or_null"]
    fn test_json_util_get_long_or_null_per_java() {}

    #[test]
    #[ignore = "feature gap: no get_string; messages 'Cannot parse missing string: x' / 'Cannot parse to a string value: x: <node>'"]
    fn test_json_util_get_string_per_java() {}

    #[test]
    #[ignore = "feature gap: no get_string_or_null"]
    fn test_json_util_get_string_or_null_per_java() {}

    #[test]
    #[ignore = "feature gap: no get_duration_string_or_null; validates ISO-8601 ('PT30M' OK, '30M' / '' rejected with 'Cannot parse to a duration string value: x: <node>')"]
    fn test_json_util_get_duration_string_or_null_per_java() {}

    #[test]
    #[ignore = "feature gap: no get_byte_buffer_or_null; decodes BASE16 hex into bytes; non-text input errors 'Cannot parse byte buffer from non-text value: x: <node>'"]
    fn test_json_util_get_byte_buffer_or_null_per_java() {}

    #[test]
    #[ignore = "feature gap: no get_bool; messages 'Cannot parse missing boolean: x' / 'Cannot parse to a boolean value: x: <node>'; \"true\" string rejected (must be raw bool)"]
    fn test_json_util_get_bool_per_java() {}

    #[test]
    #[ignore = "feature gap: no get_bool_or_null"]
    fn test_json_util_get_bool_or_null_per_java() {}

    #[test]
    #[ignore = "feature gap: no get_int_array_or_null; missing/null return None; non-int element errors 'Cannot parse integer from non-int value in items: <node>'"]
    fn test_json_util_get_int_array_or_null_per_java() {}

    #[test]
    #[ignore = "feature gap: no get_integer_list; 'Cannot parse missing list: items' on missing, 'Cannot parse JSON array from non-array value' on null, per-element type check; round-trip via writeIntegerArray"]
    fn test_json_util_get_integer_list_per_java() {}

    #[test]
    #[ignore = "feature gap: no get_integer_set; same shape as list but de-dups into a Set"]
    fn test_json_util_get_integer_set_per_java() {}

    #[test]
    #[ignore = "feature gap: no get_integer_set_or_null"]
    fn test_json_util_get_integer_set_or_null_per_java() {}

    #[test]
    #[ignore = "feature gap: no get_long_list"]
    fn test_json_util_get_long_list_per_java() {}

    #[test]
    #[ignore = "feature gap: no get_long_list_or_null"]
    fn test_json_util_get_long_list_or_null_per_java() {}

    #[test]
    #[ignore = "feature gap: no get_long_set"]
    fn test_json_util_get_long_set_per_java() {}

    #[test]
    #[ignore = "feature gap: no get_long_set_or_null"]
    fn test_json_util_get_long_set_or_null_per_java() {}

    #[test]
    #[ignore = "feature gap: no get_string_list"]
    fn test_json_util_get_string_list_per_java() {}

    #[test]
    #[ignore = "feature gap: no get_string_list_or_null"]
    fn test_json_util_get_string_list_or_null_per_java() {}

    #[test]
    #[ignore = "feature gap: no get_string_set"]
    fn test_json_util_get_string_set_per_java() {}

    #[test]
    #[ignore = "feature gap: no get_string_map; missing field -> 'Cannot parse missing map: <name>'; non-string value -> 'Cannot parse to a string value'"]
    fn test_json_util_get_string_map_per_java() {}

    #[test]
    #[ignore = "feature gap: no get_string_map_nullable_values; same as get_string_map but allows null values"]
    fn test_json_util_get_string_map_nullable_values_per_java() {}

    #[test]
    #[ignore = "feature gap: no get_string_map_or_null"]
    fn test_json_util_get_string_map_or_null_per_java() {}

    #[test]
    #[ignore = "feature gap: no get_object_list; deserializes a JSON array of objects via a per-element callback"]
    fn test_json_util_get_object_list_per_java() {}

    #[test]
    #[ignore = "feature gap: no get_object_list_or_null"]
    fn test_json_util_get_object_list_or_null_per_java() {}
}

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

    // -----------------------------------------------------------------------
    // Placeholders for a future `json_util` Jackson-style facade.
    //
    // Rust uses serde_json directly, so there is no `JsonUtil` analog. The 27
    // tests below pin the typed-accessor + error-message contract an eventual
    // facade would have to satisfy. Each case maps to one upstream @Test method.
    // -----------------------------------------------------------------------

    use rstest::rstest;

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
    #[ignore = "no json_util facade: typed accessors get/getInt/getLong/getString/getBool/...OrNull + getXxxList/Set/SetOrNull + getDurationStringOrNull + getByteBufferOrNull + getIntArrayOrNull + getStringMap/MapNullableValues/MapOrNull + getObjectList/ListOrNull with specific error messages on type mismatch"]
    fn test_json_util_typed_accessor_scenarios(#[case] _scenario: usize) {
        unimplemented!("json_util Jackson-style facade");
    }
}

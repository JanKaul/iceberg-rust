/*!
This module provides utility functions.
*/
use url::Url;

/// Strip prefix from a path
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
        assert_eq!(strip_prefix("file:///a/b"), "/a/b");
        assert_eq!(strip_prefix("s3://bucket/a/b"), "/a/b");
        assert_eq!(strip_prefix("gs://bucket/a/b"), "/a/b");
        assert_eq!(strip_prefix("az://bucket/a/b"), "/a/b");
    }
}

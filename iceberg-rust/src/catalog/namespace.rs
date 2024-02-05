/*!
Defining the [Namespace] struct for handling namespaces in the catalog.
*/

use core::fmt::{self, Display};
use itertools::Itertools;
use serde_derive::{Deserialize, Serialize};

use crate::{catalog::identifier::SEPARATOR, error::Error};

/// Namespace struct for iceberg catalogs
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Namespace {
    levels: Vec<String>,
}

impl Namespace {
    /// Try to create new namespace with sequence of strings.
    pub fn try_new(levels: &[String]) -> Result<Self, Error> {
        if levels.iter().any(|x| x.is_empty()) {
            Err(Error::InvalidFormat("namespace sequence".to_string()))
        } else {
            Ok(Namespace {
                levels: levels.to_vec(),
            })
        }
    }
    /// Create empty namespace
    pub fn empty() -> Self {
        Namespace { levels: vec![] }
    }
    /// Get the namespace levels
    pub fn levels(&self) -> &[String] {
        &self.levels
    }
    /// Get the number of levels
    pub fn len(&self) -> usize {
        self.levels.len()
    }
    /// Check if namespace is empty
    pub fn is_empty(&self) -> bool {
        self.levels.is_empty()
    }
}

impl Display for Namespace {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            Itertools::intersperse(self.levels.iter().map(|x| x as &str), SEPARATOR)
                .collect::<String>()
        )
    }
}

#[cfg(test)]

mod tests {
    use super::Namespace;

    #[test]
    fn test_new() {
        let namespace = Namespace::try_new(&[
            "level1".to_string(),
            "level2".to_string(),
            "level3".to_string(),
        ])
        .unwrap();
        assert_eq!(&format!("{}", namespace), "level1.level2.level3");
    }
    #[test]
    #[should_panic]
    fn test_empty() {
        let _ = Namespace::try_new(&["".to_string(), "level2".to_string()]).unwrap();
    }
}

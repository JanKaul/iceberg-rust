/*!
Defining the [Identifier] struct for identifying tables in an iceberg catalog.
*/

use core::fmt::{self, Display};

use iceberg_rust_spec::view_metadata::FullIdentifier;
use serde_derive::{Deserialize, Serialize};

use crate::error::Error;

use super::namespace::Namespace;

/// Seperator of different namespace levels.
pub static SEPARATOR: &str = ".";

///Identifies a table in an iceberg catalog.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Identifier {
    namespace: Namespace,
    name: String,
}

impl Identifier {
    /// Create new Identifier
    pub fn new(namespace: &[String], name: &str) -> Self {
        Self {
            namespace: Namespace(namespace.to_vec()),
            name: name.to_owned(),
        }
    }

    ///Create Identifier
    pub fn try_new(names: &[String]) -> Result<Self, Error> {
        let length = names.len();
        if names.is_empty() {
            Err(Error::InvalidFormat("identifier sequence".to_string()))
        } else if names[length - 1].is_empty() {
            Err(Error::InvalidFormat("table name".to_string()))
        } else {
            Ok(Identifier {
                namespace: Namespace::try_new(&names[0..length - 1])?,
                name: names[length - 1].clone(),
            })
        }
    }
    ///Parse
    pub fn parse(identifier: &str) -> Result<Self, Error> {
        let names = identifier
            .split(SEPARATOR)
            .map(|x| x.to_string())
            .collect::<Vec<String>>();
        Identifier::try_new(&names)
    }
    /// Return namespace of table
    pub fn namespace(&self) -> &Namespace {
        &self.namespace
    }
    /// Return name of table
    pub fn name(&self) -> &str {
        &self.name
    }
    /// Create Full Identifier
    pub fn into_full_identifier(&self, catalog: &str) -> FullIdentifier {
        FullIdentifier::new(catalog, &self.namespace, &self.name, None)
    }
}

impl Display for Identifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}{}", self.namespace, SEPARATOR, self.name)
    }
}

impl TryFrom<&str> for Identifier {
    type Error = Error;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

impl From<&FullIdentifier> for Identifier {
    fn from(value: &FullIdentifier) -> Self {
        Identifier {
            namespace: Namespace(value.namespace().clone()),
            name: value.name().clone(),
        }
    }
}

#[cfg(test)]

mod tests {
    use super::Identifier;

    #[test]
    fn test_new() {
        let identifier = Identifier::try_new(&[
            "level1".to_string(),
            "level2".to_string(),
            "table".to_string(),
        ])
        .unwrap();
        assert_eq!(&format!("{}", identifier), "level1.level2.table");
    }
    #[test]
    #[should_panic]
    fn test_empty() {
        let _ = Identifier::try_new(&["level1".to_string(), "level2".to_string(), "".to_string()])
            .unwrap();
    }
    #[test]
    #[should_panic]
    fn test_empty_identifier() {
        let _ = Identifier::try_new(&[]).unwrap();
    }
    #[test]
    fn test_parse() {
        let identifier = Identifier::parse("level1.level2.table").unwrap();
        assert_eq!(&format!("{}", identifier), "level1.level2.table");
    }
}

/*!
Defining the [Identifier] struct for identifying tables in an iceberg catalog.
*/

use core::fmt::{self, Display};
use derive_getters::Getters;

use serde_derive::{Deserialize, Serialize};

use crate::error::Error;

use super::namespace::Namespace;

/// Seperator of different namespace levels.
pub static SEPARATOR: &str = ".";

///Identifies a table in an iceberg catalog.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
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
    pub fn try_new(names: &[String], default_namespace: Option<&[String]>) -> Result<Self, Error> {
        let mut parts = names.iter().rev();
        let table_name = parts.next().ok_or(Error::InvalidFormat(format!(
            "Identifier {names:?} is empty"
        )))?;
        if table_name.is_empty() {
            return Err(Error::InvalidFormat(format!(
                "Table name {table_name:?} is empty"
            )));
        }
        let namespace: Vec<String> = parts.rev().map(ToOwned::to_owned).collect();
        let namespace = if namespace.is_empty() {
            default_namespace
                .ok_or(Error::NotFound("Default namespace".to_owned()))?
                .iter()
                .map(ToOwned::to_owned)
                .collect()
        } else {
            namespace
        };
        Ok(Identifier {
            namespace: Namespace(namespace),
            name: table_name.to_owned(),
        })
    }

    ///Parse
    pub fn parse(identifier: &str, default_namespace: Option<&[String]>) -> Result<Self, Error> {
        let names = identifier
            .split(SEPARATOR)
            .map(ToOwned::to_owned)
            .collect::<Vec<String>>();
        Identifier::try_new(&names, default_namespace)
    }
    /// Return namespace of table
    pub fn namespace(&self) -> &Namespace {
        &self.namespace
    }
    /// Return name of table
    pub fn name(&self) -> &str {
        &self.name
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
        Self::parse(value, None)
    }
}

///Identifies a table in an iceberg catalog.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize, Getters)]
pub struct FullIdentifier {
    #[serde(skip_serializing_if = "Option::is_none")]
    catalog: Option<String>,
    namespace: Namespace,
    name: String,
}

impl FullIdentifier {
    pub fn new(catalog: Option<&str>, namespace: &[String], name: &str) -> Self {
        Self {
            catalog: catalog.map(ToString::to_string),
            namespace: Namespace(namespace.to_owned()),
            name: name.to_owned(),
        }
    }
}

impl From<&FullIdentifier> for Identifier {
    fn from(value: &FullIdentifier) -> Self {
        Identifier {
            namespace: value.namespace.clone(),
            name: value.name.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{FullIdentifier, Identifier};
    use crate::error::Error;

    #[test]
    fn test_new() {
        let identifier = Identifier::try_new(
            &[
                "level1".to_string(),
                "level2".to_string(),
                "table".to_string(),
            ],
            None,
        )
        .unwrap();
        assert_eq!(&format!("{identifier}"), "level1.level2.table");
    }
    #[test]
    #[should_panic]
    fn test_empty() {
        let _ = Identifier::try_new(
            &["level1".to_string(), "level2".to_string(), "".to_string()],
            None,
        )
        .unwrap();
    }
    #[test]
    #[should_panic]
    fn test_empty_identifier() {
        let _ = Identifier::try_new(&[], None).unwrap();
    }
    #[test]
    fn test_parse() {
        let identifier = Identifier::parse("level1.level2.table", None).unwrap();
        assert_eq!(&format!("{identifier}"), "level1.level2.table");
    }

    #[test]
    fn test_identifier_try_new_uses_default_namespace_for_bare_name() {
        let id = Identifier::try_new(
            &["events".to_string()],
            Some(&["analytics".to_string(), "raw".to_string()]),
        )
        .unwrap();
        assert_eq!(id.name(), "events");
        assert_eq!(format!("{id}"), "analytics.raw.events");
    }

    #[test]
    fn test_identifier_try_new_requires_default_namespace_when_name_has_no_qualifier() {
        let err = Identifier::try_new(&["events".to_string()], None).unwrap_err();
        assert!(matches!(err, Error::NotFound(_)));
    }

    #[test]
    fn test_identifier_try_from_str_parses_dotted_form() {
        let id: Identifier = "analytics.raw.events".try_into().unwrap();
        assert_eq!(id.name(), "events");
        assert_eq!(id.namespace().len(), 2);
    }

    #[test]
    fn test_full_identifier_omits_none_catalog_from_serialization() {
        let id = FullIdentifier::new(None, &["ns".to_string()], "t");
        let json = serde_json::to_string(&id).unwrap();
        assert!(!json.contains("catalog"), "got {json}");
        let parsed: FullIdentifier = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, id);
    }

    #[test]
    fn test_full_identifier_round_trips_with_catalog() {
        let id = FullIdentifier::new(
            Some("prod_cat"),
            &["analytics".to_string(), "raw".to_string()],
            "events",
        );
        let json = serde_json::to_string(&id).unwrap();
        assert!(json.contains("\"catalog\":\"prod_cat\""));
        let parsed: FullIdentifier = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, id);
    }

    #[test]
    fn test_full_identifier_to_identifier_conversion_drops_catalog() {
        let full = FullIdentifier::new(
            Some("prod_cat"),
            &["analytics".to_string(), "raw".to_string()],
            "events",
        );
        let short: Identifier = (&full).into();
        assert_eq!(short.name(), "events");
        assert_eq!(short.namespace().len(), 2);
        assert_eq!(format!("{short}"), "analytics.raw.events");
    }
}

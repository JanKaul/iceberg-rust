#![allow(unused_qualifications)]

#[cfg(any(feature = "client", feature = "server"))]
use crate::header;
use crate::models;

type Namespace = Vec<String>;

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct AddPartitionSpecUpdate {
    // Note: inline enums are not fully supported by openapi-generator
    #[serde(rename = "action")]
    pub action: String,

    #[serde(rename = "spec")]
    pub spec: models::PartitionSpec,
}

impl AddPartitionSpecUpdate {
    #[allow(clippy::new_without_default)]
    pub fn new(action: String, spec: models::PartitionSpec) -> AddPartitionSpecUpdate {
        AddPartitionSpecUpdate { action, spec }
    }
}

/// Converts the AddPartitionSpecUpdate value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::string::ToString for AddPartitionSpecUpdate {
    fn to_string(&self) -> String {
        let params: Vec<Option<String>> = vec![
            Some("action".to_string()),
            Some(self.action.to_string()),
            // Skipping spec in query parameter serialization
        ];

        params.into_iter().flatten().collect::<Vec<_>>().join(",")
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a AddPartitionSpecUpdate value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for AddPartitionSpecUpdate {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub action: Vec<String>,
            pub spec: Vec<models::PartitionSpec>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing AddPartitionSpecUpdate".to_string(),
                    )
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "action" => intermediate_rep.action.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "spec" => intermediate_rep.spec.push(
                        <models::PartitionSpec as std::str::FromStr>::from_str(val)
                            .map_err(|x| x.to_string())?,
                    ),
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing AddPartitionSpecUpdate".to_string(),
                        )
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(AddPartitionSpecUpdate {
            action: intermediate_rep
                .action
                .into_iter()
                .next()
                .ok_or_else(|| "action missing in AddPartitionSpecUpdate".to_string())?,
            spec: intermediate_rep
                .spec
                .into_iter()
                .next()
                .ok_or_else(|| "spec missing in AddPartitionSpecUpdate".to_string())?,
        })
    }
}

// Methods for converting between header::IntoHeaderValue<AddPartitionSpecUpdate> and hyper::header::HeaderValue

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<header::IntoHeaderValue<AddPartitionSpecUpdate>>
    for hyper::header::HeaderValue
{
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<AddPartitionSpecUpdate>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match hyper::header::HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Invalid header value for AddPartitionSpecUpdate - value: {} is invalid {}",
                hdr_value, e
            )),
        }
    }
}

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<hyper::header::HeaderValue>
    for header::IntoHeaderValue<AddPartitionSpecUpdate>
{
    type Error = String;

    fn try_from(hdr_value: hyper::header::HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <AddPartitionSpecUpdate as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        "Unable to convert header value '{}' into AddPartitionSpecUpdate - {}",
                        value, err
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Unable to convert header: {:?} to string: {}",
                hdr_value, e
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct AddPartitionSpecUpdateAllOf {
    #[serde(rename = "spec")]
    pub spec: models::PartitionSpec,
}

impl AddPartitionSpecUpdateAllOf {
    #[allow(clippy::new_without_default)]
    pub fn new(spec: models::PartitionSpec) -> AddPartitionSpecUpdateAllOf {
        AddPartitionSpecUpdateAllOf { spec }
    }
}

/// Converts the AddPartitionSpecUpdateAllOf value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::string::ToString for AddPartitionSpecUpdateAllOf {
    fn to_string(&self) -> String {
        let params: Vec<Option<String>> = vec![
            // Skipping spec in query parameter serialization

        ];

        params.into_iter().flatten().collect::<Vec<_>>().join(",")
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a AddPartitionSpecUpdateAllOf value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for AddPartitionSpecUpdateAllOf {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub spec: Vec<models::PartitionSpec>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing AddPartitionSpecUpdateAllOf".to_string(),
                    )
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "spec" => intermediate_rep.spec.push(
                        <models::PartitionSpec as std::str::FromStr>::from_str(val)
                            .map_err(|x| x.to_string())?,
                    ),
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing AddPartitionSpecUpdateAllOf".to_string(),
                        )
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(AddPartitionSpecUpdateAllOf {
            spec: intermediate_rep
                .spec
                .into_iter()
                .next()
                .ok_or_else(|| "spec missing in AddPartitionSpecUpdateAllOf".to_string())?,
        })
    }
}

// Methods for converting between header::IntoHeaderValue<AddPartitionSpecUpdateAllOf> and hyper::header::HeaderValue

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<header::IntoHeaderValue<AddPartitionSpecUpdateAllOf>>
    for hyper::header::HeaderValue
{
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<AddPartitionSpecUpdateAllOf>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match hyper::header::HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Invalid header value for AddPartitionSpecUpdateAllOf - value: {} is invalid {}",
                hdr_value, e
            )),
        }
    }
}

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<hyper::header::HeaderValue>
    for header::IntoHeaderValue<AddPartitionSpecUpdateAllOf>
{
    type Error = String;

    fn try_from(hdr_value: hyper::header::HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <AddPartitionSpecUpdateAllOf as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        "Unable to convert header value '{}' into AddPartitionSpecUpdateAllOf - {}",
                        value, err
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Unable to convert header: {:?} to string: {}",
                hdr_value, e
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct AddSchemaUpdate {
    // Note: inline enums are not fully supported by openapi-generator
    #[serde(rename = "action")]
    pub action: String,

    #[serde(rename = "schema")]
    pub schema: models::Schema,
}

impl AddSchemaUpdate {
    #[allow(clippy::new_without_default)]
    pub fn new(action: String, schema: models::Schema) -> AddSchemaUpdate {
        AddSchemaUpdate { action, schema }
    }
}

/// Converts the AddSchemaUpdate value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::string::ToString for AddSchemaUpdate {
    fn to_string(&self) -> String {
        let params: Vec<Option<String>> = vec![
            Some("action".to_string()),
            Some(self.action.to_string()),
            // Skipping schema in query parameter serialization
        ];

        params.into_iter().flatten().collect::<Vec<_>>().join(",")
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a AddSchemaUpdate value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for AddSchemaUpdate {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub action: Vec<String>,
            pub schema: Vec<models::Schema>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing AddSchemaUpdate".to_string(),
                    )
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "action" => intermediate_rep.action.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "schema" => intermediate_rep.schema.push(
                        <models::Schema as std::str::FromStr>::from_str(val)
                            .map_err(|x| x.to_string())?,
                    ),
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing AddSchemaUpdate".to_string(),
                        )
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(AddSchemaUpdate {
            action: intermediate_rep
                .action
                .into_iter()
                .next()
                .ok_or_else(|| "action missing in AddSchemaUpdate".to_string())?,
            schema: intermediate_rep
                .schema
                .into_iter()
                .next()
                .ok_or_else(|| "schema missing in AddSchemaUpdate".to_string())?,
        })
    }
}

// Methods for converting between header::IntoHeaderValue<AddSchemaUpdate> and hyper::header::HeaderValue

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<header::IntoHeaderValue<AddSchemaUpdate>>
    for hyper::header::HeaderValue
{
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<AddSchemaUpdate>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match hyper::header::HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Invalid header value for AddSchemaUpdate - value: {} is invalid {}",
                hdr_value, e
            )),
        }
    }
}

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<hyper::header::HeaderValue>
    for header::IntoHeaderValue<AddSchemaUpdate>
{
    type Error = String;

    fn try_from(hdr_value: hyper::header::HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <AddSchemaUpdate as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        "Unable to convert header value '{}' into AddSchemaUpdate - {}",
                        value, err
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Unable to convert header: {:?} to string: {}",
                hdr_value, e
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct AddSchemaUpdateAllOf {
    #[serde(rename = "schema")]
    pub schema: models::Schema,
}

impl AddSchemaUpdateAllOf {
    #[allow(clippy::new_without_default)]
    pub fn new(schema: models::Schema) -> AddSchemaUpdateAllOf {
        AddSchemaUpdateAllOf { schema }
    }
}

/// Converts the AddSchemaUpdateAllOf value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::string::ToString for AddSchemaUpdateAllOf {
    fn to_string(&self) -> String {
        let params: Vec<Option<String>> = vec![
            // Skipping schema in query parameter serialization

        ];

        params.into_iter().flatten().collect::<Vec<_>>().join(",")
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a AddSchemaUpdateAllOf value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for AddSchemaUpdateAllOf {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub schema: Vec<models::Schema>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing AddSchemaUpdateAllOf".to_string(),
                    )
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "schema" => intermediate_rep.schema.push(
                        <models::Schema as std::str::FromStr>::from_str(val)
                            .map_err(|x| x.to_string())?,
                    ),
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing AddSchemaUpdateAllOf".to_string(),
                        )
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(AddSchemaUpdateAllOf {
            schema: intermediate_rep
                .schema
                .into_iter()
                .next()
                .ok_or_else(|| "schema missing in AddSchemaUpdateAllOf".to_string())?,
        })
    }
}

// Methods for converting between header::IntoHeaderValue<AddSchemaUpdateAllOf> and hyper::header::HeaderValue

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<header::IntoHeaderValue<AddSchemaUpdateAllOf>>
    for hyper::header::HeaderValue
{
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<AddSchemaUpdateAllOf>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match hyper::header::HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Invalid header value for AddSchemaUpdateAllOf - value: {} is invalid {}",
                hdr_value, e
            )),
        }
    }
}

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<hyper::header::HeaderValue>
    for header::IntoHeaderValue<AddSchemaUpdateAllOf>
{
    type Error = String;

    fn try_from(hdr_value: hyper::header::HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <AddSchemaUpdateAllOf as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        "Unable to convert header value '{}' into AddSchemaUpdateAllOf - {}",
                        value, err
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Unable to convert header: {:?} to string: {}",
                hdr_value, e
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct AddSnapshotUpdate {
    // Note: inline enums are not fully supported by openapi-generator
    #[serde(rename = "action")]
    pub action: String,

    #[serde(rename = "snapshot")]
    pub snapshot: models::Snapshot,
}

impl AddSnapshotUpdate {
    #[allow(clippy::new_without_default)]
    pub fn new(action: String, snapshot: models::Snapshot) -> AddSnapshotUpdate {
        AddSnapshotUpdate { action, snapshot }
    }
}

/// Converts the AddSnapshotUpdate value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::string::ToString for AddSnapshotUpdate {
    fn to_string(&self) -> String {
        let params: Vec<Option<String>> = vec![
            Some("action".to_string()),
            Some(self.action.to_string()),
            // Skipping snapshot in query parameter serialization
        ];

        params.into_iter().flatten().collect::<Vec<_>>().join(",")
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a AddSnapshotUpdate value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for AddSnapshotUpdate {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub action: Vec<String>,
            pub snapshot: Vec<models::Snapshot>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing AddSnapshotUpdate".to_string(),
                    )
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "action" => intermediate_rep.action.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "snapshot" => intermediate_rep.snapshot.push(
                        <models::Snapshot as std::str::FromStr>::from_str(val)
                            .map_err(|x| x.to_string())?,
                    ),
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing AddSnapshotUpdate".to_string(),
                        )
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(AddSnapshotUpdate {
            action: intermediate_rep
                .action
                .into_iter()
                .next()
                .ok_or_else(|| "action missing in AddSnapshotUpdate".to_string())?,
            snapshot: intermediate_rep
                .snapshot
                .into_iter()
                .next()
                .ok_or_else(|| "snapshot missing in AddSnapshotUpdate".to_string())?,
        })
    }
}

// Methods for converting between header::IntoHeaderValue<AddSnapshotUpdate> and hyper::header::HeaderValue

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<header::IntoHeaderValue<AddSnapshotUpdate>>
    for hyper::header::HeaderValue
{
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<AddSnapshotUpdate>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match hyper::header::HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Invalid header value for AddSnapshotUpdate - value: {} is invalid {}",
                hdr_value, e
            )),
        }
    }
}

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<hyper::header::HeaderValue>
    for header::IntoHeaderValue<AddSnapshotUpdate>
{
    type Error = String;

    fn try_from(hdr_value: hyper::header::HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <AddSnapshotUpdate as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        "Unable to convert header value '{}' into AddSnapshotUpdate - {}",
                        value, err
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Unable to convert header: {:?} to string: {}",
                hdr_value, e
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct AddSnapshotUpdateAllOf {
    #[serde(rename = "snapshot")]
    pub snapshot: models::Snapshot,
}

impl AddSnapshotUpdateAllOf {
    #[allow(clippy::new_without_default)]
    pub fn new(snapshot: models::Snapshot) -> AddSnapshotUpdateAllOf {
        AddSnapshotUpdateAllOf { snapshot }
    }
}

/// Converts the AddSnapshotUpdateAllOf value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::string::ToString for AddSnapshotUpdateAllOf {
    fn to_string(&self) -> String {
        let params: Vec<Option<String>> = vec![
            // Skipping snapshot in query parameter serialization

        ];

        params.into_iter().flatten().collect::<Vec<_>>().join(",")
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a AddSnapshotUpdateAllOf value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for AddSnapshotUpdateAllOf {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub snapshot: Vec<models::Snapshot>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing AddSnapshotUpdateAllOf".to_string(),
                    )
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "snapshot" => intermediate_rep.snapshot.push(
                        <models::Snapshot as std::str::FromStr>::from_str(val)
                            .map_err(|x| x.to_string())?,
                    ),
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing AddSnapshotUpdateAllOf".to_string(),
                        )
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(AddSnapshotUpdateAllOf {
            snapshot: intermediate_rep
                .snapshot
                .into_iter()
                .next()
                .ok_or_else(|| "snapshot missing in AddSnapshotUpdateAllOf".to_string())?,
        })
    }
}

// Methods for converting between header::IntoHeaderValue<AddSnapshotUpdateAllOf> and hyper::header::HeaderValue

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<header::IntoHeaderValue<AddSnapshotUpdateAllOf>>
    for hyper::header::HeaderValue
{
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<AddSnapshotUpdateAllOf>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match hyper::header::HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Invalid header value for AddSnapshotUpdateAllOf - value: {} is invalid {}",
                hdr_value, e
            )),
        }
    }
}

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<hyper::header::HeaderValue>
    for header::IntoHeaderValue<AddSnapshotUpdateAllOf>
{
    type Error = String;

    fn try_from(hdr_value: hyper::header::HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <AddSnapshotUpdateAllOf as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        "Unable to convert header value '{}' into AddSnapshotUpdateAllOf - {}",
                        value, err
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Unable to convert header: {:?} to string: {}",
                hdr_value, e
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct AddSortOrderUpdate {
    // Note: inline enums are not fully supported by openapi-generator
    #[serde(rename = "action")]
    pub action: String,

    #[serde(rename = "sort-order")]
    pub sort_order: models::SortOrder,
}

impl AddSortOrderUpdate {
    #[allow(clippy::new_without_default)]
    pub fn new(action: String, sort_order: models::SortOrder) -> AddSortOrderUpdate {
        AddSortOrderUpdate { action, sort_order }
    }
}

/// Converts the AddSortOrderUpdate value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::string::ToString for AddSortOrderUpdate {
    fn to_string(&self) -> String {
        let params: Vec<Option<String>> = vec![
            Some("action".to_string()),
            Some(self.action.to_string()),
            // Skipping sort-order in query parameter serialization
        ];

        params.into_iter().flatten().collect::<Vec<_>>().join(",")
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a AddSortOrderUpdate value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for AddSortOrderUpdate {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub action: Vec<String>,
            pub sort_order: Vec<models::SortOrder>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing AddSortOrderUpdate".to_string(),
                    )
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "action" => intermediate_rep.action.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "sort-order" => intermediate_rep.sort_order.push(
                        <models::SortOrder as std::str::FromStr>::from_str(val)
                            .map_err(|x| x.to_string())?,
                    ),
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing AddSortOrderUpdate".to_string(),
                        )
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(AddSortOrderUpdate {
            action: intermediate_rep
                .action
                .into_iter()
                .next()
                .ok_or_else(|| "action missing in AddSortOrderUpdate".to_string())?,
            sort_order: intermediate_rep
                .sort_order
                .into_iter()
                .next()
                .ok_or_else(|| "sort-order missing in AddSortOrderUpdate".to_string())?,
        })
    }
}

// Methods for converting between header::IntoHeaderValue<AddSortOrderUpdate> and hyper::header::HeaderValue

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<header::IntoHeaderValue<AddSortOrderUpdate>>
    for hyper::header::HeaderValue
{
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<AddSortOrderUpdate>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match hyper::header::HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Invalid header value for AddSortOrderUpdate - value: {} is invalid {}",
                hdr_value, e
            )),
        }
    }
}

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<hyper::header::HeaderValue>
    for header::IntoHeaderValue<AddSortOrderUpdate>
{
    type Error = String;

    fn try_from(hdr_value: hyper::header::HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <AddSortOrderUpdate as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        "Unable to convert header value '{}' into AddSortOrderUpdate - {}",
                        value, err
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Unable to convert header: {:?} to string: {}",
                hdr_value, e
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct AddSortOrderUpdateAllOf {
    #[serde(rename = "sort-order")]
    pub sort_order: models::SortOrder,
}

impl AddSortOrderUpdateAllOf {
    #[allow(clippy::new_without_default)]
    pub fn new(sort_order: models::SortOrder) -> AddSortOrderUpdateAllOf {
        AddSortOrderUpdateAllOf { sort_order }
    }
}

/// Converts the AddSortOrderUpdateAllOf value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::string::ToString for AddSortOrderUpdateAllOf {
    fn to_string(&self) -> String {
        let params: Vec<Option<String>> = vec![
            // Skipping sort-order in query parameter serialization

        ];

        params.into_iter().flatten().collect::<Vec<_>>().join(",")
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a AddSortOrderUpdateAllOf value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for AddSortOrderUpdateAllOf {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub sort_order: Vec<models::SortOrder>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing AddSortOrderUpdateAllOf".to_string(),
                    )
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "sort-order" => intermediate_rep.sort_order.push(
                        <models::SortOrder as std::str::FromStr>::from_str(val)
                            .map_err(|x| x.to_string())?,
                    ),
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing AddSortOrderUpdateAllOf".to_string(),
                        )
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(AddSortOrderUpdateAllOf {
            sort_order: intermediate_rep
                .sort_order
                .into_iter()
                .next()
                .ok_or_else(|| "sort-order missing in AddSortOrderUpdateAllOf".to_string())?,
        })
    }
}

// Methods for converting between header::IntoHeaderValue<AddSortOrderUpdateAllOf> and hyper::header::HeaderValue

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<header::IntoHeaderValue<AddSortOrderUpdateAllOf>>
    for hyper::header::HeaderValue
{
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<AddSortOrderUpdateAllOf>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match hyper::header::HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Invalid header value for AddSortOrderUpdateAllOf - value: {} is invalid {}",
                hdr_value, e
            )),
        }
    }
}

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<hyper::header::HeaderValue>
    for header::IntoHeaderValue<AddSortOrderUpdateAllOf>
{
    type Error = String;

    fn try_from(hdr_value: hyper::header::HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <AddSortOrderUpdateAllOf as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        "Unable to convert header value '{}' into AddSortOrderUpdateAllOf - {}",
                        value, err
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Unable to convert header: {:?} to string: {}",
                hdr_value, e
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct AndOrExpression {
    #[serde(rename = "type")]
    pub r#type: String,

    #[serde(rename = "left")]
    pub left: models::Expression,

    #[serde(rename = "right")]
    pub right: models::Expression,
}

impl AndOrExpression {
    #[allow(clippy::new_without_default)]
    pub fn new(
        r#type: String,
        left: models::Expression,
        right: models::Expression,
    ) -> AndOrExpression {
        AndOrExpression {
            r#type,
            left,
            right,
        }
    }
}

/// Converts the AndOrExpression value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::string::ToString for AndOrExpression {
    fn to_string(&self) -> String {
        let params: Vec<Option<String>> = vec![
            Some("type".to_string()),
            Some(self.r#type.to_string()),
            // Skipping left in query parameter serialization

            // Skipping right in query parameter serialization
        ];

        params.into_iter().flatten().collect::<Vec<_>>().join(",")
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a AndOrExpression value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for AndOrExpression {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub r#type: Vec<String>,
            pub left: Vec<models::Expression>,
            pub right: Vec<models::Expression>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing AndOrExpression".to_string(),
                    )
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "type" => intermediate_rep.r#type.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "left" => intermediate_rep.left.push(
                        <models::Expression as std::str::FromStr>::from_str(val)
                            .map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "right" => intermediate_rep.right.push(
                        <models::Expression as std::str::FromStr>::from_str(val)
                            .map_err(|x| x.to_string())?,
                    ),
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing AndOrExpression".to_string(),
                        )
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(AndOrExpression {
            r#type: intermediate_rep
                .r#type
                .into_iter()
                .next()
                .ok_or_else(|| "type missing in AndOrExpression".to_string())?,
            left: intermediate_rep
                .left
                .into_iter()
                .next()
                .ok_or_else(|| "left missing in AndOrExpression".to_string())?,
            right: intermediate_rep
                .right
                .into_iter()
                .next()
                .ok_or_else(|| "right missing in AndOrExpression".to_string())?,
        })
    }
}

// Methods for converting between header::IntoHeaderValue<AndOrExpression> and hyper::header::HeaderValue

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<header::IntoHeaderValue<AndOrExpression>>
    for hyper::header::HeaderValue
{
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<AndOrExpression>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match hyper::header::HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Invalid header value for AndOrExpression - value: {} is invalid {}",
                hdr_value, e
            )),
        }
    }
}

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<hyper::header::HeaderValue>
    for header::IntoHeaderValue<AndOrExpression>
{
    type Error = String;

    fn try_from(hdr_value: hyper::header::HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <AndOrExpression as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        "Unable to convert header value '{}' into AndOrExpression - {}",
                        value, err
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Unable to convert header: {:?} to string: {}",
                hdr_value, e
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct BaseUpdate {
    // Note: inline enums are not fully supported by openapi-generator
    #[serde(rename = "action")]
    pub action: String,
}

impl BaseUpdate {
    #[allow(clippy::new_without_default)]
    pub fn new(action: String) -> BaseUpdate {
        BaseUpdate { action }
    }
}

/// Converts the BaseUpdate value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::string::ToString for BaseUpdate {
    fn to_string(&self) -> String {
        let params: Vec<Option<String>> =
            vec![Some("action".to_string()), Some(self.action.to_string())];

        params.into_iter().flatten().collect::<Vec<_>>().join(",")
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a BaseUpdate value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for BaseUpdate {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub action: Vec<String>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing BaseUpdate".to_string(),
                    )
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "action" => intermediate_rep.action.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing BaseUpdate".to_string(),
                        )
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(BaseUpdate {
            action: intermediate_rep
                .action
                .into_iter()
                .next()
                .ok_or_else(|| "action missing in BaseUpdate".to_string())?,
        })
    }
}

// Methods for converting between header::IntoHeaderValue<BaseUpdate> and hyper::header::HeaderValue

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<header::IntoHeaderValue<BaseUpdate>> for hyper::header::HeaderValue {
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<BaseUpdate>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match hyper::header::HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Invalid header value for BaseUpdate - value: {} is invalid {}",
                hdr_value, e
            )),
        }
    }
}

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<hyper::header::HeaderValue> for header::IntoHeaderValue<BaseUpdate> {
    type Error = String;

    fn try_from(hdr_value: hyper::header::HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <BaseUpdate as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        "Unable to convert header value '{}' into BaseUpdate - {}",
                        value, err
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Unable to convert header: {:?} to string: {}",
                hdr_value, e
            )),
        }
    }
}

/// Server-provided configuration for the catalog.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct CatalogConfig {
    /// Properties that should be used to override client configuration; applied after defaults and client configuration.
    #[serde(rename = "overrides")]
    pub overrides: serde_json::Value,

    /// Properties that should be used as default configuration; applied before client configuration.
    #[serde(rename = "defaults")]
    pub defaults: serde_json::Value,
}

impl CatalogConfig {
    #[allow(clippy::new_without_default)]
    pub fn new(overrides: serde_json::Value, defaults: serde_json::Value) -> CatalogConfig {
        CatalogConfig {
            overrides,
            defaults,
        }
    }
}

/// Converts the CatalogConfig value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::string::ToString for CatalogConfig {
    fn to_string(&self) -> String {
        let params: Vec<Option<String>> = vec![
            // Skipping overrides in query parameter serialization

            // Skipping defaults in query parameter serialization

        ];

        params.into_iter().flatten().collect::<Vec<_>>().join(",")
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a CatalogConfig value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for CatalogConfig {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub overrides: Vec<serde_json::Value>,
            pub defaults: Vec<serde_json::Value>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing CatalogConfig".to_string(),
                    )
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "overrides" => intermediate_rep.overrides.push(
                        <serde_json::Value as std::str::FromStr>::from_str(val)
                            .map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "defaults" => intermediate_rep.defaults.push(
                        <serde_json::Value as std::str::FromStr>::from_str(val)
                            .map_err(|x| x.to_string())?,
                    ),
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing CatalogConfig".to_string(),
                        )
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(CatalogConfig {
            overrides: intermediate_rep
                .overrides
                .into_iter()
                .next()
                .ok_or_else(|| "overrides missing in CatalogConfig".to_string())?,
            defaults: intermediate_rep
                .defaults
                .into_iter()
                .next()
                .ok_or_else(|| "defaults missing in CatalogConfig".to_string())?,
        })
    }
}

// Methods for converting between header::IntoHeaderValue<CatalogConfig> and hyper::header::HeaderValue

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<header::IntoHeaderValue<CatalogConfig>> for hyper::header::HeaderValue {
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<CatalogConfig>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match hyper::header::HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Invalid header value for CatalogConfig - value: {} is invalid {}",
                hdr_value, e
            )),
        }
    }
}

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<hyper::header::HeaderValue> for header::IntoHeaderValue<CatalogConfig> {
    type Error = String;

    fn try_from(hdr_value: hyper::header::HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <CatalogConfig as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        "Unable to convert header value '{}' into CatalogConfig - {}",
                        value, err
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Unable to convert header: {:?} to string: {}",
                hdr_value, e
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct CommitTableRequest {
    #[serde(rename = "requirements")]
    pub requirements: Vec<models::TableRequirement>,

    #[serde(rename = "updates")]
    pub updates: Vec<models::TableUpdate>,
}

impl CommitTableRequest {
    #[allow(clippy::new_without_default)]
    pub fn new(
        requirements: Vec<models::TableRequirement>,
        updates: Vec<models::TableUpdate>,
    ) -> CommitTableRequest {
        CommitTableRequest {
            requirements,
            updates,
        }
    }
}

/// Converts the CommitTableRequest value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::string::ToString for CommitTableRequest {
    fn to_string(&self) -> String {
        let params: Vec<Option<String>> = vec![
            // Skipping requirements in query parameter serialization

            // Skipping updates in query parameter serialization

        ];

        params.into_iter().flatten().collect::<Vec<_>>().join(",")
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a CommitTableRequest value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for CommitTableRequest {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub requirements: Vec<Vec<models::TableRequirement>>,
            pub updates: Vec<Vec<models::TableUpdate>>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing CommitTableRequest".to_string(),
                    )
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    "requirements" => return std::result::Result::Err(
                        "Parsing a container in this style is not supported in CommitTableRequest"
                            .to_string(),
                    ),
                    "updates" => return std::result::Result::Err(
                        "Parsing a container in this style is not supported in CommitTableRequest"
                            .to_string(),
                    ),
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing CommitTableRequest".to_string(),
                        )
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(CommitTableRequest {
            requirements: intermediate_rep
                .requirements
                .into_iter()
                .next()
                .ok_or_else(|| "requirements missing in CommitTableRequest".to_string())?,
            updates: intermediate_rep
                .updates
                .into_iter()
                .next()
                .ok_or_else(|| "updates missing in CommitTableRequest".to_string())?,
        })
    }
}

// Methods for converting between header::IntoHeaderValue<CommitTableRequest> and hyper::header::HeaderValue

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<header::IntoHeaderValue<CommitTableRequest>>
    for hyper::header::HeaderValue
{
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<CommitTableRequest>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match hyper::header::HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Invalid header value for CommitTableRequest - value: {} is invalid {}",
                hdr_value, e
            )),
        }
    }
}

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<hyper::header::HeaderValue>
    for header::IntoHeaderValue<CommitTableRequest>
{
    type Error = String;

    fn try_from(hdr_value: hyper::header::HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <CommitTableRequest as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        "Unable to convert header value '{}' into CommitTableRequest - {}",
                        value, err
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Unable to convert header: {:?} to string: {}",
                hdr_value, e
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct CounterResult {
    #[serde(rename = "unit")]
    pub unit: String,

    #[serde(rename = "value")]
    pub value: i64,
}

impl CounterResult {
    #[allow(clippy::new_without_default)]
    pub fn new(unit: String, value: i64) -> CounterResult {
        CounterResult { unit, value }
    }
}

/// Converts the CounterResult value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::string::ToString for CounterResult {
    fn to_string(&self) -> String {
        let params: Vec<Option<String>> = vec![
            Some("unit".to_string()),
            Some(self.unit.to_string()),
            Some("value".to_string()),
            Some(self.value.to_string()),
        ];

        params.into_iter().flatten().collect::<Vec<_>>().join(",")
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a CounterResult value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for CounterResult {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub unit: Vec<String>,
            pub value: Vec<i64>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing CounterResult".to_string(),
                    )
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "unit" => intermediate_rep.unit.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "value" => intermediate_rep.value.push(
                        <i64 as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing CounterResult".to_string(),
                        )
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(CounterResult {
            unit: intermediate_rep
                .unit
                .into_iter()
                .next()
                .ok_or_else(|| "unit missing in CounterResult".to_string())?,
            value: intermediate_rep
                .value
                .into_iter()
                .next()
                .ok_or_else(|| "value missing in CounterResult".to_string())?,
        })
    }
}

// Methods for converting between header::IntoHeaderValue<CounterResult> and hyper::header::HeaderValue

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<header::IntoHeaderValue<CounterResult>> for hyper::header::HeaderValue {
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<CounterResult>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match hyper::header::HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Invalid header value for CounterResult - value: {} is invalid {}",
                hdr_value, e
            )),
        }
    }
}

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<hyper::header::HeaderValue> for header::IntoHeaderValue<CounterResult> {
    type Error = String;

    fn try_from(hdr_value: hyper::header::HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <CounterResult as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        "Unable to convert header value '{}' into CounterResult - {}",
                        value, err
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Unable to convert header: {:?} to string: {}",
                hdr_value, e
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct CreateNamespace200Response {
    /// Reference to one or more levels of a namespace
    #[serde(rename = "namespace")]
    pub namespace: Vec<String>,

    /// Properties stored on the namespace, if supported by the server.
    #[serde(rename = "properties")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub properties: Option<std::collections::HashMap<String, String>>,
}

impl CreateNamespace200Response {
    #[allow(clippy::new_without_default)]
    pub fn new(namespace: Vec<String>) -> CreateNamespace200Response {
        CreateNamespace200Response {
            namespace,
            properties: None,
        }
    }
}

/// Converts the CreateNamespace200Response value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::string::ToString for CreateNamespace200Response {
    fn to_string(&self) -> String {
        let params: Vec<Option<String>> = vec![
            Some("namespace".to_string()),
            Some(
                self.namespace
                    .iter()
                    .map(|x| x.to_string())
                    .collect::<Vec<_>>()
                    .join(","),
            ),
            // Skipping properties in query parameter serialization
        ];

        params.into_iter().flatten().collect::<Vec<_>>().join(",")
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a CreateNamespace200Response value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for CreateNamespace200Response {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub namespace: Vec<Vec<String>>,
            pub properties: Vec<std::collections::HashMap<String, String>>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing CreateNamespace200Response".to_string(),
                    )
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    "namespace" => return std::result::Result::Err("Parsing a container in this style is not supported in CreateNamespace200Response".to_string()),
                    "properties" => return std::result::Result::Err("Parsing a container in this style is not supported in CreateNamespace200Response".to_string()),
                    _ => return std::result::Result::Err("Unexpected key while parsing CreateNamespace200Response".to_string())
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(CreateNamespace200Response {
            namespace: intermediate_rep
                .namespace
                .into_iter()
                .next()
                .ok_or_else(|| "namespace missing in CreateNamespace200Response".to_string())?,
            properties: intermediate_rep.properties.into_iter().next(),
        })
    }
}

// Methods for converting between header::IntoHeaderValue<CreateNamespace200Response> and hyper::header::HeaderValue

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<header::IntoHeaderValue<CreateNamespace200Response>>
    for hyper::header::HeaderValue
{
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<CreateNamespace200Response>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match hyper::header::HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Invalid header value for CreateNamespace200Response - value: {} is invalid {}",
                hdr_value, e
            )),
        }
    }
}

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<hyper::header::HeaderValue>
    for header::IntoHeaderValue<CreateNamespace200Response>
{
    type Error = String;

    fn try_from(hdr_value: hyper::header::HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <CreateNamespace200Response as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        "Unable to convert header value '{}' into CreateNamespace200Response - {}",
                        value, err
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Unable to convert header: {:?} to string: {}",
                hdr_value, e
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct CreateNamespaceRequest {
    /// Reference to one or more levels of a namespace
    #[serde(rename = "namespace")]
    pub namespace: Vec<String>,

    /// Configured string to string map of properties for the namespace
    #[serde(rename = "properties")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub properties: Option<serde_json::Value>,
}

impl CreateNamespaceRequest {
    #[allow(clippy::new_without_default)]
    pub fn new(namespace: Vec<String>) -> CreateNamespaceRequest {
        CreateNamespaceRequest {
            namespace,
            properties: None,
        }
    }
}

/// Converts the CreateNamespaceRequest value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::string::ToString for CreateNamespaceRequest {
    fn to_string(&self) -> String {
        let params: Vec<Option<String>> = vec![
            Some("namespace".to_string()),
            Some(
                self.namespace
                    .iter()
                    .map(|x| x.to_string())
                    .collect::<Vec<_>>()
                    .join(","),
            ),
            // Skipping properties in query parameter serialization
        ];

        params.into_iter().flatten().collect::<Vec<_>>().join(",")
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a CreateNamespaceRequest value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for CreateNamespaceRequest {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub namespace: Vec<Vec<String>>,
            pub properties: Vec<serde_json::Value>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing CreateNamespaceRequest".to_string(),
                    )
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    "namespace" => return std::result::Result::Err("Parsing a container in this style is not supported in CreateNamespaceRequest".to_string()),
                    #[allow(clippy::redundant_clone)]
                    "properties" => intermediate_rep.properties.push(<serde_json::Value as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?),
                    _ => return std::result::Result::Err("Unexpected key while parsing CreateNamespaceRequest".to_string())
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(CreateNamespaceRequest {
            namespace: intermediate_rep
                .namespace
                .into_iter()
                .next()
                .ok_or_else(|| "namespace missing in CreateNamespaceRequest".to_string())?,
            properties: intermediate_rep.properties.into_iter().next(),
        })
    }
}

// Methods for converting between header::IntoHeaderValue<CreateNamespaceRequest> and hyper::header::HeaderValue

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<header::IntoHeaderValue<CreateNamespaceRequest>>
    for hyper::header::HeaderValue
{
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<CreateNamespaceRequest>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match hyper::header::HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Invalid header value for CreateNamespaceRequest - value: {} is invalid {}",
                hdr_value, e
            )),
        }
    }
}

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<hyper::header::HeaderValue>
    for header::IntoHeaderValue<CreateNamespaceRequest>
{
    type Error = String;

    fn try_from(hdr_value: hyper::header::HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <CreateNamespaceRequest as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        "Unable to convert header value '{}' into CreateNamespaceRequest - {}",
                        value, err
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Unable to convert header: {:?} to string: {}",
                hdr_value, e
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct CreateTableRequest {
    #[serde(rename = "name")]
    pub name: String,

    #[serde(rename = "location")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub location: Option<String>,

    #[serde(rename = "schema")]
    pub schema: models::Schema,

    #[serde(rename = "partition-spec")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition_spec: Option<models::PartitionSpec>,

    #[serde(rename = "write-order")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub write_order: Option<models::SortOrder>,

    #[serde(rename = "stage-create")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stage_create: Option<bool>,

    #[serde(rename = "properties")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub properties: Option<std::collections::HashMap<String, String>>,
}

impl CreateTableRequest {
    #[allow(clippy::new_without_default)]
    pub fn new(name: String, schema: models::Schema) -> CreateTableRequest {
        CreateTableRequest {
            name,
            location: None,
            schema,
            partition_spec: None,
            write_order: None,
            stage_create: None,
            properties: None,
        }
    }
}

/// Converts the CreateTableRequest value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::string::ToString for CreateTableRequest {
    fn to_string(&self) -> String {
        let params: Vec<Option<String>> = vec![
            Some("name".to_string()),
            Some(self.name.to_string()),
            self.location
                .as_ref()
                .map(|location| vec!["location".to_string(), location.to_string()].join(",")),
            // Skipping schema in query parameter serialization

            // Skipping partition-spec in query parameter serialization

            // Skipping write-order in query parameter serialization
            self.stage_create.as_ref().map(|stage_create| {
                vec!["stage-create".to_string(), stage_create.to_string()].join(",")
            }),
            // Skipping properties in query parameter serialization
        ];

        params.into_iter().flatten().collect::<Vec<_>>().join(",")
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a CreateTableRequest value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for CreateTableRequest {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub name: Vec<String>,
            pub location: Vec<String>,
            pub schema: Vec<models::Schema>,
            pub partition_spec: Vec<models::PartitionSpec>,
            pub write_order: Vec<models::SortOrder>,
            pub stage_create: Vec<bool>,
            pub properties: Vec<std::collections::HashMap<String, String>>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing CreateTableRequest".to_string(),
                    )
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "name" => intermediate_rep.name.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "location" => intermediate_rep.location.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "schema" => intermediate_rep.schema.push(
                        <models::Schema as std::str::FromStr>::from_str(val)
                            .map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "partition-spec" => intermediate_rep.partition_spec.push(
                        <models::PartitionSpec as std::str::FromStr>::from_str(val)
                            .map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "write-order" => intermediate_rep.write_order.push(
                        <models::SortOrder as std::str::FromStr>::from_str(val)
                            .map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "stage-create" => intermediate_rep.stage_create.push(
                        <bool as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    "properties" => return std::result::Result::Err(
                        "Parsing a container in this style is not supported in CreateTableRequest"
                            .to_string(),
                    ),
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing CreateTableRequest".to_string(),
                        )
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(CreateTableRequest {
            name: intermediate_rep
                .name
                .into_iter()
                .next()
                .ok_or_else(|| "name missing in CreateTableRequest".to_string())?,
            location: intermediate_rep.location.into_iter().next(),
            schema: intermediate_rep
                .schema
                .into_iter()
                .next()
                .ok_or_else(|| "schema missing in CreateTableRequest".to_string())?,
            partition_spec: intermediate_rep.partition_spec.into_iter().next(),
            write_order: intermediate_rep.write_order.into_iter().next(),
            stage_create: intermediate_rep.stage_create.into_iter().next(),
            properties: intermediate_rep.properties.into_iter().next(),
        })
    }
}

// Methods for converting between header::IntoHeaderValue<CreateTableRequest> and hyper::header::HeaderValue

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<header::IntoHeaderValue<CreateTableRequest>>
    for hyper::header::HeaderValue
{
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<CreateTableRequest>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match hyper::header::HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Invalid header value for CreateTableRequest - value: {} is invalid {}",
                hdr_value, e
            )),
        }
    }
}

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<hyper::header::HeaderValue>
    for header::IntoHeaderValue<CreateTableRequest>
{
    type Error = String;

    fn try_from(hdr_value: hyper::header::HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <CreateTableRequest as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        "Unable to convert header value '{}' into CreateTableRequest - {}",
                        value, err
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Unable to convert header: {:?} to string: {}",
                hdr_value, e
            )),
        }
    }
}

/// JSON error payload returned in a response with further details on the error
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct ErrorModel {
    /// Human-readable error message
    #[serde(rename = "message")]
    pub message: String,

    /// Internal type definition of the error
    #[serde(rename = "type")]
    pub r#type: String,

    /// HTTP response code
    #[serde(rename = "code")]
    pub code: u16,

    #[serde(rename = "stack")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stack: Option<Vec<String>>,
}

impl ErrorModel {
    #[allow(clippy::new_without_default)]
    pub fn new(message: String, r#type: String, code: u16) -> ErrorModel {
        ErrorModel {
            message,
            r#type,
            code,
            stack: None,
        }
    }
}

/// Converts the ErrorModel value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::string::ToString for ErrorModel {
    fn to_string(&self) -> String {
        let params: Vec<Option<String>> = vec![
            Some("message".to_string()),
            Some(self.message.to_string()),
            Some("type".to_string()),
            Some(self.r#type.to_string()),
            Some("code".to_string()),
            Some(self.code.to_string()),
            self.stack.as_ref().map(|stack| {
                vec![
                    "stack".to_string(),
                    stack
                        .iter()
                        .map(|x| x.to_string())
                        .collect::<Vec<_>>()
                        .join(","),
                ]
                .join(",")
            }),
        ];

        params.into_iter().flatten().collect::<Vec<_>>().join(",")
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a ErrorModel value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for ErrorModel {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub message: Vec<String>,
            pub r#type: Vec<String>,
            pub code: Vec<u16>,
            pub stack: Vec<Vec<String>>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing ErrorModel".to_string(),
                    )
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "message" => intermediate_rep.message.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "type" => intermediate_rep.r#type.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "code" => intermediate_rep.code.push(
                        <u16 as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    "stack" => {
                        return std::result::Result::Err(
                            "Parsing a container in this style is not supported in ErrorModel"
                                .to_string(),
                        )
                    }
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing ErrorModel".to_string(),
                        )
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(ErrorModel {
            message: intermediate_rep
                .message
                .into_iter()
                .next()
                .ok_or_else(|| "message missing in ErrorModel".to_string())?,
            r#type: intermediate_rep
                .r#type
                .into_iter()
                .next()
                .ok_or_else(|| "type missing in ErrorModel".to_string())?,
            code: intermediate_rep
                .code
                .into_iter()
                .next()
                .ok_or_else(|| "code missing in ErrorModel".to_string())?,
            stack: intermediate_rep.stack.into_iter().next(),
        })
    }
}

// Methods for converting between header::IntoHeaderValue<ErrorModel> and hyper::header::HeaderValue

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<header::IntoHeaderValue<ErrorModel>> for hyper::header::HeaderValue {
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<ErrorModel>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match hyper::header::HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Invalid header value for ErrorModel - value: {} is invalid {}",
                hdr_value, e
            )),
        }
    }
}

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<hyper::header::HeaderValue> for header::IntoHeaderValue<ErrorModel> {
    type Error = String;

    fn try_from(hdr_value: hyper::header::HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <ErrorModel as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        "Unable to convert header value '{}' into ErrorModel - {}",
                        value, err
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Unable to convert header: {:?} to string: {}",
                hdr_value, e
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct Expression {
    #[serde(rename = "type")]
    pub r#type: String,

    #[serde(rename = "left")]
    pub left: Box<Expression>,

    #[serde(rename = "right")]
    pub right: Box<Expression>,

    #[serde(rename = "child")]
    pub child: Box<Expression>,

    #[serde(rename = "term")]
    pub term: models::Term,

    #[serde(rename = "values")]
    pub values: Vec<serde_json::Value>,

    #[serde(rename = "value")]
    pub value: serde_json::Value,
}

impl Expression {
    #[allow(clippy::new_without_default)]
    pub fn new(
        r#type: String,
        left: models::Expression,
        right: models::Expression,
        child: models::Expression,
        term: models::Term,
        values: Vec<serde_json::Value>,
        value: serde_json::Value,
    ) -> Expression {
        Expression {
            r#type,
            left: Box::new(left),
            right: Box::new(right),
            child: Box::new(child),
            term,
            values,
            value,
        }
    }
}

/// Converts the Expression value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::string::ToString for Expression {
    fn to_string(&self) -> String {
        let params: Vec<Option<String>> = vec![
            Some("type".to_string()),
            Some(self.r#type.to_string()),
            // Skipping left in query parameter serialization

            // Skipping right in query parameter serialization

            // Skipping child in query parameter serialization

            // Skipping term in query parameter serialization

            // Skipping values in query parameter serialization

            // Skipping value in query parameter serialization
        ];

        params.into_iter().flatten().collect::<Vec<_>>().join(",")
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a Expression value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for Expression {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub r#type: Vec<String>,
            pub left: Vec<models::Expression>,
            pub right: Vec<models::Expression>,
            pub child: Vec<models::Expression>,
            pub term: Vec<models::Term>,
            pub values: Vec<Vec<serde_json::Value>>,
            pub value: Vec<serde_json::Value>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing Expression".to_string(),
                    )
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "type" => intermediate_rep.r#type.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "left" => intermediate_rep.left.push(
                        <models::Expression as std::str::FromStr>::from_str(val)
                            .map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "right" => intermediate_rep.right.push(
                        <models::Expression as std::str::FromStr>::from_str(val)
                            .map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "child" => intermediate_rep.child.push(
                        <models::Expression as std::str::FromStr>::from_str(val)
                            .map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "term" => intermediate_rep.term.push(
                        <models::Term as std::str::FromStr>::from_str(val)
                            .map_err(|x| x.to_string())?,
                    ),
                    "values" => {
                        return std::result::Result::Err(
                            "Parsing a container in this style is not supported in Expression"
                                .to_string(),
                        )
                    }
                    #[allow(clippy::redundant_clone)]
                    "value" => intermediate_rep.value.push(
                        <serde_json::Value as std::str::FromStr>::from_str(val)
                            .map_err(|x| x.to_string())?,
                    ),
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing Expression".to_string(),
                        )
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(Expression {
            r#type: intermediate_rep
                .r#type
                .into_iter()
                .next()
                .ok_or_else(|| "type missing in Expression".to_string())?,
            left: Box::new(
                intermediate_rep
                    .left
                    .into_iter()
                    .next()
                    .ok_or_else(|| "left missing in Expression".to_string())?,
            ),
            right: Box::new(
                intermediate_rep
                    .right
                    .into_iter()
                    .next()
                    .ok_or_else(|| "right missing in Expression".to_string())?,
            ),
            child: Box::new(
                intermediate_rep
                    .child
                    .into_iter()
                    .next()
                    .ok_or_else(|| "child missing in Expression".to_string())?,
            ),
            term: intermediate_rep
                .term
                .into_iter()
                .next()
                .ok_or_else(|| "term missing in Expression".to_string())?,
            values: intermediate_rep
                .values
                .into_iter()
                .next()
                .ok_or_else(|| "values missing in Expression".to_string())?,
            value: intermediate_rep
                .value
                .into_iter()
                .next()
                .ok_or_else(|| "value missing in Expression".to_string())?,
        })
    }
}

// Methods for converting between header::IntoHeaderValue<Expression> and hyper::header::HeaderValue

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<header::IntoHeaderValue<Expression>> for hyper::header::HeaderValue {
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<Expression>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match hyper::header::HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Invalid header value for Expression - value: {} is invalid {}",
                hdr_value, e
            )),
        }
    }
}

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<hyper::header::HeaderValue> for header::IntoHeaderValue<Expression> {
    type Error = String;

    fn try_from(hdr_value: hyper::header::HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <Expression as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        "Unable to convert header value '{}' into Expression - {}",
                        value, err
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Unable to convert header: {:?} to string: {}",
                hdr_value, e
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct ExpressionType(String);

impl std::convert::From<String> for ExpressionType {
    fn from(x: String) -> Self {
        ExpressionType(x)
    }
}

impl std::string::ToString for ExpressionType {
    fn to_string(&self) -> String {
        self.0.to_string()
    }
}

impl std::str::FromStr for ExpressionType {
    type Err = std::string::ParseError;
    fn from_str(x: &str) -> std::result::Result<Self, Self::Err> {
        std::result::Result::Ok(ExpressionType(x.to_string()))
    }
}

impl std::convert::From<ExpressionType> for String {
    fn from(x: ExpressionType) -> Self {
        x.0
    }
}

impl std::ops::Deref for ExpressionType {
    type Target = String;
    fn deref(&self) -> &String {
        &self.0
    }
}

impl std::ops::DerefMut for ExpressionType {
    fn deref_mut(&mut self) -> &mut String {
        &mut self.0
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct GetToken200Response {
    /// The access token, for client credentials or token exchange
    #[serde(rename = "access_token")]
    pub access_token: String,

    /// Access token type for client credentials or token exchange  See https://datatracker.ietf.org/doc/html/rfc6749#section-7.1
    // Note: inline enums are not fully supported by openapi-generator
    #[serde(rename = "token_type")]
    pub token_type: String,

    /// Lifetime of the access token in seconds for client credentials or token exchange
    #[serde(rename = "expires_in")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_in: Option<isize>,

    #[serde(rename = "issued_token_type")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub issued_token_type: Option<models::TokenType>,

    /// Refresh token for client credentials or token exchange
    #[serde(rename = "refresh_token")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refresh_token: Option<String>,

    /// Authorization scope for client credentials or token exchange
    #[serde(rename = "scope")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
}

impl GetToken200Response {
    #[allow(clippy::new_without_default)]
    pub fn new(access_token: String, token_type: String) -> GetToken200Response {
        GetToken200Response {
            access_token,
            token_type,
            expires_in: None,
            issued_token_type: None,
            refresh_token: None,
            scope: None,
        }
    }
}

/// Converts the GetToken200Response value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::string::ToString for GetToken200Response {
    fn to_string(&self) -> String {
        let params: Vec<Option<String>> = vec![
            Some("access_token".to_string()),
            Some(self.access_token.to_string()),
            Some("token_type".to_string()),
            Some(self.token_type.to_string()),
            self.expires_in
                .as_ref()
                .map(|expires_in| vec!["expires_in".to_string(), expires_in.to_string()].join(",")),
            // Skipping issued_token_type in query parameter serialization
            self.refresh_token.as_ref().map(|refresh_token| {
                vec!["refresh_token".to_string(), refresh_token.to_string()].join(",")
            }),
            self.scope
                .as_ref()
                .map(|scope| vec!["scope".to_string(), scope.to_string()].join(",")),
        ];

        params.into_iter().flatten().collect::<Vec<_>>().join(",")
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a GetToken200Response value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for GetToken200Response {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub access_token: Vec<String>,
            pub token_type: Vec<String>,
            pub expires_in: Vec<isize>,
            pub issued_token_type: Vec<models::TokenType>,
            pub refresh_token: Vec<String>,
            pub scope: Vec<String>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing GetToken200Response".to_string(),
                    )
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "access_token" => intermediate_rep.access_token.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "token_type" => intermediate_rep.token_type.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "expires_in" => intermediate_rep.expires_in.push(
                        <isize as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "issued_token_type" => intermediate_rep.issued_token_type.push(
                        <models::TokenType as std::str::FromStr>::from_str(val)
                            .map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "refresh_token" => intermediate_rep.refresh_token.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "scope" => intermediate_rep.scope.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing GetToken200Response".to_string(),
                        )
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(GetToken200Response {
            access_token: intermediate_rep
                .access_token
                .into_iter()
                .next()
                .ok_or_else(|| "access_token missing in GetToken200Response".to_string())?,
            token_type: intermediate_rep
                .token_type
                .into_iter()
                .next()
                .ok_or_else(|| "token_type missing in GetToken200Response".to_string())?,
            expires_in: intermediate_rep.expires_in.into_iter().next(),
            issued_token_type: intermediate_rep.issued_token_type.into_iter().next(),
            refresh_token: intermediate_rep.refresh_token.into_iter().next(),
            scope: intermediate_rep.scope.into_iter().next(),
        })
    }
}

// Methods for converting between header::IntoHeaderValue<GetToken200Response> and hyper::header::HeaderValue

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<header::IntoHeaderValue<GetToken200Response>>
    for hyper::header::HeaderValue
{
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<GetToken200Response>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match hyper::header::HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Invalid header value for GetToken200Response - value: {} is invalid {}",
                hdr_value, e
            )),
        }
    }
}

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<hyper::header::HeaderValue>
    for header::IntoHeaderValue<GetToken200Response>
{
    type Error = String;

    fn try_from(hdr_value: hyper::header::HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <GetToken200Response as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        "Unable to convert header value '{}' into GetToken200Response - {}",
                        value, err
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Unable to convert header: {:?} to string: {}",
                hdr_value, e
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct GetToken400Response {
    // Note: inline enums are not fully supported by openapi-generator
    #[serde(rename = "error")]
    pub error: String,

    #[serde(rename = "error_description")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_description: Option<String>,

    #[serde(rename = "error_uri")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_uri: Option<String>,
}

impl GetToken400Response {
    #[allow(clippy::new_without_default)]
    pub fn new(error: String) -> GetToken400Response {
        GetToken400Response {
            error,
            error_description: None,
            error_uri: None,
        }
    }
}

/// Converts the GetToken400Response value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::string::ToString for GetToken400Response {
    fn to_string(&self) -> String {
        let params: Vec<Option<String>> = vec![
            Some("error".to_string()),
            Some(self.error.to_string()),
            self.error_description.as_ref().map(|error_description| {
                vec![
                    "error_description".to_string(),
                    error_description.to_string(),
                ]
                .join(",")
            }),
            self.error_uri
                .as_ref()
                .map(|error_uri| vec!["error_uri".to_string(), error_uri.to_string()].join(",")),
        ];

        params.into_iter().flatten().collect::<Vec<_>>().join(",")
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a GetToken400Response value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for GetToken400Response {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub error: Vec<String>,
            pub error_description: Vec<String>,
            pub error_uri: Vec<String>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing GetToken400Response".to_string(),
                    )
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "error" => intermediate_rep.error.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "error_description" => intermediate_rep.error_description.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "error_uri" => intermediate_rep.error_uri.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing GetToken400Response".to_string(),
                        )
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(GetToken400Response {
            error: intermediate_rep
                .error
                .into_iter()
                .next()
                .ok_or_else(|| "error missing in GetToken400Response".to_string())?,
            error_description: intermediate_rep.error_description.into_iter().next(),
            error_uri: intermediate_rep.error_uri.into_iter().next(),
        })
    }
}

// Methods for converting between header::IntoHeaderValue<GetToken400Response> and hyper::header::HeaderValue

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<header::IntoHeaderValue<GetToken400Response>>
    for hyper::header::HeaderValue
{
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<GetToken400Response>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match hyper::header::HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Invalid header value for GetToken400Response - value: {} is invalid {}",
                hdr_value, e
            )),
        }
    }
}

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<hyper::header::HeaderValue>
    for header::IntoHeaderValue<GetToken400Response>
{
    type Error = String;

    fn try_from(hdr_value: hyper::header::HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <GetToken400Response as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        "Unable to convert header value '{}' into GetToken400Response - {}",
                        value, err
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Unable to convert header: {:?} to string: {}",
                hdr_value, e
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct ListNamespaces200Response {
    #[serde(rename = "namespaces")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespaces: Option<Vec<models::Namespace>>,
}

impl ListNamespaces200Response {
    #[allow(clippy::new_without_default)]
    pub fn new() -> ListNamespaces200Response {
        ListNamespaces200Response { namespaces: None }
    }
}

/// Converts the ListNamespaces200Response value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::string::ToString for ListNamespaces200Response {
    fn to_string(&self) -> String {
        let params: Vec<Option<String>> = vec![
            // Skipping namespaces in query parameter serialization

        ];

        params.into_iter().flatten().collect::<Vec<_>>().join(",")
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a ListNamespaces200Response value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for ListNamespaces200Response {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub namespaces: Vec<Vec<models::Namespace>>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing ListNamespaces200Response".to_string(),
                    )
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    "namespaces" => return std::result::Result::Err("Parsing a container in this style is not supported in ListNamespaces200Response".to_string()),
                    _ => return std::result::Result::Err("Unexpected key while parsing ListNamespaces200Response".to_string())
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(ListNamespaces200Response {
            namespaces: intermediate_rep.namespaces.into_iter().next(),
        })
    }
}

// Methods for converting between header::IntoHeaderValue<ListNamespaces200Response> and hyper::header::HeaderValue

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<header::IntoHeaderValue<ListNamespaces200Response>>
    for hyper::header::HeaderValue
{
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<ListNamespaces200Response>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match hyper::header::HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Invalid header value for ListNamespaces200Response - value: {} is invalid {}",
                hdr_value, e
            )),
        }
    }
}

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<hyper::header::HeaderValue>
    for header::IntoHeaderValue<ListNamespaces200Response>
{
    type Error = String;

    fn try_from(hdr_value: hyper::header::HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <ListNamespaces200Response as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        "Unable to convert header value '{}' into ListNamespaces200Response - {}",
                        value, err
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Unable to convert header: {:?} to string: {}",
                hdr_value, e
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct ListTables200Response {
    #[serde(rename = "identifiers")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub identifiers: Option<Vec<models::TableIdentifier>>,
}

impl ListTables200Response {
    #[allow(clippy::new_without_default)]
    pub fn new() -> ListTables200Response {
        ListTables200Response { identifiers: None }
    }
}

/// Converts the ListTables200Response value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::string::ToString for ListTables200Response {
    fn to_string(&self) -> String {
        let params: Vec<Option<String>> = vec![
            // Skipping identifiers in query parameter serialization

        ];

        params.into_iter().flatten().collect::<Vec<_>>().join(",")
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a ListTables200Response value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for ListTables200Response {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub identifiers: Vec<Vec<models::TableIdentifier>>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing ListTables200Response".to_string(),
                    )
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    "identifiers" => return std::result::Result::Err("Parsing a container in this style is not supported in ListTables200Response".to_string()),
                    _ => return std::result::Result::Err("Unexpected key while parsing ListTables200Response".to_string())
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(ListTables200Response {
            identifiers: intermediate_rep.identifiers.into_iter().next(),
        })
    }
}

// Methods for converting between header::IntoHeaderValue<ListTables200Response> and hyper::header::HeaderValue

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<header::IntoHeaderValue<ListTables200Response>>
    for hyper::header::HeaderValue
{
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<ListTables200Response>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match hyper::header::HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Invalid header value for ListTables200Response - value: {} is invalid {}",
                hdr_value, e
            )),
        }
    }
}

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<hyper::header::HeaderValue>
    for header::IntoHeaderValue<ListTables200Response>
{
    type Error = String;

    fn try_from(hdr_value: hyper::header::HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <ListTables200Response as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        "Unable to convert header value '{}' into ListTables200Response - {}",
                        value, err
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Unable to convert header: {:?} to string: {}",
                hdr_value, e
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct ListType {
    // Note: inline enums are not fully supported by openapi-generator
    #[serde(rename = "type")]
    pub r#type: String,

    #[serde(rename = "element-id")]
    pub element_id: isize,

    #[serde(rename = "element")]
    pub element: models::Type,

    #[serde(rename = "element-required")]
    pub element_required: bool,
}

impl ListType {
    #[allow(clippy::new_without_default)]
    pub fn new(
        r#type: String,
        element_id: isize,
        element: models::Type,
        element_required: bool,
    ) -> ListType {
        ListType {
            r#type,
            element_id,
            element,
            element_required,
        }
    }
}

/// Converts the ListType value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::string::ToString for ListType {
    fn to_string(&self) -> String {
        let params: Vec<Option<String>> = vec![
            Some("type".to_string()),
            Some(self.r#type.to_string()),
            Some("element-id".to_string()),
            Some(self.element_id.to_string()),
            // Skipping element in query parameter serialization
            Some("element-required".to_string()),
            Some(self.element_required.to_string()),
        ];

        params.into_iter().flatten().collect::<Vec<_>>().join(",")
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a ListType value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for ListType {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub r#type: Vec<String>,
            pub element_id: Vec<isize>,
            pub element: Vec<models::Type>,
            pub element_required: Vec<bool>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing ListType".to_string(),
                    )
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "type" => intermediate_rep.r#type.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "element-id" => intermediate_rep.element_id.push(
                        <isize as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "element" => intermediate_rep.element.push(
                        <models::Type as std::str::FromStr>::from_str(val)
                            .map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "element-required" => intermediate_rep.element_required.push(
                        <bool as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing ListType".to_string(),
                        )
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(ListType {
            r#type: intermediate_rep
                .r#type
                .into_iter()
                .next()
                .ok_or_else(|| "type missing in ListType".to_string())?,
            element_id: intermediate_rep
                .element_id
                .into_iter()
                .next()
                .ok_or_else(|| "element-id missing in ListType".to_string())?,
            element: intermediate_rep
                .element
                .into_iter()
                .next()
                .ok_or_else(|| "element missing in ListType".to_string())?,
            element_required: intermediate_rep
                .element_required
                .into_iter()
                .next()
                .ok_or_else(|| "element-required missing in ListType".to_string())?,
        })
    }
}

// Methods for converting between header::IntoHeaderValue<ListType> and hyper::header::HeaderValue

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<header::IntoHeaderValue<ListType>> for hyper::header::HeaderValue {
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<ListType>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match hyper::header::HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Invalid header value for ListType - value: {} is invalid {}",
                hdr_value, e
            )),
        }
    }
}

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<hyper::header::HeaderValue> for header::IntoHeaderValue<ListType> {
    type Error = String;

    fn try_from(hdr_value: hyper::header::HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <ListType as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        "Unable to convert header value '{}' into ListType - {}",
                        value, err
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Unable to convert header: {:?} to string: {}",
                hdr_value, e
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct LiteralExpression {
    #[serde(rename = "type")]
    pub r#type: String,

    #[serde(rename = "term")]
    pub term: models::Term,

    #[serde(rename = "value")]
    pub value: serde_json::Value,
}

impl LiteralExpression {
    #[allow(clippy::new_without_default)]
    pub fn new(r#type: String, term: models::Term, value: serde_json::Value) -> LiteralExpression {
        LiteralExpression {
            r#type,
            term,
            value,
        }
    }
}

/// Converts the LiteralExpression value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::string::ToString for LiteralExpression {
    fn to_string(&self) -> String {
        let params: Vec<Option<String>> = vec![
            Some("type".to_string()),
            Some(self.r#type.to_string()),
            // Skipping term in query parameter serialization

            // Skipping value in query parameter serialization
        ];

        params.into_iter().flatten().collect::<Vec<_>>().join(",")
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a LiteralExpression value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for LiteralExpression {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub r#type: Vec<String>,
            pub term: Vec<models::Term>,
            pub value: Vec<serde_json::Value>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing LiteralExpression".to_string(),
                    )
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "type" => intermediate_rep.r#type.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "term" => intermediate_rep.term.push(
                        <models::Term as std::str::FromStr>::from_str(val)
                            .map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "value" => intermediate_rep.value.push(
                        <serde_json::Value as std::str::FromStr>::from_str(val)
                            .map_err(|x| x.to_string())?,
                    ),
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing LiteralExpression".to_string(),
                        )
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(LiteralExpression {
            r#type: intermediate_rep
                .r#type
                .into_iter()
                .next()
                .ok_or_else(|| "type missing in LiteralExpression".to_string())?,
            term: intermediate_rep
                .term
                .into_iter()
                .next()
                .ok_or_else(|| "term missing in LiteralExpression".to_string())?,
            value: intermediate_rep
                .value
                .into_iter()
                .next()
                .ok_or_else(|| "value missing in LiteralExpression".to_string())?,
        })
    }
}

// Methods for converting between header::IntoHeaderValue<LiteralExpression> and hyper::header::HeaderValue

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<header::IntoHeaderValue<LiteralExpression>>
    for hyper::header::HeaderValue
{
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<LiteralExpression>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match hyper::header::HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Invalid header value for LiteralExpression - value: {} is invalid {}",
                hdr_value, e
            )),
        }
    }
}

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<hyper::header::HeaderValue>
    for header::IntoHeaderValue<LiteralExpression>
{
    type Error = String;

    fn try_from(hdr_value: hyper::header::HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <LiteralExpression as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        "Unable to convert header value '{}' into LiteralExpression - {}",
                        value, err
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Unable to convert header: {:?} to string: {}",
                hdr_value, e
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct LoadNamespaceMetadata200Response {
    /// Reference to one or more levels of a namespace
    #[serde(rename = "namespace")]
    pub namespace: Vec<String>,

    /// Properties stored on the namespace, if supported by the server. If the server does not support namespace properties, it should return null for this field. If namespace properties are supported, but none are set, it should return an empty object.
    #[serde(rename = "properties")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub properties: Option<serde_json::Value>,
}

impl LoadNamespaceMetadata200Response {
    #[allow(clippy::new_without_default)]
    pub fn new(namespace: Vec<String>) -> LoadNamespaceMetadata200Response {
        LoadNamespaceMetadata200Response {
            namespace,
            properties: None,
        }
    }
}

/// Converts the LoadNamespaceMetadata200Response value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::string::ToString for LoadNamespaceMetadata200Response {
    fn to_string(&self) -> String {
        let params: Vec<Option<String>> = vec![
            Some("namespace".to_string()),
            Some(
                self.namespace
                    .iter()
                    .map(|x| x.to_string())
                    .collect::<Vec<_>>()
                    .join(","),
            ),
            // Skipping properties in query parameter serialization
        ];

        params.into_iter().flatten().collect::<Vec<_>>().join(",")
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a LoadNamespaceMetadata200Response value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for LoadNamespaceMetadata200Response {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub namespace: Vec<Vec<String>>,
            pub properties: Vec<serde_json::Value>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing LoadNamespaceMetadata200Response".to_string(),
                    )
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    "namespace" => return std::result::Result::Err("Parsing a container in this style is not supported in LoadNamespaceMetadata200Response".to_string()),
                    #[allow(clippy::redundant_clone)]
                    "properties" => intermediate_rep.properties.push(<serde_json::Value as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?),
                    _ => return std::result::Result::Err("Unexpected key while parsing LoadNamespaceMetadata200Response".to_string())
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(LoadNamespaceMetadata200Response {
            namespace: intermediate_rep
                .namespace
                .into_iter()
                .next()
                .ok_or_else(|| {
                    "namespace missing in LoadNamespaceMetadata200Response".to_string()
                })?,
            properties: intermediate_rep.properties.into_iter().next(),
        })
    }
}

// Methods for converting between header::IntoHeaderValue<LoadNamespaceMetadata200Response> and hyper::header::HeaderValue

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<header::IntoHeaderValue<LoadNamespaceMetadata200Response>>
    for hyper::header::HeaderValue
{
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<LoadNamespaceMetadata200Response>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match hyper::header::HeaderValue::from_str(&hdr_value) {
             std::result::Result::Ok(value) => std::result::Result::Ok(value),
             std::result::Result::Err(e) => std::result::Result::Err(
                 format!("Invalid header value for LoadNamespaceMetadata200Response - value: {} is invalid {}",
                     hdr_value, e))
        }
    }
}

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<hyper::header::HeaderValue>
    for header::IntoHeaderValue<LoadNamespaceMetadata200Response>
{
    type Error = String;

    fn try_from(hdr_value: hyper::header::HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
             std::result::Result::Ok(value) => {
                    match <LoadNamespaceMetadata200Response as std::str::FromStr>::from_str(value) {
                        std::result::Result::Ok(value) => std::result::Result::Ok(header::IntoHeaderValue(value)),
                        std::result::Result::Err(err) => std::result::Result::Err(
                            format!("Unable to convert header value '{}' into LoadNamespaceMetadata200Response - {}",
                                value, err))
                    }
             },
             std::result::Result::Err(e) => std::result::Result::Err(
                 format!("Unable to convert header: {:?} to string: {}",
                     hdr_value, e))
        }
    }
}

/// Result used when a table is successfully loaded.  The table metadata JSON is returned in the `metadata` field. The corresponding file location of table metadata should be returned in the `metadata-location` field, unless the metadata is not yet committed. For example, a create transaction may return metadata that is staged but not committed. Clients can check whether metadata has changed by comparing metadata locations after the table has been created.  The `config` map returns table-specific configuration for the table's resources, including its HTTP client and FileIO. For example, config may contain a specific FileIO implementation class for the table depending on its underlying storage.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct LoadTableResult {
    /// May be null if the table is staged as part of a transaction
    #[serde(rename = "metadata-location")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata_location: Option<String>,

    #[serde(rename = "metadata")]
    pub metadata: models::TableMetadata,

    #[serde(rename = "config")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub config: Option<std::collections::HashMap<String, String>>,
}

impl LoadTableResult {
    #[allow(clippy::new_without_default)]
    pub fn new(metadata: models::TableMetadata) -> LoadTableResult {
        LoadTableResult {
            metadata_location: None,
            metadata,
            config: None,
        }
    }
}

/// Converts the LoadTableResult value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::string::ToString for LoadTableResult {
    fn to_string(&self) -> String {
        let params: Vec<Option<String>> = vec![
            self.metadata_location.as_ref().map(|metadata_location| {
                vec![
                    "metadata-location".to_string(),
                    metadata_location.to_string(),
                ]
                .join(",")
            }),
            // Skipping metadata in query parameter serialization

            // Skipping config in query parameter serialization
        ];

        params.into_iter().flatten().collect::<Vec<_>>().join(",")
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a LoadTableResult value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for LoadTableResult {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub metadata_location: Vec<String>,
            pub metadata: Vec<models::TableMetadata>,
            pub config: Vec<std::collections::HashMap<String, String>>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing LoadTableResult".to_string(),
                    )
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "metadata-location" => intermediate_rep.metadata_location.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "metadata" => intermediate_rep.metadata.push(
                        <models::TableMetadata as std::str::FromStr>::from_str(val)
                            .map_err(|x| x.to_string())?,
                    ),
                    "config" => {
                        return std::result::Result::Err(
                            "Parsing a container in this style is not supported in LoadTableResult"
                                .to_string(),
                        )
                    }
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing LoadTableResult".to_string(),
                        )
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(LoadTableResult {
            metadata_location: intermediate_rep.metadata_location.into_iter().next(),
            metadata: intermediate_rep
                .metadata
                .into_iter()
                .next()
                .ok_or_else(|| "metadata missing in LoadTableResult".to_string())?,
            config: intermediate_rep.config.into_iter().next(),
        })
    }
}

// Methods for converting between header::IntoHeaderValue<LoadTableResult> and hyper::header::HeaderValue

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<header::IntoHeaderValue<LoadTableResult>>
    for hyper::header::HeaderValue
{
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<LoadTableResult>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match hyper::header::HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Invalid header value for LoadTableResult - value: {} is invalid {}",
                hdr_value, e
            )),
        }
    }
}

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<hyper::header::HeaderValue>
    for header::IntoHeaderValue<LoadTableResult>
{
    type Error = String;

    fn try_from(hdr_value: hyper::header::HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <LoadTableResult as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        "Unable to convert header value '{}' into LoadTableResult - {}",
                        value, err
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Unable to convert header: {:?} to string: {}",
                hdr_value, e
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct MapType {
    // Note: inline enums are not fully supported by openapi-generator
    #[serde(rename = "type")]
    pub r#type: String,

    #[serde(rename = "key-id")]
    pub key_id: isize,

    #[serde(rename = "key")]
    pub key: models::Type,

    #[serde(rename = "value-id")]
    pub value_id: isize,

    #[serde(rename = "value")]
    pub value: models::Type,

    #[serde(rename = "value-required")]
    pub value_required: bool,
}

impl MapType {
    #[allow(clippy::new_without_default)]
    pub fn new(
        r#type: String,
        key_id: isize,
        key: models::Type,
        value_id: isize,
        value: models::Type,
        value_required: bool,
    ) -> MapType {
        MapType {
            r#type,
            key_id,
            key,
            value_id,
            value,
            value_required,
        }
    }
}

/// Converts the MapType value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::string::ToString for MapType {
    fn to_string(&self) -> String {
        let params: Vec<Option<String>> = vec![
            Some("type".to_string()),
            Some(self.r#type.to_string()),
            Some("key-id".to_string()),
            Some(self.key_id.to_string()),
            // Skipping key in query parameter serialization
            Some("value-id".to_string()),
            Some(self.value_id.to_string()),
            // Skipping value in query parameter serialization
            Some("value-required".to_string()),
            Some(self.value_required.to_string()),
        ];

        params.into_iter().flatten().collect::<Vec<_>>().join(",")
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a MapType value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for MapType {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub r#type: Vec<String>,
            pub key_id: Vec<isize>,
            pub key: Vec<models::Type>,
            pub value_id: Vec<isize>,
            pub value: Vec<models::Type>,
            pub value_required: Vec<bool>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing MapType".to_string(),
                    )
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "type" => intermediate_rep.r#type.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "key-id" => intermediate_rep.key_id.push(
                        <isize as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "key" => intermediate_rep.key.push(
                        <models::Type as std::str::FromStr>::from_str(val)
                            .map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "value-id" => intermediate_rep.value_id.push(
                        <isize as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "value" => intermediate_rep.value.push(
                        <models::Type as std::str::FromStr>::from_str(val)
                            .map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "value-required" => intermediate_rep.value_required.push(
                        <bool as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing MapType".to_string(),
                        )
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(MapType {
            r#type: intermediate_rep
                .r#type
                .into_iter()
                .next()
                .ok_or_else(|| "type missing in MapType".to_string())?,
            key_id: intermediate_rep
                .key_id
                .into_iter()
                .next()
                .ok_or_else(|| "key-id missing in MapType".to_string())?,
            key: intermediate_rep
                .key
                .into_iter()
                .next()
                .ok_or_else(|| "key missing in MapType".to_string())?,
            value_id: intermediate_rep
                .value_id
                .into_iter()
                .next()
                .ok_or_else(|| "value-id missing in MapType".to_string())?,
            value: intermediate_rep
                .value
                .into_iter()
                .next()
                .ok_or_else(|| "value missing in MapType".to_string())?,
            value_required: intermediate_rep
                .value_required
                .into_iter()
                .next()
                .ok_or_else(|| "value-required missing in MapType".to_string())?,
        })
    }
}

// Methods for converting between header::IntoHeaderValue<MapType> and hyper::header::HeaderValue

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<header::IntoHeaderValue<MapType>> for hyper::header::HeaderValue {
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<MapType>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match hyper::header::HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Invalid header value for MapType - value: {} is invalid {}",
                hdr_value, e
            )),
        }
    }
}

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<hyper::header::HeaderValue> for header::IntoHeaderValue<MapType> {
    type Error = String;

    fn try_from(hdr_value: hyper::header::HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <MapType as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        "Unable to convert header value '{}' into MapType - {}",
                        value, err
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Unable to convert header: {:?} to string: {}",
                hdr_value, e
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct MetadataLogInner {
    #[serde(rename = "metadata-file")]
    pub metadata_file: String,

    #[serde(rename = "timestamp-ms")]
    pub timestamp_ms: isize,
}

impl MetadataLogInner {
    #[allow(clippy::new_without_default)]
    pub fn new(metadata_file: String, timestamp_ms: isize) -> MetadataLogInner {
        MetadataLogInner {
            metadata_file,
            timestamp_ms,
        }
    }
}

/// Converts the MetadataLogInner value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::string::ToString for MetadataLogInner {
    fn to_string(&self) -> String {
        let params: Vec<Option<String>> = vec![
            Some("metadata-file".to_string()),
            Some(self.metadata_file.to_string()),
            Some("timestamp-ms".to_string()),
            Some(self.timestamp_ms.to_string()),
        ];

        params.into_iter().flatten().collect::<Vec<_>>().join(",")
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a MetadataLogInner value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for MetadataLogInner {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub metadata_file: Vec<String>,
            pub timestamp_ms: Vec<isize>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing MetadataLogInner".to_string(),
                    )
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "metadata-file" => intermediate_rep.metadata_file.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "timestamp-ms" => intermediate_rep.timestamp_ms.push(
                        <isize as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing MetadataLogInner".to_string(),
                        )
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(MetadataLogInner {
            metadata_file: intermediate_rep
                .metadata_file
                .into_iter()
                .next()
                .ok_or_else(|| "metadata-file missing in MetadataLogInner".to_string())?,
            timestamp_ms: intermediate_rep
                .timestamp_ms
                .into_iter()
                .next()
                .ok_or_else(|| "timestamp-ms missing in MetadataLogInner".to_string())?,
        })
    }
}

// Methods for converting between header::IntoHeaderValue<MetadataLogInner> and hyper::header::HeaderValue

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<header::IntoHeaderValue<MetadataLogInner>>
    for hyper::header::HeaderValue
{
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<MetadataLogInner>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match hyper::header::HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Invalid header value for MetadataLogInner - value: {} is invalid {}",
                hdr_value, e
            )),
        }
    }
}

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<hyper::header::HeaderValue>
    for header::IntoHeaderValue<MetadataLogInner>
{
    type Error = String;

    fn try_from(hdr_value: hyper::header::HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <MetadataLogInner as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        "Unable to convert header value '{}' into MetadataLogInner - {}",
                        value, err
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Unable to convert header: {:?} to string: {}",
                hdr_value, e
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct MetricResult {
    #[serde(rename = "unit")]
    pub unit: String,

    #[serde(rename = "value")]
    pub value: i64,

    #[serde(rename = "time-unit")]
    pub time_unit: String,

    #[serde(rename = "count")]
    pub count: i64,

    #[serde(rename = "total-duration")]
    pub total_duration: i64,
}

impl MetricResult {
    #[allow(clippy::new_without_default)]
    pub fn new(
        unit: String,
        value: i64,
        time_unit: String,
        count: i64,
        total_duration: i64,
    ) -> MetricResult {
        MetricResult {
            unit,
            value,
            time_unit,
            count,
            total_duration,
        }
    }
}

/// Converts the MetricResult value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::string::ToString for MetricResult {
    fn to_string(&self) -> String {
        let params: Vec<Option<String>> = vec![
            Some("unit".to_string()),
            Some(self.unit.to_string()),
            Some("value".to_string()),
            Some(self.value.to_string()),
            Some("time-unit".to_string()),
            Some(self.time_unit.to_string()),
            Some("count".to_string()),
            Some(self.count.to_string()),
            Some("total-duration".to_string()),
            Some(self.total_duration.to_string()),
        ];

        params.into_iter().flatten().collect::<Vec<_>>().join(",")
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a MetricResult value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for MetricResult {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub unit: Vec<String>,
            pub value: Vec<i64>,
            pub time_unit: Vec<String>,
            pub count: Vec<i64>,
            pub total_duration: Vec<i64>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing MetricResult".to_string(),
                    )
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "unit" => intermediate_rep.unit.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "value" => intermediate_rep.value.push(
                        <i64 as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "time-unit" => intermediate_rep.time_unit.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "count" => intermediate_rep.count.push(
                        <i64 as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "total-duration" => intermediate_rep.total_duration.push(
                        <i64 as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing MetricResult".to_string(),
                        )
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(MetricResult {
            unit: intermediate_rep
                .unit
                .into_iter()
                .next()
                .ok_or_else(|| "unit missing in MetricResult".to_string())?,
            value: intermediate_rep
                .value
                .into_iter()
                .next()
                .ok_or_else(|| "value missing in MetricResult".to_string())?,
            time_unit: intermediate_rep
                .time_unit
                .into_iter()
                .next()
                .ok_or_else(|| "time-unit missing in MetricResult".to_string())?,
            count: intermediate_rep
                .count
                .into_iter()
                .next()
                .ok_or_else(|| "count missing in MetricResult".to_string())?,
            total_duration: intermediate_rep
                .total_duration
                .into_iter()
                .next()
                .ok_or_else(|| "total-duration missing in MetricResult".to_string())?,
        })
    }
}

// Methods for converting between header::IntoHeaderValue<MetricResult> and hyper::header::HeaderValue

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<header::IntoHeaderValue<MetricResult>> for hyper::header::HeaderValue {
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<MetricResult>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match hyper::header::HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Invalid header value for MetricResult - value: {} is invalid {}",
                hdr_value, e
            )),
        }
    }
}

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<hyper::header::HeaderValue> for header::IntoHeaderValue<MetricResult> {
    type Error = String;

    fn try_from(hdr_value: hyper::header::HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <MetricResult as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        "Unable to convert header value '{}' into MetricResult - {}",
                        value, err
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Unable to convert header: {:?} to string: {}",
                hdr_value, e
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct NotExpression {
    #[serde(rename = "type")]
    pub r#type: String,

    #[serde(rename = "child")]
    pub child: models::Expression,
}

impl NotExpression {
    #[allow(clippy::new_without_default)]
    pub fn new(r#type: String, child: models::Expression) -> NotExpression {
        NotExpression { r#type, child }
    }
}

/// Converts the NotExpression value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::string::ToString for NotExpression {
    fn to_string(&self) -> String {
        let params: Vec<Option<String>> = vec![
            Some("type".to_string()),
            Some(self.r#type.to_string()),
            // Skipping child in query parameter serialization
        ];

        params.into_iter().flatten().collect::<Vec<_>>().join(",")
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a NotExpression value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for NotExpression {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub r#type: Vec<String>,
            pub child: Vec<models::Expression>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing NotExpression".to_string(),
                    )
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "type" => intermediate_rep.r#type.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "child" => intermediate_rep.child.push(
                        <models::Expression as std::str::FromStr>::from_str(val)
                            .map_err(|x| x.to_string())?,
                    ),
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing NotExpression".to_string(),
                        )
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(NotExpression {
            r#type: intermediate_rep
                .r#type
                .into_iter()
                .next()
                .ok_or_else(|| "type missing in NotExpression".to_string())?,
            child: intermediate_rep
                .child
                .into_iter()
                .next()
                .ok_or_else(|| "child missing in NotExpression".to_string())?,
        })
    }
}

// Methods for converting between header::IntoHeaderValue<NotExpression> and hyper::header::HeaderValue

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<header::IntoHeaderValue<NotExpression>> for hyper::header::HeaderValue {
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<NotExpression>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match hyper::header::HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Invalid header value for NotExpression - value: {} is invalid {}",
                hdr_value, e
            )),
        }
    }
}

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<hyper::header::HeaderValue> for header::IntoHeaderValue<NotExpression> {
    type Error = String;

    fn try_from(hdr_value: hyper::header::HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <NotExpression as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        "Unable to convert header value '{}' into NotExpression - {}",
                        value, err
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Unable to convert header: {:?} to string: {}",
                hdr_value, e
            )),
        }
    }
}

/// Enumeration of values.
/// Since this enum's variants do not hold data, we can easily define them them as `#[repr(C)]`
/// which helps with FFI.
#[allow(non_camel_case_types)]
#[repr(C)]
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
#[cfg_attr(feature = "conversion", derive(frunk_enum_derive::LabelledGenericEnum))]
pub enum NullOrder {
    #[serde(rename = "nulls-first")]
    First,
    #[serde(rename = "nulls-last")]
    Last,
}

impl std::fmt::Display for NullOrder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            NullOrder::First => write!(f, "nulls-first"),
            NullOrder::Last => write!(f, "nulls-last"),
        }
    }
}

impl std::str::FromStr for NullOrder {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "nulls-first" => std::result::Result::Ok(NullOrder::First),
            "nulls-last" => std::result::Result::Ok(NullOrder::Last),
            _ => std::result::Result::Err(format!("Value not valid: {}", s)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct PartitionField {
    #[serde(rename = "field-id")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field_id: Option<isize>,

    #[serde(rename = "source-id")]
    pub source_id: isize,

    #[serde(rename = "name")]
    pub name: String,

    #[serde(rename = "transform")]
    pub transform: String,
}

impl PartitionField {
    #[allow(clippy::new_without_default)]
    pub fn new(source_id: isize, name: String, transform: String) -> PartitionField {
        PartitionField {
            field_id: None,
            source_id,
            name,
            transform,
        }
    }
}

/// Converts the PartitionField value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::string::ToString for PartitionField {
    fn to_string(&self) -> String {
        let params: Vec<Option<String>> = vec![
            self.field_id
                .as_ref()
                .map(|field_id| vec!["field-id".to_string(), field_id.to_string()].join(",")),
            Some("source-id".to_string()),
            Some(self.source_id.to_string()),
            Some("name".to_string()),
            Some(self.name.to_string()),
            Some("transform".to_string()),
            Some(self.transform.to_string()),
        ];

        params.into_iter().flatten().collect::<Vec<_>>().join(",")
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a PartitionField value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for PartitionField {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub field_id: Vec<isize>,
            pub source_id: Vec<isize>,
            pub name: Vec<String>,
            pub transform: Vec<String>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing PartitionField".to_string(),
                    )
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "field-id" => intermediate_rep.field_id.push(
                        <isize as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "source-id" => intermediate_rep.source_id.push(
                        <isize as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "name" => intermediate_rep.name.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "transform" => intermediate_rep.transform.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing PartitionField".to_string(),
                        )
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(PartitionField {
            field_id: intermediate_rep.field_id.into_iter().next(),
            source_id: intermediate_rep
                .source_id
                .into_iter()
                .next()
                .ok_or_else(|| "source-id missing in PartitionField".to_string())?,
            name: intermediate_rep
                .name
                .into_iter()
                .next()
                .ok_or_else(|| "name missing in PartitionField".to_string())?,
            transform: intermediate_rep
                .transform
                .into_iter()
                .next()
                .ok_or_else(|| "transform missing in PartitionField".to_string())?,
        })
    }
}

// Methods for converting between header::IntoHeaderValue<PartitionField> and hyper::header::HeaderValue

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<header::IntoHeaderValue<PartitionField>> for hyper::header::HeaderValue {
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<PartitionField>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match hyper::header::HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Invalid header value for PartitionField - value: {} is invalid {}",
                hdr_value, e
            )),
        }
    }
}

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<hyper::header::HeaderValue> for header::IntoHeaderValue<PartitionField> {
    type Error = String;

    fn try_from(hdr_value: hyper::header::HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <PartitionField as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        "Unable to convert header value '{}' into PartitionField - {}",
                        value, err
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Unable to convert header: {:?} to string: {}",
                hdr_value, e
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct PartitionSpec {
    #[serde(rename = "spec-id")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub spec_id: Option<isize>,

    #[serde(rename = "fields")]
    pub fields: Vec<models::PartitionField>,
}

impl PartitionSpec {
    #[allow(clippy::new_without_default)]
    pub fn new(fields: Vec<models::PartitionField>) -> PartitionSpec {
        PartitionSpec {
            spec_id: None,
            fields,
        }
    }
}

/// Converts the PartitionSpec value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::string::ToString for PartitionSpec {
    fn to_string(&self) -> String {
        let params: Vec<Option<String>> = vec![
            self.spec_id
                .as_ref()
                .map(|spec_id| vec!["spec-id".to_string(), spec_id.to_string()].join(",")),
            // Skipping fields in query parameter serialization
        ];

        params.into_iter().flatten().collect::<Vec<_>>().join(",")
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a PartitionSpec value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for PartitionSpec {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub spec_id: Vec<isize>,
            pub fields: Vec<Vec<models::PartitionField>>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing PartitionSpec".to_string(),
                    )
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "spec-id" => intermediate_rep.spec_id.push(
                        <isize as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    "fields" => {
                        return std::result::Result::Err(
                            "Parsing a container in this style is not supported in PartitionSpec"
                                .to_string(),
                        )
                    }
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing PartitionSpec".to_string(),
                        )
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(PartitionSpec {
            spec_id: intermediate_rep.spec_id.into_iter().next(),
            fields: intermediate_rep
                .fields
                .into_iter()
                .next()
                .ok_or_else(|| "fields missing in PartitionSpec".to_string())?,
        })
    }
}

// Methods for converting between header::IntoHeaderValue<PartitionSpec> and hyper::header::HeaderValue

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<header::IntoHeaderValue<PartitionSpec>> for hyper::header::HeaderValue {
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<PartitionSpec>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match hyper::header::HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Invalid header value for PartitionSpec - value: {} is invalid {}",
                hdr_value, e
            )),
        }
    }
}

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<hyper::header::HeaderValue> for header::IntoHeaderValue<PartitionSpec> {
    type Error = String;

    fn try_from(hdr_value: hyper::header::HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <PartitionSpec as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        "Unable to convert header value '{}' into PartitionSpec - {}",
                        value, err
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Unable to convert header: {:?} to string: {}",
                hdr_value, e
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct PrimitiveType(String);

impl std::convert::From<String> for PrimitiveType {
    fn from(x: String) -> Self {
        PrimitiveType(x)
    }
}

impl std::string::ToString for PrimitiveType {
    fn to_string(&self) -> String {
        self.0.to_string()
    }
}

impl std::str::FromStr for PrimitiveType {
    type Err = std::string::ParseError;
    fn from_str(x: &str) -> std::result::Result<Self, Self::Err> {
        std::result::Result::Ok(PrimitiveType(x.to_string()))
    }
}

impl std::convert::From<PrimitiveType> for String {
    fn from(x: PrimitiveType) -> Self {
        x.0
    }
}

impl std::ops::Deref for PrimitiveType {
    type Target = String;
    fn deref(&self) -> &String {
        &self.0
    }
}

impl std::ops::DerefMut for PrimitiveType {
    fn deref_mut(&mut self) -> &mut String {
        &mut self.0
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct Reference(String);

impl std::convert::From<String> for Reference {
    fn from(x: String) -> Self {
        Reference(x)
    }
}

impl std::string::ToString for Reference {
    fn to_string(&self) -> String {
        self.0.to_string()
    }
}

impl std::str::FromStr for Reference {
    type Err = std::string::ParseError;
    fn from_str(x: &str) -> std::result::Result<Self, Self::Err> {
        std::result::Result::Ok(Reference(x.to_string()))
    }
}

impl std::convert::From<Reference> for String {
    fn from(x: Reference) -> Self {
        x.0
    }
}

impl std::ops::Deref for Reference {
    type Target = String;
    fn deref(&self) -> &String {
        &self.0
    }
}

impl std::ops::DerefMut for Reference {
    fn deref_mut(&mut self) -> &mut String {
        &mut self.0
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct RemovePropertiesUpdate {
    // Note: inline enums are not fully supported by openapi-generator
    #[serde(rename = "action")]
    pub action: String,

    #[serde(rename = "removals")]
    pub removals: Vec<String>,
}

impl RemovePropertiesUpdate {
    #[allow(clippy::new_without_default)]
    pub fn new(action: String, removals: Vec<String>) -> RemovePropertiesUpdate {
        RemovePropertiesUpdate { action, removals }
    }
}

/// Converts the RemovePropertiesUpdate value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::string::ToString for RemovePropertiesUpdate {
    fn to_string(&self) -> String {
        let params: Vec<Option<String>> = vec![
            Some("action".to_string()),
            Some(self.action.to_string()),
            Some("removals".to_string()),
            Some(
                self.removals
                    .iter()
                    .map(|x| x.to_string())
                    .collect::<Vec<_>>()
                    .join(","),
            ),
        ];

        params.into_iter().flatten().collect::<Vec<_>>().join(",")
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a RemovePropertiesUpdate value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for RemovePropertiesUpdate {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub action: Vec<String>,
            pub removals: Vec<Vec<String>>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing RemovePropertiesUpdate".to_string(),
                    )
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "action" => intermediate_rep.action.push(<String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?),
                    "removals" => return std::result::Result::Err("Parsing a container in this style is not supported in RemovePropertiesUpdate".to_string()),
                    _ => return std::result::Result::Err("Unexpected key while parsing RemovePropertiesUpdate".to_string())
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(RemovePropertiesUpdate {
            action: intermediate_rep
                .action
                .into_iter()
                .next()
                .ok_or_else(|| "action missing in RemovePropertiesUpdate".to_string())?,
            removals: intermediate_rep
                .removals
                .into_iter()
                .next()
                .ok_or_else(|| "removals missing in RemovePropertiesUpdate".to_string())?,
        })
    }
}

// Methods for converting between header::IntoHeaderValue<RemovePropertiesUpdate> and hyper::header::HeaderValue

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<header::IntoHeaderValue<RemovePropertiesUpdate>>
    for hyper::header::HeaderValue
{
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<RemovePropertiesUpdate>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match hyper::header::HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Invalid header value for RemovePropertiesUpdate - value: {} is invalid {}",
                hdr_value, e
            )),
        }
    }
}

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<hyper::header::HeaderValue>
    for header::IntoHeaderValue<RemovePropertiesUpdate>
{
    type Error = String;

    fn try_from(hdr_value: hyper::header::HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <RemovePropertiesUpdate as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        "Unable to convert header value '{}' into RemovePropertiesUpdate - {}",
                        value, err
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Unable to convert header: {:?} to string: {}",
                hdr_value, e
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct RemovePropertiesUpdateAllOf {
    #[serde(rename = "removals")]
    pub removals: Vec<String>,
}

impl RemovePropertiesUpdateAllOf {
    #[allow(clippy::new_without_default)]
    pub fn new(removals: Vec<String>) -> RemovePropertiesUpdateAllOf {
        RemovePropertiesUpdateAllOf { removals }
    }
}

/// Converts the RemovePropertiesUpdateAllOf value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::string::ToString for RemovePropertiesUpdateAllOf {
    fn to_string(&self) -> String {
        let params: Vec<Option<String>> = vec![
            Some("removals".to_string()),
            Some(
                self.removals
                    .iter()
                    .map(|x| x.to_string())
                    .collect::<Vec<_>>()
                    .join(","),
            ),
        ];

        params.into_iter().flatten().collect::<Vec<_>>().join(",")
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a RemovePropertiesUpdateAllOf value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for RemovePropertiesUpdateAllOf {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub removals: Vec<Vec<String>>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing RemovePropertiesUpdateAllOf".to_string(),
                    )
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    "removals" => return std::result::Result::Err("Parsing a container in this style is not supported in RemovePropertiesUpdateAllOf".to_string()),
                    _ => return std::result::Result::Err("Unexpected key while parsing RemovePropertiesUpdateAllOf".to_string())
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(RemovePropertiesUpdateAllOf {
            removals: intermediate_rep
                .removals
                .into_iter()
                .next()
                .ok_or_else(|| "removals missing in RemovePropertiesUpdateAllOf".to_string())?,
        })
    }
}

// Methods for converting between header::IntoHeaderValue<RemovePropertiesUpdateAllOf> and hyper::header::HeaderValue

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<header::IntoHeaderValue<RemovePropertiesUpdateAllOf>>
    for hyper::header::HeaderValue
{
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<RemovePropertiesUpdateAllOf>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match hyper::header::HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Invalid header value for RemovePropertiesUpdateAllOf - value: {} is invalid {}",
                hdr_value, e
            )),
        }
    }
}

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<hyper::header::HeaderValue>
    for header::IntoHeaderValue<RemovePropertiesUpdateAllOf>
{
    type Error = String;

    fn try_from(hdr_value: hyper::header::HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <RemovePropertiesUpdateAllOf as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        "Unable to convert header value '{}' into RemovePropertiesUpdateAllOf - {}",
                        value, err
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Unable to convert header: {:?} to string: {}",
                hdr_value, e
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct RemoveSnapshotRefUpdate {
    // Note: inline enums are not fully supported by openapi-generator
    #[serde(rename = "action")]
    pub action: String,

    #[serde(rename = "ref-name")]
    pub ref_name: String,
}

impl RemoveSnapshotRefUpdate {
    #[allow(clippy::new_without_default)]
    pub fn new(action: String, ref_name: String) -> RemoveSnapshotRefUpdate {
        RemoveSnapshotRefUpdate { action, ref_name }
    }
}

/// Converts the RemoveSnapshotRefUpdate value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::string::ToString for RemoveSnapshotRefUpdate {
    fn to_string(&self) -> String {
        let params: Vec<Option<String>> = vec![
            Some("action".to_string()),
            Some(self.action.to_string()),
            Some("ref-name".to_string()),
            Some(self.ref_name.to_string()),
        ];

        params.into_iter().flatten().collect::<Vec<_>>().join(",")
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a RemoveSnapshotRefUpdate value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for RemoveSnapshotRefUpdate {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub action: Vec<String>,
            pub ref_name: Vec<String>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing RemoveSnapshotRefUpdate".to_string(),
                    )
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "action" => intermediate_rep.action.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "ref-name" => intermediate_rep.ref_name.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing RemoveSnapshotRefUpdate".to_string(),
                        )
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(RemoveSnapshotRefUpdate {
            action: intermediate_rep
                .action
                .into_iter()
                .next()
                .ok_or_else(|| "action missing in RemoveSnapshotRefUpdate".to_string())?,
            ref_name: intermediate_rep
                .ref_name
                .into_iter()
                .next()
                .ok_or_else(|| "ref-name missing in RemoveSnapshotRefUpdate".to_string())?,
        })
    }
}

// Methods for converting between header::IntoHeaderValue<RemoveSnapshotRefUpdate> and hyper::header::HeaderValue

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<header::IntoHeaderValue<RemoveSnapshotRefUpdate>>
    for hyper::header::HeaderValue
{
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<RemoveSnapshotRefUpdate>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match hyper::header::HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Invalid header value for RemoveSnapshotRefUpdate - value: {} is invalid {}",
                hdr_value, e
            )),
        }
    }
}

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<hyper::header::HeaderValue>
    for header::IntoHeaderValue<RemoveSnapshotRefUpdate>
{
    type Error = String;

    fn try_from(hdr_value: hyper::header::HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <RemoveSnapshotRefUpdate as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        "Unable to convert header value '{}' into RemoveSnapshotRefUpdate - {}",
                        value, err
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Unable to convert header: {:?} to string: {}",
                hdr_value, e
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct RemoveSnapshotsUpdate {
    // Note: inline enums are not fully supported by openapi-generator
    #[serde(rename = "action")]
    pub action: String,

    #[serde(rename = "snapshot-ids")]
    pub snapshot_ids: Vec<i64>,
}

impl RemoveSnapshotsUpdate {
    #[allow(clippy::new_without_default)]
    pub fn new(action: String, snapshot_ids: Vec<i64>) -> RemoveSnapshotsUpdate {
        RemoveSnapshotsUpdate {
            action,
            snapshot_ids,
        }
    }
}

/// Converts the RemoveSnapshotsUpdate value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::string::ToString for RemoveSnapshotsUpdate {
    fn to_string(&self) -> String {
        let params: Vec<Option<String>> = vec![
            Some("action".to_string()),
            Some(self.action.to_string()),
            Some("snapshot-ids".to_string()),
            Some(
                self.snapshot_ids
                    .iter()
                    .map(|x| x.to_string())
                    .collect::<Vec<_>>()
                    .join(","),
            ),
        ];

        params.into_iter().flatten().collect::<Vec<_>>().join(",")
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a RemoveSnapshotsUpdate value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for RemoveSnapshotsUpdate {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub action: Vec<String>,
            pub snapshot_ids: Vec<Vec<i64>>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing RemoveSnapshotsUpdate".to_string(),
                    )
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "action" => intermediate_rep.action.push(<String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?),
                    "snapshot-ids" => return std::result::Result::Err("Parsing a container in this style is not supported in RemoveSnapshotsUpdate".to_string()),
                    _ => return std::result::Result::Err("Unexpected key while parsing RemoveSnapshotsUpdate".to_string())
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(RemoveSnapshotsUpdate {
            action: intermediate_rep
                .action
                .into_iter()
                .next()
                .ok_or_else(|| "action missing in RemoveSnapshotsUpdate".to_string())?,
            snapshot_ids: intermediate_rep
                .snapshot_ids
                .into_iter()
                .next()
                .ok_or_else(|| "snapshot-ids missing in RemoveSnapshotsUpdate".to_string())?,
        })
    }
}

// Methods for converting between header::IntoHeaderValue<RemoveSnapshotsUpdate> and hyper::header::HeaderValue

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<header::IntoHeaderValue<RemoveSnapshotsUpdate>>
    for hyper::header::HeaderValue
{
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<RemoveSnapshotsUpdate>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match hyper::header::HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Invalid header value for RemoveSnapshotsUpdate - value: {} is invalid {}",
                hdr_value, e
            )),
        }
    }
}

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<hyper::header::HeaderValue>
    for header::IntoHeaderValue<RemoveSnapshotsUpdate>
{
    type Error = String;

    fn try_from(hdr_value: hyper::header::HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <RemoveSnapshotsUpdate as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        "Unable to convert header value '{}' into RemoveSnapshotsUpdate - {}",
                        value, err
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Unable to convert header: {:?} to string: {}",
                hdr_value, e
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct RemoveSnapshotsUpdateAllOf {
    #[serde(rename = "snapshot-ids")]
    pub snapshot_ids: Vec<i64>,
}

impl RemoveSnapshotsUpdateAllOf {
    #[allow(clippy::new_without_default)]
    pub fn new(snapshot_ids: Vec<i64>) -> RemoveSnapshotsUpdateAllOf {
        RemoveSnapshotsUpdateAllOf { snapshot_ids }
    }
}

/// Converts the RemoveSnapshotsUpdateAllOf value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::string::ToString for RemoveSnapshotsUpdateAllOf {
    fn to_string(&self) -> String {
        let params: Vec<Option<String>> = vec![
            Some("snapshot-ids".to_string()),
            Some(
                self.snapshot_ids
                    .iter()
                    .map(|x| x.to_string())
                    .collect::<Vec<_>>()
                    .join(","),
            ),
        ];

        params.into_iter().flatten().collect::<Vec<_>>().join(",")
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a RemoveSnapshotsUpdateAllOf value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for RemoveSnapshotsUpdateAllOf {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub snapshot_ids: Vec<Vec<i64>>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing RemoveSnapshotsUpdateAllOf".to_string(),
                    )
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    "snapshot-ids" => return std::result::Result::Err("Parsing a container in this style is not supported in RemoveSnapshotsUpdateAllOf".to_string()),
                    _ => return std::result::Result::Err("Unexpected key while parsing RemoveSnapshotsUpdateAllOf".to_string())
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(RemoveSnapshotsUpdateAllOf {
            snapshot_ids: intermediate_rep
                .snapshot_ids
                .into_iter()
                .next()
                .ok_or_else(|| "snapshot-ids missing in RemoveSnapshotsUpdateAllOf".to_string())?,
        })
    }
}

// Methods for converting between header::IntoHeaderValue<RemoveSnapshotsUpdateAllOf> and hyper::header::HeaderValue

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<header::IntoHeaderValue<RemoveSnapshotsUpdateAllOf>>
    for hyper::header::HeaderValue
{
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<RemoveSnapshotsUpdateAllOf>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match hyper::header::HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Invalid header value for RemoveSnapshotsUpdateAllOf - value: {} is invalid {}",
                hdr_value, e
            )),
        }
    }
}

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<hyper::header::HeaderValue>
    for header::IntoHeaderValue<RemoveSnapshotsUpdateAllOf>
{
    type Error = String;

    fn try_from(hdr_value: hyper::header::HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <RemoveSnapshotsUpdateAllOf as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        "Unable to convert header value '{}' into RemoveSnapshotsUpdateAllOf - {}",
                        value, err
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Unable to convert header: {:?} to string: {}",
                hdr_value, e
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct RenameTableRequest {
    #[serde(rename = "source")]
    pub source: models::TableIdentifier,

    #[serde(rename = "destination")]
    pub destination: models::TableIdentifier,
}

impl RenameTableRequest {
    #[allow(clippy::new_without_default)]
    pub fn new(
        source: models::TableIdentifier,
        destination: models::TableIdentifier,
    ) -> RenameTableRequest {
        RenameTableRequest {
            source,
            destination,
        }
    }
}

/// Converts the RenameTableRequest value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::string::ToString for RenameTableRequest {
    fn to_string(&self) -> String {
        let params: Vec<Option<String>> = vec![
            // Skipping source in query parameter serialization

            // Skipping destination in query parameter serialization

        ];

        params.into_iter().flatten().collect::<Vec<_>>().join(",")
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a RenameTableRequest value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for RenameTableRequest {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub source: Vec<models::TableIdentifier>,
            pub destination: Vec<models::TableIdentifier>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing RenameTableRequest".to_string(),
                    )
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "source" => intermediate_rep.source.push(
                        <models::TableIdentifier as std::str::FromStr>::from_str(val)
                            .map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "destination" => intermediate_rep.destination.push(
                        <models::TableIdentifier as std::str::FromStr>::from_str(val)
                            .map_err(|x| x.to_string())?,
                    ),
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing RenameTableRequest".to_string(),
                        )
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(RenameTableRequest {
            source: intermediate_rep
                .source
                .into_iter()
                .next()
                .ok_or_else(|| "source missing in RenameTableRequest".to_string())?,
            destination: intermediate_rep
                .destination
                .into_iter()
                .next()
                .ok_or_else(|| "destination missing in RenameTableRequest".to_string())?,
        })
    }
}

// Methods for converting between header::IntoHeaderValue<RenameTableRequest> and hyper::header::HeaderValue

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<header::IntoHeaderValue<RenameTableRequest>>
    for hyper::header::HeaderValue
{
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<RenameTableRequest>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match hyper::header::HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Invalid header value for RenameTableRequest - value: {} is invalid {}",
                hdr_value, e
            )),
        }
    }
}

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<hyper::header::HeaderValue>
    for header::IntoHeaderValue<RenameTableRequest>
{
    type Error = String;

    fn try_from(hdr_value: hyper::header::HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <RenameTableRequest as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        "Unable to convert header value '{}' into RenameTableRequest - {}",
                        value, err
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Unable to convert header: {:?} to string: {}",
                hdr_value, e
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct ReportMetricsRequest {
    #[serde(rename = "report-type")]
    pub report_type: String,

    #[serde(rename = "table-name")]
    pub table_name: String,

    #[serde(rename = "snapshot-id")]
    pub snapshot_id: i64,

    #[serde(rename = "filter")]
    pub filter: models::Expression,

    #[serde(rename = "projection")]
    pub projection: models::Schema,

    #[serde(rename = "metrics")]
    pub metrics: std::collections::HashMap<String, models::MetricResult>,
}

impl ReportMetricsRequest {
    #[allow(clippy::new_without_default)]
    pub fn new(
        report_type: String,
        table_name: String,
        snapshot_id: i64,
        filter: models::Expression,
        projection: models::Schema,
        metrics: std::collections::HashMap<String, models::MetricResult>,
    ) -> ReportMetricsRequest {
        ReportMetricsRequest {
            report_type,
            table_name,
            snapshot_id,
            filter,
            projection,
            metrics,
        }
    }
}

/// Converts the ReportMetricsRequest value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::string::ToString for ReportMetricsRequest {
    fn to_string(&self) -> String {
        let params: Vec<Option<String>> = vec![
            Some("report-type".to_string()),
            Some(self.report_type.to_string()),
            Some("table-name".to_string()),
            Some(self.table_name.to_string()),
            Some("snapshot-id".to_string()),
            Some(self.snapshot_id.to_string()),
            // Skipping filter in query parameter serialization

            // Skipping projection in query parameter serialization

            // Skipping metrics in query parameter serialization
            // Skipping metrics in query parameter serialization
        ];

        params.into_iter().flatten().collect::<Vec<_>>().join(",")
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a ReportMetricsRequest value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for ReportMetricsRequest {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub report_type: Vec<String>,
            pub table_name: Vec<String>,
            pub snapshot_id: Vec<i64>,
            pub filter: Vec<models::Expression>,
            pub projection: Vec<models::Schema>,
            pub metrics: Vec<std::collections::HashMap<String, models::MetricResult>>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing ReportMetricsRequest".to_string(),
                    )
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "report-type" => intermediate_rep.report_type.push(<String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?),
                    #[allow(clippy::redundant_clone)]
                    "table-name" => intermediate_rep.table_name.push(<String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?),
                    #[allow(clippy::redundant_clone)]
                    "snapshot-id" => intermediate_rep.snapshot_id.push(<i64 as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?),
                    #[allow(clippy::redundant_clone)]
                    "filter" => intermediate_rep.filter.push(<models::Expression as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?),
                    #[allow(clippy::redundant_clone)]
                    "projection" => intermediate_rep.projection.push(<models::Schema as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?),
                    "metrics" => return std::result::Result::Err("Parsing a container in this style is not supported in ReportMetricsRequest".to_string()),
                    _ => return std::result::Result::Err("Unexpected key while parsing ReportMetricsRequest".to_string())
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(ReportMetricsRequest {
            report_type: intermediate_rep
                .report_type
                .into_iter()
                .next()
                .ok_or_else(|| "report-type missing in ReportMetricsRequest".to_string())?,
            table_name: intermediate_rep
                .table_name
                .into_iter()
                .next()
                .ok_or_else(|| "table-name missing in ReportMetricsRequest".to_string())?,
            snapshot_id: intermediate_rep
                .snapshot_id
                .into_iter()
                .next()
                .ok_or_else(|| "snapshot-id missing in ReportMetricsRequest".to_string())?,
            filter: intermediate_rep
                .filter
                .into_iter()
                .next()
                .ok_or_else(|| "filter missing in ReportMetricsRequest".to_string())?,
            projection: intermediate_rep
                .projection
                .into_iter()
                .next()
                .ok_or_else(|| "projection missing in ReportMetricsRequest".to_string())?,
            metrics: intermediate_rep
                .metrics
                .into_iter()
                .next()
                .ok_or_else(|| "metrics missing in ReportMetricsRequest".to_string())?,
        })
    }
}

// Methods for converting between header::IntoHeaderValue<ReportMetricsRequest> and hyper::header::HeaderValue

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<header::IntoHeaderValue<ReportMetricsRequest>>
    for hyper::header::HeaderValue
{
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<ReportMetricsRequest>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match hyper::header::HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Invalid header value for ReportMetricsRequest - value: {} is invalid {}",
                hdr_value, e
            )),
        }
    }
}

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<hyper::header::HeaderValue>
    for header::IntoHeaderValue<ReportMetricsRequest>
{
    type Error = String;

    fn try_from(hdr_value: hyper::header::HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <ReportMetricsRequest as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        "Unable to convert header value '{}' into ReportMetricsRequest - {}",
                        value, err
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Unable to convert header: {:?} to string: {}",
                hdr_value, e
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct ScanReport {
    #[serde(rename = "table-name")]
    pub table_name: String,

    #[serde(rename = "snapshot-id")]
    pub snapshot_id: i64,

    #[serde(rename = "filter")]
    pub filter: models::Expression,

    #[serde(rename = "projection")]
    pub projection: models::Schema,

    #[serde(rename = "metrics")]
    pub metrics: std::collections::HashMap<String, models::MetricResult>,
}

impl ScanReport {
    #[allow(clippy::new_without_default)]
    pub fn new(
        table_name: String,
        snapshot_id: i64,
        filter: models::Expression,
        projection: models::Schema,
        metrics: std::collections::HashMap<String, models::MetricResult>,
    ) -> ScanReport {
        ScanReport {
            table_name,
            snapshot_id,
            filter,
            projection,
            metrics,
        }
    }
}

/// Converts the ScanReport value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::string::ToString for ScanReport {
    fn to_string(&self) -> String {
        let params: Vec<Option<String>> = vec![
            Some("table-name".to_string()),
            Some(self.table_name.to_string()),
            Some("snapshot-id".to_string()),
            Some(self.snapshot_id.to_string()),
            // Skipping filter in query parameter serialization

            // Skipping projection in query parameter serialization

            // Skipping metrics in query parameter serialization
            // Skipping metrics in query parameter serialization
        ];

        params.into_iter().flatten().collect::<Vec<_>>().join(",")
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a ScanReport value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for ScanReport {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub table_name: Vec<String>,
            pub snapshot_id: Vec<i64>,
            pub filter: Vec<models::Expression>,
            pub projection: Vec<models::Schema>,
            pub metrics: Vec<std::collections::HashMap<String, models::MetricResult>>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing ScanReport".to_string(),
                    )
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "table-name" => intermediate_rep.table_name.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "snapshot-id" => intermediate_rep.snapshot_id.push(
                        <i64 as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "filter" => intermediate_rep.filter.push(
                        <models::Expression as std::str::FromStr>::from_str(val)
                            .map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "projection" => intermediate_rep.projection.push(
                        <models::Schema as std::str::FromStr>::from_str(val)
                            .map_err(|x| x.to_string())?,
                    ),
                    "metrics" => {
                        return std::result::Result::Err(
                            "Parsing a container in this style is not supported in ScanReport"
                                .to_string(),
                        )
                    }
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing ScanReport".to_string(),
                        )
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(ScanReport {
            table_name: intermediate_rep
                .table_name
                .into_iter()
                .next()
                .ok_or_else(|| "table-name missing in ScanReport".to_string())?,
            snapshot_id: intermediate_rep
                .snapshot_id
                .into_iter()
                .next()
                .ok_or_else(|| "snapshot-id missing in ScanReport".to_string())?,
            filter: intermediate_rep
                .filter
                .into_iter()
                .next()
                .ok_or_else(|| "filter missing in ScanReport".to_string())?,
            projection: intermediate_rep
                .projection
                .into_iter()
                .next()
                .ok_or_else(|| "projection missing in ScanReport".to_string())?,
            metrics: intermediate_rep
                .metrics
                .into_iter()
                .next()
                .ok_or_else(|| "metrics missing in ScanReport".to_string())?,
        })
    }
}

// Methods for converting between header::IntoHeaderValue<ScanReport> and hyper::header::HeaderValue

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<header::IntoHeaderValue<ScanReport>> for hyper::header::HeaderValue {
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<ScanReport>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match hyper::header::HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Invalid header value for ScanReport - value: {} is invalid {}",
                hdr_value, e
            )),
        }
    }
}

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<hyper::header::HeaderValue> for header::IntoHeaderValue<ScanReport> {
    type Error = String;

    fn try_from(hdr_value: hyper::header::HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <ScanReport as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        "Unable to convert header value '{}' into ScanReport - {}",
                        value, err
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Unable to convert header: {:?} to string: {}",
                hdr_value, e
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct Schema {
    // Note: inline enums are not fully supported by openapi-generator
    #[serde(rename = "type")]
    pub r#type: String,

    #[serde(rename = "fields")]
    pub fields: Vec<models::StructField>,

    #[serde(rename = "schema-id")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_id: Option<isize>,

    #[serde(rename = "identifier-field-ids")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub identifier_field_ids: Option<Vec<i32>>,
}

impl Schema {
    #[allow(clippy::new_without_default)]
    pub fn new(r#type: String, fields: Vec<models::StructField>) -> Schema {
        Schema {
            r#type,
            fields,
            schema_id: None,
            identifier_field_ids: None,
        }
    }
}

/// Converts the Schema value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::string::ToString for Schema {
    fn to_string(&self) -> String {
        let params: Vec<Option<String>> = vec![
            Some("type".to_string()),
            Some(self.r#type.to_string()),
            // Skipping fields in query parameter serialization
            self.schema_id
                .as_ref()
                .map(|schema_id| vec!["schema-id".to_string(), schema_id.to_string()].join(",")),
            self.identifier_field_ids
                .as_ref()
                .map(|identifier_field_ids| {
                    vec![
                        "identifier-field-ids".to_string(),
                        identifier_field_ids
                            .iter()
                            .map(|x| x.to_string())
                            .collect::<Vec<_>>()
                            .join(","),
                    ]
                    .join(",")
                }),
        ];

        params.into_iter().flatten().collect::<Vec<_>>().join(",")
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a Schema value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for Schema {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub r#type: Vec<String>,
            pub fields: Vec<Vec<models::StructField>>,
            pub schema_id: Vec<isize>,
            pub identifier_field_ids: Vec<Vec<i32>>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing Schema".to_string(),
                    )
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "type" => intermediate_rep.r#type.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    "fields" => {
                        return std::result::Result::Err(
                            "Parsing a container in this style is not supported in Schema"
                                .to_string(),
                        )
                    }
                    #[allow(clippy::redundant_clone)]
                    "schema-id" => intermediate_rep.schema_id.push(
                        <isize as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    "identifier-field-ids" => {
                        return std::result::Result::Err(
                            "Parsing a container in this style is not supported in Schema"
                                .to_string(),
                        )
                    }
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing Schema".to_string(),
                        )
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(Schema {
            r#type: intermediate_rep
                .r#type
                .into_iter()
                .next()
                .ok_or_else(|| "type missing in Schema".to_string())?,
            fields: intermediate_rep
                .fields
                .into_iter()
                .next()
                .ok_or_else(|| "fields missing in Schema".to_string())?,
            schema_id: intermediate_rep.schema_id.into_iter().next(),
            identifier_field_ids: intermediate_rep.identifier_field_ids.into_iter().next(),
        })
    }
}

// Methods for converting between header::IntoHeaderValue<Schema> and hyper::header::HeaderValue

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<header::IntoHeaderValue<Schema>> for hyper::header::HeaderValue {
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<Schema>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match hyper::header::HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Invalid header value for Schema - value: {} is invalid {}",
                hdr_value, e
            )),
        }
    }
}

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<hyper::header::HeaderValue> for header::IntoHeaderValue<Schema> {
    type Error = String;

    fn try_from(hdr_value: hyper::header::HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <Schema as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        "Unable to convert header value '{}' into Schema - {}",
                        value, err
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Unable to convert header: {:?} to string: {}",
                hdr_value, e
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct SchemaAllOf {
    #[serde(rename = "schema-id")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_id: Option<isize>,

    #[serde(rename = "identifier-field-ids")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub identifier_field_ids: Option<Vec<i32>>,
}

impl SchemaAllOf {
    #[allow(clippy::new_without_default)]
    pub fn new() -> SchemaAllOf {
        SchemaAllOf {
            schema_id: None,
            identifier_field_ids: None,
        }
    }
}

/// Converts the SchemaAllOf value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::string::ToString for SchemaAllOf {
    fn to_string(&self) -> String {
        let params: Vec<Option<String>> = vec![
            self.schema_id
                .as_ref()
                .map(|schema_id| vec!["schema-id".to_string(), schema_id.to_string()].join(",")),
            self.identifier_field_ids
                .as_ref()
                .map(|identifier_field_ids| {
                    vec![
                        "identifier-field-ids".to_string(),
                        identifier_field_ids
                            .iter()
                            .map(|x| x.to_string())
                            .collect::<Vec<_>>()
                            .join(","),
                    ]
                    .join(",")
                }),
        ];

        params.into_iter().flatten().collect::<Vec<_>>().join(",")
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a SchemaAllOf value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for SchemaAllOf {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub schema_id: Vec<isize>,
            pub identifier_field_ids: Vec<Vec<i32>>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing SchemaAllOf".to_string(),
                    )
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "schema-id" => intermediate_rep.schema_id.push(
                        <isize as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    "identifier-field-ids" => {
                        return std::result::Result::Err(
                            "Parsing a container in this style is not supported in SchemaAllOf"
                                .to_string(),
                        )
                    }
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing SchemaAllOf".to_string(),
                        )
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(SchemaAllOf {
            schema_id: intermediate_rep.schema_id.into_iter().next(),
            identifier_field_ids: intermediate_rep.identifier_field_ids.into_iter().next(),
        })
    }
}

// Methods for converting between header::IntoHeaderValue<SchemaAllOf> and hyper::header::HeaderValue

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<header::IntoHeaderValue<SchemaAllOf>> for hyper::header::HeaderValue {
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<SchemaAllOf>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match hyper::header::HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Invalid header value for SchemaAllOf - value: {} is invalid {}",
                hdr_value, e
            )),
        }
    }
}

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<hyper::header::HeaderValue> for header::IntoHeaderValue<SchemaAllOf> {
    type Error = String;

    fn try_from(hdr_value: hyper::header::HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <SchemaAllOf as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        "Unable to convert header value '{}' into SchemaAllOf - {}",
                        value, err
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Unable to convert header: {:?} to string: {}",
                hdr_value, e
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct SetCurrentSchemaUpdate {
    // Note: inline enums are not fully supported by openapi-generator
    #[serde(rename = "action")]
    pub action: String,

    /// Schema ID to set as current, or -1 to set last added schema
    #[serde(rename = "schema-id")]
    pub schema_id: isize,
}

impl SetCurrentSchemaUpdate {
    #[allow(clippy::new_without_default)]
    pub fn new(action: String, schema_id: isize) -> SetCurrentSchemaUpdate {
        SetCurrentSchemaUpdate { action, schema_id }
    }
}

/// Converts the SetCurrentSchemaUpdate value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::string::ToString for SetCurrentSchemaUpdate {
    fn to_string(&self) -> String {
        let params: Vec<Option<String>> = vec![
            Some("action".to_string()),
            Some(self.action.to_string()),
            Some("schema-id".to_string()),
            Some(self.schema_id.to_string()),
        ];

        params.into_iter().flatten().collect::<Vec<_>>().join(",")
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a SetCurrentSchemaUpdate value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for SetCurrentSchemaUpdate {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub action: Vec<String>,
            pub schema_id: Vec<isize>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing SetCurrentSchemaUpdate".to_string(),
                    )
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "action" => intermediate_rep.action.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "schema-id" => intermediate_rep.schema_id.push(
                        <isize as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing SetCurrentSchemaUpdate".to_string(),
                        )
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(SetCurrentSchemaUpdate {
            action: intermediate_rep
                .action
                .into_iter()
                .next()
                .ok_or_else(|| "action missing in SetCurrentSchemaUpdate".to_string())?,
            schema_id: intermediate_rep
                .schema_id
                .into_iter()
                .next()
                .ok_or_else(|| "schema-id missing in SetCurrentSchemaUpdate".to_string())?,
        })
    }
}

// Methods for converting between header::IntoHeaderValue<SetCurrentSchemaUpdate> and hyper::header::HeaderValue

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<header::IntoHeaderValue<SetCurrentSchemaUpdate>>
    for hyper::header::HeaderValue
{
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<SetCurrentSchemaUpdate>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match hyper::header::HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Invalid header value for SetCurrentSchemaUpdate - value: {} is invalid {}",
                hdr_value, e
            )),
        }
    }
}

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<hyper::header::HeaderValue>
    for header::IntoHeaderValue<SetCurrentSchemaUpdate>
{
    type Error = String;

    fn try_from(hdr_value: hyper::header::HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <SetCurrentSchemaUpdate as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        "Unable to convert header value '{}' into SetCurrentSchemaUpdate - {}",
                        value, err
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Unable to convert header: {:?} to string: {}",
                hdr_value, e
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct SetCurrentSchemaUpdateAllOf {
    /// Schema ID to set as current, or -1 to set last added schema
    #[serde(rename = "schema-id")]
    pub schema_id: isize,
}

impl SetCurrentSchemaUpdateAllOf {
    #[allow(clippy::new_without_default)]
    pub fn new(schema_id: isize) -> SetCurrentSchemaUpdateAllOf {
        SetCurrentSchemaUpdateAllOf { schema_id }
    }
}

/// Converts the SetCurrentSchemaUpdateAllOf value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::string::ToString for SetCurrentSchemaUpdateAllOf {
    fn to_string(&self) -> String {
        let params: Vec<Option<String>> = vec![
            Some("schema-id".to_string()),
            Some(self.schema_id.to_string()),
        ];

        params.into_iter().flatten().collect::<Vec<_>>().join(",")
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a SetCurrentSchemaUpdateAllOf value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for SetCurrentSchemaUpdateAllOf {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub schema_id: Vec<isize>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing SetCurrentSchemaUpdateAllOf".to_string(),
                    )
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "schema-id" => intermediate_rep.schema_id.push(
                        <isize as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing SetCurrentSchemaUpdateAllOf".to_string(),
                        )
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(SetCurrentSchemaUpdateAllOf {
            schema_id: intermediate_rep
                .schema_id
                .into_iter()
                .next()
                .ok_or_else(|| "schema-id missing in SetCurrentSchemaUpdateAllOf".to_string())?,
        })
    }
}

// Methods for converting between header::IntoHeaderValue<SetCurrentSchemaUpdateAllOf> and hyper::header::HeaderValue

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<header::IntoHeaderValue<SetCurrentSchemaUpdateAllOf>>
    for hyper::header::HeaderValue
{
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<SetCurrentSchemaUpdateAllOf>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match hyper::header::HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Invalid header value for SetCurrentSchemaUpdateAllOf - value: {} is invalid {}",
                hdr_value, e
            )),
        }
    }
}

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<hyper::header::HeaderValue>
    for header::IntoHeaderValue<SetCurrentSchemaUpdateAllOf>
{
    type Error = String;

    fn try_from(hdr_value: hyper::header::HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <SetCurrentSchemaUpdateAllOf as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        "Unable to convert header value '{}' into SetCurrentSchemaUpdateAllOf - {}",
                        value, err
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Unable to convert header: {:?} to string: {}",
                hdr_value, e
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct SetDefaultSortOrderUpdate {
    // Note: inline enums are not fully supported by openapi-generator
    #[serde(rename = "action")]
    pub action: String,

    /// Sort order ID to set as the default, or -1 to set last added sort order
    #[serde(rename = "sort-order-id")]
    pub sort_order_id: isize,
}

impl SetDefaultSortOrderUpdate {
    #[allow(clippy::new_without_default)]
    pub fn new(action: String, sort_order_id: isize) -> SetDefaultSortOrderUpdate {
        SetDefaultSortOrderUpdate {
            action,
            sort_order_id,
        }
    }
}

/// Converts the SetDefaultSortOrderUpdate value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::string::ToString for SetDefaultSortOrderUpdate {
    fn to_string(&self) -> String {
        let params: Vec<Option<String>> = vec![
            Some("action".to_string()),
            Some(self.action.to_string()),
            Some("sort-order-id".to_string()),
            Some(self.sort_order_id.to_string()),
        ];

        params.into_iter().flatten().collect::<Vec<_>>().join(",")
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a SetDefaultSortOrderUpdate value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for SetDefaultSortOrderUpdate {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub action: Vec<String>,
            pub sort_order_id: Vec<isize>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing SetDefaultSortOrderUpdate".to_string(),
                    )
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "action" => intermediate_rep.action.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "sort-order-id" => intermediate_rep.sort_order_id.push(
                        <isize as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing SetDefaultSortOrderUpdate".to_string(),
                        )
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(SetDefaultSortOrderUpdate {
            action: intermediate_rep
                .action
                .into_iter()
                .next()
                .ok_or_else(|| "action missing in SetDefaultSortOrderUpdate".to_string())?,
            sort_order_id: intermediate_rep
                .sort_order_id
                .into_iter()
                .next()
                .ok_or_else(|| "sort-order-id missing in SetDefaultSortOrderUpdate".to_string())?,
        })
    }
}

// Methods for converting between header::IntoHeaderValue<SetDefaultSortOrderUpdate> and hyper::header::HeaderValue

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<header::IntoHeaderValue<SetDefaultSortOrderUpdate>>
    for hyper::header::HeaderValue
{
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<SetDefaultSortOrderUpdate>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match hyper::header::HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Invalid header value for SetDefaultSortOrderUpdate - value: {} is invalid {}",
                hdr_value, e
            )),
        }
    }
}

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<hyper::header::HeaderValue>
    for header::IntoHeaderValue<SetDefaultSortOrderUpdate>
{
    type Error = String;

    fn try_from(hdr_value: hyper::header::HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <SetDefaultSortOrderUpdate as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        "Unable to convert header value '{}' into SetDefaultSortOrderUpdate - {}",
                        value, err
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Unable to convert header: {:?} to string: {}",
                hdr_value, e
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct SetDefaultSortOrderUpdateAllOf {
    /// Sort order ID to set as the default, or -1 to set last added sort order
    #[serde(rename = "sort-order-id")]
    pub sort_order_id: isize,
}

impl SetDefaultSortOrderUpdateAllOf {
    #[allow(clippy::new_without_default)]
    pub fn new(sort_order_id: isize) -> SetDefaultSortOrderUpdateAllOf {
        SetDefaultSortOrderUpdateAllOf { sort_order_id }
    }
}

/// Converts the SetDefaultSortOrderUpdateAllOf value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::string::ToString for SetDefaultSortOrderUpdateAllOf {
    fn to_string(&self) -> String {
        let params: Vec<Option<String>> = vec![
            Some("sort-order-id".to_string()),
            Some(self.sort_order_id.to_string()),
        ];

        params.into_iter().flatten().collect::<Vec<_>>().join(",")
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a SetDefaultSortOrderUpdateAllOf value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for SetDefaultSortOrderUpdateAllOf {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub sort_order_id: Vec<isize>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing SetDefaultSortOrderUpdateAllOf".to_string(),
                    )
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "sort-order-id" => intermediate_rep.sort_order_id.push(
                        <isize as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing SetDefaultSortOrderUpdateAllOf"
                                .to_string(),
                        )
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(SetDefaultSortOrderUpdateAllOf {
            sort_order_id: intermediate_rep
                .sort_order_id
                .into_iter()
                .next()
                .ok_or_else(|| {
                    "sort-order-id missing in SetDefaultSortOrderUpdateAllOf".to_string()
                })?,
        })
    }
}

// Methods for converting between header::IntoHeaderValue<SetDefaultSortOrderUpdateAllOf> and hyper::header::HeaderValue

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<header::IntoHeaderValue<SetDefaultSortOrderUpdateAllOf>>
    for hyper::header::HeaderValue
{
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<SetDefaultSortOrderUpdateAllOf>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match hyper::header::HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Invalid header value for SetDefaultSortOrderUpdateAllOf - value: {} is invalid {}",
                hdr_value, e
            )),
        }
    }
}

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<hyper::header::HeaderValue>
    for header::IntoHeaderValue<SetDefaultSortOrderUpdateAllOf>
{
    type Error = String;

    fn try_from(hdr_value: hyper::header::HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
             std::result::Result::Ok(value) => {
                    match <SetDefaultSortOrderUpdateAllOf as std::str::FromStr>::from_str(value) {
                        std::result::Result::Ok(value) => std::result::Result::Ok(header::IntoHeaderValue(value)),
                        std::result::Result::Err(err) => std::result::Result::Err(
                            format!("Unable to convert header value '{}' into SetDefaultSortOrderUpdateAllOf - {}",
                                value, err))
                    }
             },
             std::result::Result::Err(e) => std::result::Result::Err(
                 format!("Unable to convert header: {:?} to string: {}",
                     hdr_value, e))
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct SetDefaultSpecUpdate {
    // Note: inline enums are not fully supported by openapi-generator
    #[serde(rename = "action")]
    pub action: String,

    /// Partition spec ID to set as the default, or -1 to set last added spec
    #[serde(rename = "spec-id")]
    pub spec_id: isize,
}

impl SetDefaultSpecUpdate {
    #[allow(clippy::new_without_default)]
    pub fn new(action: String, spec_id: isize) -> SetDefaultSpecUpdate {
        SetDefaultSpecUpdate { action, spec_id }
    }
}

/// Converts the SetDefaultSpecUpdate value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::string::ToString for SetDefaultSpecUpdate {
    fn to_string(&self) -> String {
        let params: Vec<Option<String>> = vec![
            Some("action".to_string()),
            Some(self.action.to_string()),
            Some("spec-id".to_string()),
            Some(self.spec_id.to_string()),
        ];

        params.into_iter().flatten().collect::<Vec<_>>().join(",")
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a SetDefaultSpecUpdate value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for SetDefaultSpecUpdate {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub action: Vec<String>,
            pub spec_id: Vec<isize>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing SetDefaultSpecUpdate".to_string(),
                    )
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "action" => intermediate_rep.action.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "spec-id" => intermediate_rep.spec_id.push(
                        <isize as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing SetDefaultSpecUpdate".to_string(),
                        )
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(SetDefaultSpecUpdate {
            action: intermediate_rep
                .action
                .into_iter()
                .next()
                .ok_or_else(|| "action missing in SetDefaultSpecUpdate".to_string())?,
            spec_id: intermediate_rep
                .spec_id
                .into_iter()
                .next()
                .ok_or_else(|| "spec-id missing in SetDefaultSpecUpdate".to_string())?,
        })
    }
}

// Methods for converting between header::IntoHeaderValue<SetDefaultSpecUpdate> and hyper::header::HeaderValue

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<header::IntoHeaderValue<SetDefaultSpecUpdate>>
    for hyper::header::HeaderValue
{
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<SetDefaultSpecUpdate>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match hyper::header::HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Invalid header value for SetDefaultSpecUpdate - value: {} is invalid {}",
                hdr_value, e
            )),
        }
    }
}

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<hyper::header::HeaderValue>
    for header::IntoHeaderValue<SetDefaultSpecUpdate>
{
    type Error = String;

    fn try_from(hdr_value: hyper::header::HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <SetDefaultSpecUpdate as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        "Unable to convert header value '{}' into SetDefaultSpecUpdate - {}",
                        value, err
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Unable to convert header: {:?} to string: {}",
                hdr_value, e
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct SetDefaultSpecUpdateAllOf {
    /// Partition spec ID to set as the default, or -1 to set last added spec
    #[serde(rename = "spec-id")]
    pub spec_id: isize,
}

impl SetDefaultSpecUpdateAllOf {
    #[allow(clippy::new_without_default)]
    pub fn new(spec_id: isize) -> SetDefaultSpecUpdateAllOf {
        SetDefaultSpecUpdateAllOf { spec_id }
    }
}

/// Converts the SetDefaultSpecUpdateAllOf value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::string::ToString for SetDefaultSpecUpdateAllOf {
    fn to_string(&self) -> String {
        let params: Vec<Option<String>> =
            vec![Some("spec-id".to_string()), Some(self.spec_id.to_string())];

        params.into_iter().flatten().collect::<Vec<_>>().join(",")
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a SetDefaultSpecUpdateAllOf value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for SetDefaultSpecUpdateAllOf {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub spec_id: Vec<isize>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing SetDefaultSpecUpdateAllOf".to_string(),
                    )
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "spec-id" => intermediate_rep.spec_id.push(
                        <isize as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing SetDefaultSpecUpdateAllOf".to_string(),
                        )
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(SetDefaultSpecUpdateAllOf {
            spec_id: intermediate_rep
                .spec_id
                .into_iter()
                .next()
                .ok_or_else(|| "spec-id missing in SetDefaultSpecUpdateAllOf".to_string())?,
        })
    }
}

// Methods for converting between header::IntoHeaderValue<SetDefaultSpecUpdateAllOf> and hyper::header::HeaderValue

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<header::IntoHeaderValue<SetDefaultSpecUpdateAllOf>>
    for hyper::header::HeaderValue
{
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<SetDefaultSpecUpdateAllOf>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match hyper::header::HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Invalid header value for SetDefaultSpecUpdateAllOf - value: {} is invalid {}",
                hdr_value, e
            )),
        }
    }
}

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<hyper::header::HeaderValue>
    for header::IntoHeaderValue<SetDefaultSpecUpdateAllOf>
{
    type Error = String;

    fn try_from(hdr_value: hyper::header::HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <SetDefaultSpecUpdateAllOf as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        "Unable to convert header value '{}' into SetDefaultSpecUpdateAllOf - {}",
                        value, err
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Unable to convert header: {:?} to string: {}",
                hdr_value, e
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct SetExpression {
    #[serde(rename = "type")]
    pub r#type: String,

    #[serde(rename = "term")]
    pub term: models::Term,

    #[serde(rename = "values")]
    pub values: Vec<serde_json::Value>,
}

impl SetExpression {
    #[allow(clippy::new_without_default)]
    pub fn new(
        r#type: String,
        term: models::Term,
        values: Vec<serde_json::Value>,
    ) -> SetExpression {
        SetExpression {
            r#type,
            term,
            values,
        }
    }
}

/// Converts the SetExpression value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::string::ToString for SetExpression {
    fn to_string(&self) -> String {
        let params: Vec<Option<String>> = vec![
            Some("type".to_string()),
            Some(self.r#type.to_string()),
            // Skipping term in query parameter serialization

            // Skipping values in query parameter serialization
        ];

        params.into_iter().flatten().collect::<Vec<_>>().join(",")
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a SetExpression value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for SetExpression {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub r#type: Vec<String>,
            pub term: Vec<models::Term>,
            pub values: Vec<Vec<serde_json::Value>>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing SetExpression".to_string(),
                    )
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "type" => intermediate_rep.r#type.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "term" => intermediate_rep.term.push(
                        <models::Term as std::str::FromStr>::from_str(val)
                            .map_err(|x| x.to_string())?,
                    ),
                    "values" => {
                        return std::result::Result::Err(
                            "Parsing a container in this style is not supported in SetExpression"
                                .to_string(),
                        )
                    }
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing SetExpression".to_string(),
                        )
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(SetExpression {
            r#type: intermediate_rep
                .r#type
                .into_iter()
                .next()
                .ok_or_else(|| "type missing in SetExpression".to_string())?,
            term: intermediate_rep
                .term
                .into_iter()
                .next()
                .ok_or_else(|| "term missing in SetExpression".to_string())?,
            values: intermediate_rep
                .values
                .into_iter()
                .next()
                .ok_or_else(|| "values missing in SetExpression".to_string())?,
        })
    }
}

// Methods for converting between header::IntoHeaderValue<SetExpression> and hyper::header::HeaderValue

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<header::IntoHeaderValue<SetExpression>> for hyper::header::HeaderValue {
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<SetExpression>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match hyper::header::HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Invalid header value for SetExpression - value: {} is invalid {}",
                hdr_value, e
            )),
        }
    }
}

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<hyper::header::HeaderValue> for header::IntoHeaderValue<SetExpression> {
    type Error = String;

    fn try_from(hdr_value: hyper::header::HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <SetExpression as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        "Unable to convert header value '{}' into SetExpression - {}",
                        value, err
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Unable to convert header: {:?} to string: {}",
                hdr_value, e
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct SetLocationUpdate {
    // Note: inline enums are not fully supported by openapi-generator
    #[serde(rename = "action")]
    pub action: String,

    #[serde(rename = "location")]
    pub location: String,
}

impl SetLocationUpdate {
    #[allow(clippy::new_without_default)]
    pub fn new(action: String, location: String) -> SetLocationUpdate {
        SetLocationUpdate { action, location }
    }
}

/// Converts the SetLocationUpdate value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::string::ToString for SetLocationUpdate {
    fn to_string(&self) -> String {
        let params: Vec<Option<String>> = vec![
            Some("action".to_string()),
            Some(self.action.to_string()),
            Some("location".to_string()),
            Some(self.location.to_string()),
        ];

        params.into_iter().flatten().collect::<Vec<_>>().join(",")
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a SetLocationUpdate value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for SetLocationUpdate {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub action: Vec<String>,
            pub location: Vec<String>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing SetLocationUpdate".to_string(),
                    )
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "action" => intermediate_rep.action.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "location" => intermediate_rep.location.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing SetLocationUpdate".to_string(),
                        )
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(SetLocationUpdate {
            action: intermediate_rep
                .action
                .into_iter()
                .next()
                .ok_or_else(|| "action missing in SetLocationUpdate".to_string())?,
            location: intermediate_rep
                .location
                .into_iter()
                .next()
                .ok_or_else(|| "location missing in SetLocationUpdate".to_string())?,
        })
    }
}

// Methods for converting between header::IntoHeaderValue<SetLocationUpdate> and hyper::header::HeaderValue

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<header::IntoHeaderValue<SetLocationUpdate>>
    for hyper::header::HeaderValue
{
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<SetLocationUpdate>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match hyper::header::HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Invalid header value for SetLocationUpdate - value: {} is invalid {}",
                hdr_value, e
            )),
        }
    }
}

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<hyper::header::HeaderValue>
    for header::IntoHeaderValue<SetLocationUpdate>
{
    type Error = String;

    fn try_from(hdr_value: hyper::header::HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <SetLocationUpdate as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        "Unable to convert header value '{}' into SetLocationUpdate - {}",
                        value, err
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Unable to convert header: {:?} to string: {}",
                hdr_value, e
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct SetLocationUpdateAllOf {
    #[serde(rename = "location")]
    pub location: String,
}

impl SetLocationUpdateAllOf {
    #[allow(clippy::new_without_default)]
    pub fn new(location: String) -> SetLocationUpdateAllOf {
        SetLocationUpdateAllOf { location }
    }
}

/// Converts the SetLocationUpdateAllOf value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::string::ToString for SetLocationUpdateAllOf {
    fn to_string(&self) -> String {
        let params: Vec<Option<String>> = vec![
            Some("location".to_string()),
            Some(self.location.to_string()),
        ];

        params.into_iter().flatten().collect::<Vec<_>>().join(",")
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a SetLocationUpdateAllOf value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for SetLocationUpdateAllOf {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub location: Vec<String>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing SetLocationUpdateAllOf".to_string(),
                    )
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "location" => intermediate_rep.location.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing SetLocationUpdateAllOf".to_string(),
                        )
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(SetLocationUpdateAllOf {
            location: intermediate_rep
                .location
                .into_iter()
                .next()
                .ok_or_else(|| "location missing in SetLocationUpdateAllOf".to_string())?,
        })
    }
}

// Methods for converting between header::IntoHeaderValue<SetLocationUpdateAllOf> and hyper::header::HeaderValue

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<header::IntoHeaderValue<SetLocationUpdateAllOf>>
    for hyper::header::HeaderValue
{
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<SetLocationUpdateAllOf>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match hyper::header::HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Invalid header value for SetLocationUpdateAllOf - value: {} is invalid {}",
                hdr_value, e
            )),
        }
    }
}

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<hyper::header::HeaderValue>
    for header::IntoHeaderValue<SetLocationUpdateAllOf>
{
    type Error = String;

    fn try_from(hdr_value: hyper::header::HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <SetLocationUpdateAllOf as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        "Unable to convert header value '{}' into SetLocationUpdateAllOf - {}",
                        value, err
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Unable to convert header: {:?} to string: {}",
                hdr_value, e
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct SetPropertiesUpdate {
    // Note: inline enums are not fully supported by openapi-generator
    #[serde(rename = "action")]
    pub action: String,

    #[serde(rename = "updates")]
    pub updates: std::collections::HashMap<String, String>,
}

impl SetPropertiesUpdate {
    #[allow(clippy::new_without_default)]
    pub fn new(
        action: String,
        updates: std::collections::HashMap<String, String>,
    ) -> SetPropertiesUpdate {
        SetPropertiesUpdate { action, updates }
    }
}

/// Converts the SetPropertiesUpdate value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::string::ToString for SetPropertiesUpdate {
    fn to_string(&self) -> String {
        let params: Vec<Option<String>> = vec![
            Some("action".to_string()),
            Some(self.action.to_string()),
            // Skipping updates in query parameter serialization
        ];

        params.into_iter().flatten().collect::<Vec<_>>().join(",")
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a SetPropertiesUpdate value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for SetPropertiesUpdate {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub action: Vec<String>,
            pub updates: Vec<std::collections::HashMap<String, String>>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing SetPropertiesUpdate".to_string(),
                    )
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "action" => intermediate_rep.action.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    "updates" => return std::result::Result::Err(
                        "Parsing a container in this style is not supported in SetPropertiesUpdate"
                            .to_string(),
                    ),
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing SetPropertiesUpdate".to_string(),
                        )
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(SetPropertiesUpdate {
            action: intermediate_rep
                .action
                .into_iter()
                .next()
                .ok_or_else(|| "action missing in SetPropertiesUpdate".to_string())?,
            updates: intermediate_rep
                .updates
                .into_iter()
                .next()
                .ok_or_else(|| "updates missing in SetPropertiesUpdate".to_string())?,
        })
    }
}

// Methods for converting between header::IntoHeaderValue<SetPropertiesUpdate> and hyper::header::HeaderValue

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<header::IntoHeaderValue<SetPropertiesUpdate>>
    for hyper::header::HeaderValue
{
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<SetPropertiesUpdate>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match hyper::header::HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Invalid header value for SetPropertiesUpdate - value: {} is invalid {}",
                hdr_value, e
            )),
        }
    }
}

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<hyper::header::HeaderValue>
    for header::IntoHeaderValue<SetPropertiesUpdate>
{
    type Error = String;

    fn try_from(hdr_value: hyper::header::HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <SetPropertiesUpdate as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        "Unable to convert header value '{}' into SetPropertiesUpdate - {}",
                        value, err
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Unable to convert header: {:?} to string: {}",
                hdr_value, e
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct SetPropertiesUpdateAllOf {
    #[serde(rename = "updates")]
    pub updates: std::collections::HashMap<String, String>,
}

impl SetPropertiesUpdateAllOf {
    #[allow(clippy::new_without_default)]
    pub fn new(updates: std::collections::HashMap<String, String>) -> SetPropertiesUpdateAllOf {
        SetPropertiesUpdateAllOf { updates }
    }
}

/// Converts the SetPropertiesUpdateAllOf value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::string::ToString for SetPropertiesUpdateAllOf {
    fn to_string(&self) -> String {
        let params: Vec<Option<String>> = vec![
            // Skipping updates in query parameter serialization

        ];

        params.into_iter().flatten().collect::<Vec<_>>().join(",")
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a SetPropertiesUpdateAllOf value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for SetPropertiesUpdateAllOf {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub updates: Vec<std::collections::HashMap<String, String>>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing SetPropertiesUpdateAllOf".to_string(),
                    )
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    "updates" => return std::result::Result::Err("Parsing a container in this style is not supported in SetPropertiesUpdateAllOf".to_string()),
                    _ => return std::result::Result::Err("Unexpected key while parsing SetPropertiesUpdateAllOf".to_string())
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(SetPropertiesUpdateAllOf {
            updates: intermediate_rep
                .updates
                .into_iter()
                .next()
                .ok_or_else(|| "updates missing in SetPropertiesUpdateAllOf".to_string())?,
        })
    }
}

// Methods for converting between header::IntoHeaderValue<SetPropertiesUpdateAllOf> and hyper::header::HeaderValue

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<header::IntoHeaderValue<SetPropertiesUpdateAllOf>>
    for hyper::header::HeaderValue
{
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<SetPropertiesUpdateAllOf>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match hyper::header::HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Invalid header value for SetPropertiesUpdateAllOf - value: {} is invalid {}",
                hdr_value, e
            )),
        }
    }
}

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<hyper::header::HeaderValue>
    for header::IntoHeaderValue<SetPropertiesUpdateAllOf>
{
    type Error = String;

    fn try_from(hdr_value: hyper::header::HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <SetPropertiesUpdateAllOf as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        "Unable to convert header value '{}' into SetPropertiesUpdateAllOf - {}",
                        value, err
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Unable to convert header: {:?} to string: {}",
                hdr_value, e
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct SetSnapshotRefUpdate {
    // Note: inline enums are not fully supported by openapi-generator
    #[serde(rename = "action")]
    pub action: String,

    // Note: inline enums are not fully supported by openapi-generator
    #[serde(rename = "type")]
    pub r#type: String,

    #[serde(rename = "snapshot-id")]
    pub snapshot_id: i64,

    #[serde(rename = "max-ref-age-ms")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_ref_age_ms: Option<i64>,

    #[serde(rename = "max-snapshot-age-ms")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_snapshot_age_ms: Option<i64>,

    #[serde(rename = "min-snapshots-to-keep")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_snapshots_to_keep: Option<isize>,

    #[serde(rename = "ref-name")]
    pub ref_name: String,
}

impl SetSnapshotRefUpdate {
    #[allow(clippy::new_without_default)]
    pub fn new(
        action: String,
        r#type: String,
        snapshot_id: i64,
        ref_name: String,
    ) -> SetSnapshotRefUpdate {
        SetSnapshotRefUpdate {
            action,
            r#type,
            snapshot_id,
            max_ref_age_ms: None,
            max_snapshot_age_ms: None,
            min_snapshots_to_keep: None,
            ref_name,
        }
    }
}

/// Converts the SetSnapshotRefUpdate value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::string::ToString for SetSnapshotRefUpdate {
    fn to_string(&self) -> String {
        let params: Vec<Option<String>> = vec![
            Some("action".to_string()),
            Some(self.action.to_string()),
            Some("type".to_string()),
            Some(self.r#type.to_string()),
            Some("snapshot-id".to_string()),
            Some(self.snapshot_id.to_string()),
            self.max_ref_age_ms.as_ref().map(|max_ref_age_ms| {
                vec!["max-ref-age-ms".to_string(), max_ref_age_ms.to_string()].join(",")
            }),
            self.max_snapshot_age_ms
                .as_ref()
                .map(|max_snapshot_age_ms| {
                    vec![
                        "max-snapshot-age-ms".to_string(),
                        max_snapshot_age_ms.to_string(),
                    ]
                    .join(",")
                }),
            self.min_snapshots_to_keep
                .as_ref()
                .map(|min_snapshots_to_keep| {
                    vec![
                        "min-snapshots-to-keep".to_string(),
                        min_snapshots_to_keep.to_string(),
                    ]
                    .join(",")
                }),
            Some("ref-name".to_string()),
            Some(self.ref_name.to_string()),
        ];

        params.into_iter().flatten().collect::<Vec<_>>().join(",")
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a SetSnapshotRefUpdate value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for SetSnapshotRefUpdate {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub action: Vec<String>,
            pub r#type: Vec<String>,
            pub snapshot_id: Vec<i64>,
            pub max_ref_age_ms: Vec<i64>,
            pub max_snapshot_age_ms: Vec<i64>,
            pub min_snapshots_to_keep: Vec<isize>,
            pub ref_name: Vec<String>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing SetSnapshotRefUpdate".to_string(),
                    )
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "action" => intermediate_rep.action.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "type" => intermediate_rep.r#type.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "snapshot-id" => intermediate_rep.snapshot_id.push(
                        <i64 as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "max-ref-age-ms" => intermediate_rep.max_ref_age_ms.push(
                        <i64 as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "max-snapshot-age-ms" => intermediate_rep.max_snapshot_age_ms.push(
                        <i64 as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "min-snapshots-to-keep" => intermediate_rep.min_snapshots_to_keep.push(
                        <isize as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "ref-name" => intermediate_rep.ref_name.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing SetSnapshotRefUpdate".to_string(),
                        )
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(SetSnapshotRefUpdate {
            action: intermediate_rep
                .action
                .into_iter()
                .next()
                .ok_or_else(|| "action missing in SetSnapshotRefUpdate".to_string())?,
            r#type: intermediate_rep
                .r#type
                .into_iter()
                .next()
                .ok_or_else(|| "type missing in SetSnapshotRefUpdate".to_string())?,
            snapshot_id: intermediate_rep
                .snapshot_id
                .into_iter()
                .next()
                .ok_or_else(|| "snapshot-id missing in SetSnapshotRefUpdate".to_string())?,
            max_ref_age_ms: intermediate_rep.max_ref_age_ms.into_iter().next(),
            max_snapshot_age_ms: intermediate_rep.max_snapshot_age_ms.into_iter().next(),
            min_snapshots_to_keep: intermediate_rep.min_snapshots_to_keep.into_iter().next(),
            ref_name: intermediate_rep
                .ref_name
                .into_iter()
                .next()
                .ok_or_else(|| "ref-name missing in SetSnapshotRefUpdate".to_string())?,
        })
    }
}

// Methods for converting between header::IntoHeaderValue<SetSnapshotRefUpdate> and hyper::header::HeaderValue

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<header::IntoHeaderValue<SetSnapshotRefUpdate>>
    for hyper::header::HeaderValue
{
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<SetSnapshotRefUpdate>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match hyper::header::HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Invalid header value for SetSnapshotRefUpdate - value: {} is invalid {}",
                hdr_value, e
            )),
        }
    }
}

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<hyper::header::HeaderValue>
    for header::IntoHeaderValue<SetSnapshotRefUpdate>
{
    type Error = String;

    fn try_from(hdr_value: hyper::header::HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <SetSnapshotRefUpdate as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        "Unable to convert header value '{}' into SetSnapshotRefUpdate - {}",
                        value, err
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Unable to convert header: {:?} to string: {}",
                hdr_value, e
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct SetSnapshotRefUpdateAllOf {
    #[serde(rename = "ref-name")]
    pub ref_name: String,
}

impl SetSnapshotRefUpdateAllOf {
    #[allow(clippy::new_without_default)]
    pub fn new(ref_name: String) -> SetSnapshotRefUpdateAllOf {
        SetSnapshotRefUpdateAllOf { ref_name }
    }
}

/// Converts the SetSnapshotRefUpdateAllOf value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::string::ToString for SetSnapshotRefUpdateAllOf {
    fn to_string(&self) -> String {
        let params: Vec<Option<String>> = vec![
            Some("ref-name".to_string()),
            Some(self.ref_name.to_string()),
        ];

        params.into_iter().flatten().collect::<Vec<_>>().join(",")
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a SetSnapshotRefUpdateAllOf value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for SetSnapshotRefUpdateAllOf {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub ref_name: Vec<String>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing SetSnapshotRefUpdateAllOf".to_string(),
                    )
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "ref-name" => intermediate_rep.ref_name.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing SetSnapshotRefUpdateAllOf".to_string(),
                        )
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(SetSnapshotRefUpdateAllOf {
            ref_name: intermediate_rep
                .ref_name
                .into_iter()
                .next()
                .ok_or_else(|| "ref-name missing in SetSnapshotRefUpdateAllOf".to_string())?,
        })
    }
}

// Methods for converting between header::IntoHeaderValue<SetSnapshotRefUpdateAllOf> and hyper::header::HeaderValue

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<header::IntoHeaderValue<SetSnapshotRefUpdateAllOf>>
    for hyper::header::HeaderValue
{
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<SetSnapshotRefUpdateAllOf>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match hyper::header::HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Invalid header value for SetSnapshotRefUpdateAllOf - value: {} is invalid {}",
                hdr_value, e
            )),
        }
    }
}

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<hyper::header::HeaderValue>
    for header::IntoHeaderValue<SetSnapshotRefUpdateAllOf>
{
    type Error = String;

    fn try_from(hdr_value: hyper::header::HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <SetSnapshotRefUpdateAllOf as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        "Unable to convert header value '{}' into SetSnapshotRefUpdateAllOf - {}",
                        value, err
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Unable to convert header: {:?} to string: {}",
                hdr_value, e
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct Snapshot {
    #[serde(rename = "snapshot-id")]
    pub snapshot_id: isize,

    #[serde(rename = "parent-snapshot-id")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_snapshot_id: Option<isize>,

    #[serde(rename = "sequence-number")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sequence_number: Option<isize>,

    #[serde(rename = "timestamp-ms")]
    pub timestamp_ms: isize,

    /// Location of the snapshot's manifest list file
    #[serde(rename = "manifest-list")]
    pub manifest_list: String,

    #[serde(rename = "summary")]
    pub summary: models::SnapshotSummary,

    #[serde(rename = "schema-id")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_id: Option<isize>,
}

impl Snapshot {
    #[allow(clippy::new_without_default)]
    pub fn new(
        snapshot_id: isize,
        timestamp_ms: isize,
        manifest_list: String,
        summary: models::SnapshotSummary,
    ) -> Snapshot {
        Snapshot {
            snapshot_id,
            parent_snapshot_id: None,
            sequence_number: None,
            timestamp_ms,
            manifest_list,
            summary,
            schema_id: None,
        }
    }
}

/// Converts the Snapshot value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::string::ToString for Snapshot {
    fn to_string(&self) -> String {
        let params: Vec<Option<String>> = vec![
            Some("snapshot-id".to_string()),
            Some(self.snapshot_id.to_string()),
            self.parent_snapshot_id.as_ref().map(|parent_snapshot_id| {
                vec![
                    "parent-snapshot-id".to_string(),
                    parent_snapshot_id.to_string(),
                ]
                .join(",")
            }),
            self.sequence_number.as_ref().map(|sequence_number| {
                vec!["sequence-number".to_string(), sequence_number.to_string()].join(",")
            }),
            Some("timestamp-ms".to_string()),
            Some(self.timestamp_ms.to_string()),
            Some("manifest-list".to_string()),
            Some(self.manifest_list.to_string()),
            // Skipping summary in query parameter serialization
            self.schema_id
                .as_ref()
                .map(|schema_id| vec!["schema-id".to_string(), schema_id.to_string()].join(",")),
        ];

        params.into_iter().flatten().collect::<Vec<_>>().join(",")
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a Snapshot value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for Snapshot {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub snapshot_id: Vec<isize>,
            pub parent_snapshot_id: Vec<isize>,
            pub sequence_number: Vec<isize>,
            pub timestamp_ms: Vec<isize>,
            pub manifest_list: Vec<String>,
            pub summary: Vec<models::SnapshotSummary>,
            pub schema_id: Vec<isize>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing Snapshot".to_string(),
                    )
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "snapshot-id" => intermediate_rep.snapshot_id.push(
                        <isize as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "parent-snapshot-id" => intermediate_rep.parent_snapshot_id.push(
                        <isize as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "sequence-number" => intermediate_rep.sequence_number.push(
                        <isize as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "timestamp-ms" => intermediate_rep.timestamp_ms.push(
                        <isize as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "manifest-list" => intermediate_rep.manifest_list.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "summary" => intermediate_rep.summary.push(
                        <models::SnapshotSummary as std::str::FromStr>::from_str(val)
                            .map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "schema-id" => intermediate_rep.schema_id.push(
                        <isize as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing Snapshot".to_string(),
                        )
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(Snapshot {
            snapshot_id: intermediate_rep
                .snapshot_id
                .into_iter()
                .next()
                .ok_or_else(|| "snapshot-id missing in Snapshot".to_string())?,
            parent_snapshot_id: intermediate_rep.parent_snapshot_id.into_iter().next(),
            sequence_number: intermediate_rep.sequence_number.into_iter().next(),
            timestamp_ms: intermediate_rep
                .timestamp_ms
                .into_iter()
                .next()
                .ok_or_else(|| "timestamp-ms missing in Snapshot".to_string())?,
            manifest_list: intermediate_rep
                .manifest_list
                .into_iter()
                .next()
                .ok_or_else(|| "manifest-list missing in Snapshot".to_string())?,
            summary: intermediate_rep
                .summary
                .into_iter()
                .next()
                .ok_or_else(|| "summary missing in Snapshot".to_string())?,
            schema_id: intermediate_rep.schema_id.into_iter().next(),
        })
    }
}

// Methods for converting between header::IntoHeaderValue<Snapshot> and hyper::header::HeaderValue

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<header::IntoHeaderValue<Snapshot>> for hyper::header::HeaderValue {
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<Snapshot>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match hyper::header::HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Invalid header value for Snapshot - value: {} is invalid {}",
                hdr_value, e
            )),
        }
    }
}

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<hyper::header::HeaderValue> for header::IntoHeaderValue<Snapshot> {
    type Error = String;

    fn try_from(hdr_value: hyper::header::HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <Snapshot as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        "Unable to convert header value '{}' into Snapshot - {}",
                        value, err
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Unable to convert header: {:?} to string: {}",
                hdr_value, e
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct SnapshotLogInner {
    #[serde(rename = "snapshot-id")]
    pub snapshot_id: isize,

    #[serde(rename = "timestamp-ms")]
    pub timestamp_ms: isize,
}

impl SnapshotLogInner {
    #[allow(clippy::new_without_default)]
    pub fn new(snapshot_id: isize, timestamp_ms: isize) -> SnapshotLogInner {
        SnapshotLogInner {
            snapshot_id,
            timestamp_ms,
        }
    }
}

/// Converts the SnapshotLogInner value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::string::ToString for SnapshotLogInner {
    fn to_string(&self) -> String {
        let params: Vec<Option<String>> = vec![
            Some("snapshot-id".to_string()),
            Some(self.snapshot_id.to_string()),
            Some("timestamp-ms".to_string()),
            Some(self.timestamp_ms.to_string()),
        ];

        params.into_iter().flatten().collect::<Vec<_>>().join(",")
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a SnapshotLogInner value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for SnapshotLogInner {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub snapshot_id: Vec<isize>,
            pub timestamp_ms: Vec<isize>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing SnapshotLogInner".to_string(),
                    )
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "snapshot-id" => intermediate_rep.snapshot_id.push(
                        <isize as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "timestamp-ms" => intermediate_rep.timestamp_ms.push(
                        <isize as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing SnapshotLogInner".to_string(),
                        )
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(SnapshotLogInner {
            snapshot_id: intermediate_rep
                .snapshot_id
                .into_iter()
                .next()
                .ok_or_else(|| "snapshot-id missing in SnapshotLogInner".to_string())?,
            timestamp_ms: intermediate_rep
                .timestamp_ms
                .into_iter()
                .next()
                .ok_or_else(|| "timestamp-ms missing in SnapshotLogInner".to_string())?,
        })
    }
}

// Methods for converting between header::IntoHeaderValue<SnapshotLogInner> and hyper::header::HeaderValue

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<header::IntoHeaderValue<SnapshotLogInner>>
    for hyper::header::HeaderValue
{
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<SnapshotLogInner>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match hyper::header::HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Invalid header value for SnapshotLogInner - value: {} is invalid {}",
                hdr_value, e
            )),
        }
    }
}

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<hyper::header::HeaderValue>
    for header::IntoHeaderValue<SnapshotLogInner>
{
    type Error = String;

    fn try_from(hdr_value: hyper::header::HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <SnapshotLogInner as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        "Unable to convert header value '{}' into SnapshotLogInner - {}",
                        value, err
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Unable to convert header: {:?} to string: {}",
                hdr_value, e
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct SnapshotReference {
    // Note: inline enums are not fully supported by openapi-generator
    #[serde(rename = "type")]
    pub r#type: String,

    #[serde(rename = "snapshot-id")]
    pub snapshot_id: i64,

    #[serde(rename = "max-ref-age-ms")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_ref_age_ms: Option<i64>,

    #[serde(rename = "max-snapshot-age-ms")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_snapshot_age_ms: Option<i64>,

    #[serde(rename = "min-snapshots-to-keep")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_snapshots_to_keep: Option<isize>,
}

impl SnapshotReference {
    #[allow(clippy::new_without_default)]
    pub fn new(r#type: String, snapshot_id: i64) -> SnapshotReference {
        SnapshotReference {
            r#type,
            snapshot_id,
            max_ref_age_ms: None,
            max_snapshot_age_ms: None,
            min_snapshots_to_keep: None,
        }
    }
}

/// Converts the SnapshotReference value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::string::ToString for SnapshotReference {
    fn to_string(&self) -> String {
        let params: Vec<Option<String>> = vec![
            Some("type".to_string()),
            Some(self.r#type.to_string()),
            Some("snapshot-id".to_string()),
            Some(self.snapshot_id.to_string()),
            self.max_ref_age_ms.as_ref().map(|max_ref_age_ms| {
                vec!["max-ref-age-ms".to_string(), max_ref_age_ms.to_string()].join(",")
            }),
            self.max_snapshot_age_ms
                .as_ref()
                .map(|max_snapshot_age_ms| {
                    vec![
                        "max-snapshot-age-ms".to_string(),
                        max_snapshot_age_ms.to_string(),
                    ]
                    .join(",")
                }),
            self.min_snapshots_to_keep
                .as_ref()
                .map(|min_snapshots_to_keep| {
                    vec![
                        "min-snapshots-to-keep".to_string(),
                        min_snapshots_to_keep.to_string(),
                    ]
                    .join(",")
                }),
        ];

        params.into_iter().flatten().collect::<Vec<_>>().join(",")
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a SnapshotReference value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for SnapshotReference {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub r#type: Vec<String>,
            pub snapshot_id: Vec<i64>,
            pub max_ref_age_ms: Vec<i64>,
            pub max_snapshot_age_ms: Vec<i64>,
            pub min_snapshots_to_keep: Vec<isize>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing SnapshotReference".to_string(),
                    )
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "type" => intermediate_rep.r#type.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "snapshot-id" => intermediate_rep.snapshot_id.push(
                        <i64 as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "max-ref-age-ms" => intermediate_rep.max_ref_age_ms.push(
                        <i64 as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "max-snapshot-age-ms" => intermediate_rep.max_snapshot_age_ms.push(
                        <i64 as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "min-snapshots-to-keep" => intermediate_rep.min_snapshots_to_keep.push(
                        <isize as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing SnapshotReference".to_string(),
                        )
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(SnapshotReference {
            r#type: intermediate_rep
                .r#type
                .into_iter()
                .next()
                .ok_or_else(|| "type missing in SnapshotReference".to_string())?,
            snapshot_id: intermediate_rep
                .snapshot_id
                .into_iter()
                .next()
                .ok_or_else(|| "snapshot-id missing in SnapshotReference".to_string())?,
            max_ref_age_ms: intermediate_rep.max_ref_age_ms.into_iter().next(),
            max_snapshot_age_ms: intermediate_rep.max_snapshot_age_ms.into_iter().next(),
            min_snapshots_to_keep: intermediate_rep.min_snapshots_to_keep.into_iter().next(),
        })
    }
}

// Methods for converting between header::IntoHeaderValue<SnapshotReference> and hyper::header::HeaderValue

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<header::IntoHeaderValue<SnapshotReference>>
    for hyper::header::HeaderValue
{
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<SnapshotReference>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match hyper::header::HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Invalid header value for SnapshotReference - value: {} is invalid {}",
                hdr_value, e
            )),
        }
    }
}

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<hyper::header::HeaderValue>
    for header::IntoHeaderValue<SnapshotReference>
{
    type Error = String;

    fn try_from(hdr_value: hyper::header::HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <SnapshotReference as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        "Unable to convert header value '{}' into SnapshotReference - {}",
                        value, err
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Unable to convert header: {:?} to string: {}",
                hdr_value, e
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct SnapshotSummary {
    // Note: inline enums are not fully supported by openapi-generator
    #[serde(rename = "operation")]
    pub operation: String,

    #[serde(rename = "additionalProperties")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub additional_properties: Option<String>,
}

impl SnapshotSummary {
    #[allow(clippy::new_without_default)]
    pub fn new(operation: String) -> SnapshotSummary {
        SnapshotSummary {
            operation,
            additional_properties: None,
        }
    }
}

/// Converts the SnapshotSummary value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::string::ToString for SnapshotSummary {
    fn to_string(&self) -> String {
        let params: Vec<Option<String>> = vec![
            Some("operation".to_string()),
            Some(self.operation.to_string()),
            self.additional_properties
                .as_ref()
                .map(|additional_properties| {
                    vec![
                        "additionalProperties".to_string(),
                        additional_properties.to_string(),
                    ]
                    .join(",")
                }),
        ];

        params.into_iter().flatten().collect::<Vec<_>>().join(",")
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a SnapshotSummary value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for SnapshotSummary {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub operation: Vec<String>,
            pub additional_properties: Vec<String>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing SnapshotSummary".to_string(),
                    )
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "operation" => intermediate_rep.operation.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "additionalProperties" => intermediate_rep.additional_properties.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing SnapshotSummary".to_string(),
                        )
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(SnapshotSummary {
            operation: intermediate_rep
                .operation
                .into_iter()
                .next()
                .ok_or_else(|| "operation missing in SnapshotSummary".to_string())?,
            additional_properties: intermediate_rep.additional_properties.into_iter().next(),
        })
    }
}

// Methods for converting between header::IntoHeaderValue<SnapshotSummary> and hyper::header::HeaderValue

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<header::IntoHeaderValue<SnapshotSummary>>
    for hyper::header::HeaderValue
{
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<SnapshotSummary>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match hyper::header::HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Invalid header value for SnapshotSummary - value: {} is invalid {}",
                hdr_value, e
            )),
        }
    }
}

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<hyper::header::HeaderValue>
    for header::IntoHeaderValue<SnapshotSummary>
{
    type Error = String;

    fn try_from(hdr_value: hyper::header::HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <SnapshotSummary as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        "Unable to convert header value '{}' into SnapshotSummary - {}",
                        value, err
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Unable to convert header: {:?} to string: {}",
                hdr_value, e
            )),
        }
    }
}

/// Enumeration of values.
/// Since this enum's variants do not hold data, we can easily define them them as `#[repr(C)]`
/// which helps with FFI.
#[allow(non_camel_case_types)]
#[repr(C)]
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
#[cfg_attr(feature = "conversion", derive(frunk_enum_derive::LabelledGenericEnum))]
pub enum SortDirection {
    #[serde(rename = "asc")]
    Asc,
    #[serde(rename = "desc")]
    Desc,
}

impl std::fmt::Display for SortDirection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            SortDirection::Asc => write!(f, "asc"),
            SortDirection::Desc => write!(f, "desc"),
        }
    }
}

impl std::str::FromStr for SortDirection {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "asc" => std::result::Result::Ok(SortDirection::Asc),
            "desc" => std::result::Result::Ok(SortDirection::Desc),
            _ => std::result::Result::Err(format!("Value not valid: {}", s)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct SortField {
    #[serde(rename = "source-id")]
    pub source_id: isize,

    #[serde(rename = "transform")]
    pub transform: String,

    #[serde(rename = "direction")]
    pub direction: models::SortDirection,

    #[serde(rename = "null-order")]
    pub null_order: models::NullOrder,
}

impl SortField {
    #[allow(clippy::new_without_default)]
    pub fn new(
        source_id: isize,
        transform: String,
        direction: models::SortDirection,
        null_order: models::NullOrder,
    ) -> SortField {
        SortField {
            source_id,
            transform,
            direction,
            null_order,
        }
    }
}

/// Converts the SortField value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::string::ToString for SortField {
    fn to_string(&self) -> String {
        let params: Vec<Option<String>> = vec![
            Some("source-id".to_string()),
            Some(self.source_id.to_string()),
            Some("transform".to_string()),
            Some(self.transform.to_string()),
            // Skipping direction in query parameter serialization

            // Skipping null-order in query parameter serialization
        ];

        params.into_iter().flatten().collect::<Vec<_>>().join(",")
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a SortField value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for SortField {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub source_id: Vec<isize>,
            pub transform: Vec<String>,
            pub direction: Vec<models::SortDirection>,
            pub null_order: Vec<models::NullOrder>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing SortField".to_string(),
                    )
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "source-id" => intermediate_rep.source_id.push(
                        <isize as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "transform" => intermediate_rep.transform.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "direction" => intermediate_rep.direction.push(
                        <models::SortDirection as std::str::FromStr>::from_str(val)
                            .map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "null-order" => intermediate_rep.null_order.push(
                        <models::NullOrder as std::str::FromStr>::from_str(val)
                            .map_err(|x| x.to_string())?,
                    ),
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing SortField".to_string(),
                        )
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(SortField {
            source_id: intermediate_rep
                .source_id
                .into_iter()
                .next()
                .ok_or_else(|| "source-id missing in SortField".to_string())?,
            transform: intermediate_rep
                .transform
                .into_iter()
                .next()
                .ok_or_else(|| "transform missing in SortField".to_string())?,
            direction: intermediate_rep
                .direction
                .into_iter()
                .next()
                .ok_or_else(|| "direction missing in SortField".to_string())?,
            null_order: intermediate_rep
                .null_order
                .into_iter()
                .next()
                .ok_or_else(|| "null-order missing in SortField".to_string())?,
        })
    }
}

// Methods for converting between header::IntoHeaderValue<SortField> and hyper::header::HeaderValue

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<header::IntoHeaderValue<SortField>> for hyper::header::HeaderValue {
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<SortField>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match hyper::header::HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Invalid header value for SortField - value: {} is invalid {}",
                hdr_value, e
            )),
        }
    }
}

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<hyper::header::HeaderValue> for header::IntoHeaderValue<SortField> {
    type Error = String;

    fn try_from(hdr_value: hyper::header::HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <SortField as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        "Unable to convert header value '{}' into SortField - {}",
                        value, err
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Unable to convert header: {:?} to string: {}",
                hdr_value, e
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct SortOrder {
    #[serde(rename = "order-id")]
    pub order_id: isize,

    #[serde(rename = "fields")]
    pub fields: Vec<models::SortField>,
}

impl SortOrder {
    #[allow(clippy::new_without_default)]
    pub fn new(order_id: isize, fields: Vec<models::SortField>) -> SortOrder {
        SortOrder { order_id, fields }
    }
}

/// Converts the SortOrder value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::string::ToString for SortOrder {
    fn to_string(&self) -> String {
        let params: Vec<Option<String>> = vec![
            Some("order-id".to_string()),
            Some(self.order_id.to_string()),
            // Skipping fields in query parameter serialization
        ];

        params.into_iter().flatten().collect::<Vec<_>>().join(",")
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a SortOrder value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for SortOrder {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub order_id: Vec<isize>,
            pub fields: Vec<Vec<models::SortField>>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing SortOrder".to_string(),
                    )
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "order-id" => intermediate_rep.order_id.push(
                        <isize as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    "fields" => {
                        return std::result::Result::Err(
                            "Parsing a container in this style is not supported in SortOrder"
                                .to_string(),
                        )
                    }
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing SortOrder".to_string(),
                        )
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(SortOrder {
            order_id: intermediate_rep
                .order_id
                .into_iter()
                .next()
                .ok_or_else(|| "order-id missing in SortOrder".to_string())?,
            fields: intermediate_rep
                .fields
                .into_iter()
                .next()
                .ok_or_else(|| "fields missing in SortOrder".to_string())?,
        })
    }
}

// Methods for converting between header::IntoHeaderValue<SortOrder> and hyper::header::HeaderValue

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<header::IntoHeaderValue<SortOrder>> for hyper::header::HeaderValue {
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<SortOrder>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match hyper::header::HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Invalid header value for SortOrder - value: {} is invalid {}",
                hdr_value, e
            )),
        }
    }
}

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<hyper::header::HeaderValue> for header::IntoHeaderValue<SortOrder> {
    type Error = String;

    fn try_from(hdr_value: hyper::header::HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <SortOrder as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        "Unable to convert header value '{}' into SortOrder - {}",
                        value, err
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Unable to convert header: {:?} to string: {}",
                hdr_value, e
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct StructField {
    #[serde(rename = "id")]
    pub id: isize,

    #[serde(rename = "name")]
    pub name: String,

    #[serde(rename = "type")]
    pub r#type: models::Type,

    #[serde(rename = "required")]
    pub required: bool,

    #[serde(rename = "doc")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub doc: Option<String>,
}

impl StructField {
    #[allow(clippy::new_without_default)]
    pub fn new(id: isize, name: String, r#type: models::Type, required: bool) -> StructField {
        StructField {
            id,
            name,
            r#type,
            required,
            doc: None,
        }
    }
}

/// Converts the StructField value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::string::ToString for StructField {
    fn to_string(&self) -> String {
        let params: Vec<Option<String>> = vec![
            Some("id".to_string()),
            Some(self.id.to_string()),
            Some("name".to_string()),
            Some(self.name.to_string()),
            // Skipping type in query parameter serialization
            Some("required".to_string()),
            Some(self.required.to_string()),
            self.doc
                .as_ref()
                .map(|doc| vec!["doc".to_string(), doc.to_string()].join(",")),
        ];

        params.into_iter().flatten().collect::<Vec<_>>().join(",")
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a StructField value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for StructField {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub id: Vec<isize>,
            pub name: Vec<String>,
            pub r#type: Vec<models::Type>,
            pub required: Vec<bool>,
            pub doc: Vec<String>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing StructField".to_string(),
                    )
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "id" => intermediate_rep.id.push(
                        <isize as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "name" => intermediate_rep.name.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "type" => intermediate_rep.r#type.push(
                        <models::Type as std::str::FromStr>::from_str(val)
                            .map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "required" => intermediate_rep.required.push(
                        <bool as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "doc" => intermediate_rep.doc.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing StructField".to_string(),
                        )
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(StructField {
            id: intermediate_rep
                .id
                .into_iter()
                .next()
                .ok_or_else(|| "id missing in StructField".to_string())?,
            name: intermediate_rep
                .name
                .into_iter()
                .next()
                .ok_or_else(|| "name missing in StructField".to_string())?,
            r#type: intermediate_rep
                .r#type
                .into_iter()
                .next()
                .ok_or_else(|| "type missing in StructField".to_string())?,
            required: intermediate_rep
                .required
                .into_iter()
                .next()
                .ok_or_else(|| "required missing in StructField".to_string())?,
            doc: intermediate_rep.doc.into_iter().next(),
        })
    }
}

// Methods for converting between header::IntoHeaderValue<StructField> and hyper::header::HeaderValue

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<header::IntoHeaderValue<StructField>> for hyper::header::HeaderValue {
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<StructField>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match hyper::header::HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Invalid header value for StructField - value: {} is invalid {}",
                hdr_value, e
            )),
        }
    }
}

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<hyper::header::HeaderValue> for header::IntoHeaderValue<StructField> {
    type Error = String;

    fn try_from(hdr_value: hyper::header::HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <StructField as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        "Unable to convert header value '{}' into StructField - {}",
                        value, err
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Unable to convert header: {:?} to string: {}",
                hdr_value, e
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct StructType {
    // Note: inline enums are not fully supported by openapi-generator
    #[serde(rename = "type")]
    pub r#type: String,

    #[serde(rename = "fields")]
    pub fields: Vec<models::StructField>,
}

impl StructType {
    #[allow(clippy::new_without_default)]
    pub fn new(r#type: String, fields: Vec<models::StructField>) -> StructType {
        StructType { r#type, fields }
    }
}

/// Converts the StructType value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::string::ToString for StructType {
    fn to_string(&self) -> String {
        let params: Vec<Option<String>> = vec![
            Some("type".to_string()),
            Some(self.r#type.to_string()),
            // Skipping fields in query parameter serialization
        ];

        params.into_iter().flatten().collect::<Vec<_>>().join(",")
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a StructType value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for StructType {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub r#type: Vec<String>,
            pub fields: Vec<Vec<models::StructField>>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing StructType".to_string(),
                    )
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "type" => intermediate_rep.r#type.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    "fields" => {
                        return std::result::Result::Err(
                            "Parsing a container in this style is not supported in StructType"
                                .to_string(),
                        )
                    }
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing StructType".to_string(),
                        )
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(StructType {
            r#type: intermediate_rep
                .r#type
                .into_iter()
                .next()
                .ok_or_else(|| "type missing in StructType".to_string())?,
            fields: intermediate_rep
                .fields
                .into_iter()
                .next()
                .ok_or_else(|| "fields missing in StructType".to_string())?,
        })
    }
}

// Methods for converting between header::IntoHeaderValue<StructType> and hyper::header::HeaderValue

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<header::IntoHeaderValue<StructType>> for hyper::header::HeaderValue {
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<StructType>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match hyper::header::HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Invalid header value for StructType - value: {} is invalid {}",
                hdr_value, e
            )),
        }
    }
}

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<hyper::header::HeaderValue> for header::IntoHeaderValue<StructType> {
    type Error = String;

    fn try_from(hdr_value: hyper::header::HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <StructType as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        "Unable to convert header value '{}' into StructType - {}",
                        value, err
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Unable to convert header: {:?} to string: {}",
                hdr_value, e
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct TableIdentifier {
    /// Reference to one or more levels of a namespace
    #[serde(rename = "namespace")]
    pub namespace: Vec<String>,

    #[serde(rename = "name")]
    pub name: String,
}

impl TableIdentifier {
    #[allow(clippy::new_without_default)]
    pub fn new(namespace: Vec<String>, name: String) -> TableIdentifier {
        TableIdentifier { namespace, name }
    }
}

/// Converts the TableIdentifier value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::string::ToString for TableIdentifier {
    fn to_string(&self) -> String {
        let params: Vec<Option<String>> = vec![
            Some("namespace".to_string()),
            Some(
                self.namespace
                    .iter()
                    .map(|x| x.to_string())
                    .collect::<Vec<_>>()
                    .join(","),
            ),
            Some("name".to_string()),
            Some(self.name.to_string()),
        ];

        params.into_iter().flatten().collect::<Vec<_>>().join(",")
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a TableIdentifier value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for TableIdentifier {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub namespace: Vec<Vec<String>>,
            pub name: Vec<String>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing TableIdentifier".to_string(),
                    )
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    "namespace" => {
                        return std::result::Result::Err(
                            "Parsing a container in this style is not supported in TableIdentifier"
                                .to_string(),
                        )
                    }
                    #[allow(clippy::redundant_clone)]
                    "name" => intermediate_rep.name.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing TableIdentifier".to_string(),
                        )
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(TableIdentifier {
            namespace: intermediate_rep
                .namespace
                .into_iter()
                .next()
                .ok_or_else(|| "namespace missing in TableIdentifier".to_string())?,
            name: intermediate_rep
                .name
                .into_iter()
                .next()
                .ok_or_else(|| "name missing in TableIdentifier".to_string())?,
        })
    }
}

// Methods for converting between header::IntoHeaderValue<TableIdentifier> and hyper::header::HeaderValue

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<header::IntoHeaderValue<TableIdentifier>>
    for hyper::header::HeaderValue
{
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<TableIdentifier>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match hyper::header::HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Invalid header value for TableIdentifier - value: {} is invalid {}",
                hdr_value, e
            )),
        }
    }
}

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<hyper::header::HeaderValue>
    for header::IntoHeaderValue<TableIdentifier>
{
    type Error = String;

    fn try_from(hdr_value: hyper::header::HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <TableIdentifier as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        "Unable to convert header value '{}' into TableIdentifier - {}",
                        value, err
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Unable to convert header: {:?} to string: {}",
                hdr_value, e
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct TableMetadata {
    #[serde(rename = "format-version")]
    pub format_version: u8,

    #[serde(rename = "table-uuid")]
    pub table_uuid: String,

    #[serde(rename = "location")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub location: Option<String>,

    #[serde(rename = "last-updated-ms")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_updated_ms: Option<isize>,

    #[serde(rename = "properties")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub properties: Option<std::collections::HashMap<String, String>>,

    #[serde(rename = "schemas")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schemas: Option<Vec<models::Schema>>,

    #[serde(rename = "current-schema-id")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_schema_id: Option<isize>,

    #[serde(rename = "last-column-id")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_column_id: Option<isize>,

    #[serde(rename = "partition-specs")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition_specs: Option<Vec<models::PartitionSpec>>,

    #[serde(rename = "default-spec-id")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_spec_id: Option<isize>,

    #[serde(rename = "last-partition-id")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_partition_id: Option<isize>,

    #[serde(rename = "sort-orders")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort_orders: Option<Vec<models::SortOrder>>,

    #[serde(rename = "default-sort-order-id")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_sort_order_id: Option<isize>,

    #[serde(rename = "snapshots")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshots: Option<Vec<models::Snapshot>>,

    #[serde(rename = "refs")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refs: Option<std::collections::HashMap<String, models::SnapshotReference>>,

    #[serde(rename = "current-snapshot-id")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_snapshot_id: Option<isize>,

    #[serde(rename = "snapshot-log")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot_log: Option<Vec<models::SnapshotLogInner>>,

    #[serde(rename = "metadata-log")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata_log: Option<Vec<models::MetadataLogInner>>,
}

impl TableMetadata {
    #[allow(clippy::new_without_default)]
    pub fn new(format_version: u8, table_uuid: String) -> TableMetadata {
        TableMetadata {
            format_version,
            table_uuid,
            location: None,
            last_updated_ms: None,
            properties: None,
            schemas: None,
            current_schema_id: None,
            last_column_id: None,
            partition_specs: None,
            default_spec_id: None,
            last_partition_id: None,
            sort_orders: None,
            default_sort_order_id: None,
            snapshots: None,
            refs: None,
            current_snapshot_id: None,
            snapshot_log: None,
            metadata_log: None,
        }
    }
}

/// Converts the TableMetadata value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::string::ToString for TableMetadata {
    fn to_string(&self) -> String {
        let params: Vec<Option<String>> = vec![
            Some("format-version".to_string()),
            Some(self.format_version.to_string()),
            Some("table-uuid".to_string()),
            Some(self.table_uuid.to_string()),
            self.location
                .as_ref()
                .map(|location| vec!["location".to_string(), location.to_string()].join(",")),
            self.last_updated_ms.as_ref().map(|last_updated_ms| {
                vec!["last-updated-ms".to_string(), last_updated_ms.to_string()].join(",")
            }),
            // Skipping properties in query parameter serialization

            // Skipping schemas in query parameter serialization
            self.current_schema_id.as_ref().map(|current_schema_id| {
                vec![
                    "current-schema-id".to_string(),
                    current_schema_id.to_string(),
                ]
                .join(",")
            }),
            self.last_column_id.as_ref().map(|last_column_id| {
                vec!["last-column-id".to_string(), last_column_id.to_string()].join(",")
            }),
            // Skipping partition-specs in query parameter serialization
            self.default_spec_id.as_ref().map(|default_spec_id| {
                vec!["default-spec-id".to_string(), default_spec_id.to_string()].join(",")
            }),
            self.last_partition_id.as_ref().map(|last_partition_id| {
                vec![
                    "last-partition-id".to_string(),
                    last_partition_id.to_string(),
                ]
                .join(",")
            }),
            // Skipping sort-orders in query parameter serialization
            self.default_sort_order_id
                .as_ref()
                .map(|default_sort_order_id| {
                    vec![
                        "default-sort-order-id".to_string(),
                        default_sort_order_id.to_string(),
                    ]
                    .join(",")
                }),
            // Skipping snapshots in query parameter serialization

            // Skipping refs in query parameter serialization
            // Skipping refs in query parameter serialization
            self.current_snapshot_id
                .as_ref()
                .map(|current_snapshot_id| {
                    vec![
                        "current-snapshot-id".to_string(),
                        current_snapshot_id.to_string(),
                    ]
                    .join(",")
                }),
            // Skipping snapshot-log in query parameter serialization

            // Skipping metadata-log in query parameter serialization
        ];

        params.into_iter().flatten().collect::<Vec<_>>().join(",")
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a TableMetadata value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for TableMetadata {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub format_version: Vec<u8>,
            pub table_uuid: Vec<String>,
            pub location: Vec<String>,
            pub last_updated_ms: Vec<isize>,
            pub properties: Vec<std::collections::HashMap<String, String>>,
            pub schemas: Vec<Vec<models::Schema>>,
            pub current_schema_id: Vec<isize>,
            pub last_column_id: Vec<isize>,
            pub partition_specs: Vec<Vec<models::PartitionSpec>>,
            pub default_spec_id: Vec<isize>,
            pub last_partition_id: Vec<isize>,
            pub sort_orders: Vec<Vec<models::SortOrder>>,
            pub default_sort_order_id: Vec<isize>,
            pub snapshots: Vec<Vec<models::Snapshot>>,
            pub refs: Vec<std::collections::HashMap<String, models::SnapshotReference>>,
            pub current_snapshot_id: Vec<isize>,
            pub snapshot_log: Vec<Vec<models::SnapshotLogInner>>,
            pub metadata_log: Vec<Vec<models::MetadataLogInner>>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing TableMetadata".to_string(),
                    )
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "format-version" => intermediate_rep
                        .format_version
                        .push(<u8 as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?),
                    #[allow(clippy::redundant_clone)]
                    "table-uuid" => intermediate_rep.table_uuid.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "location" => intermediate_rep.location.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "last-updated-ms" => intermediate_rep.last_updated_ms.push(
                        <isize as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    "properties" => {
                        return std::result::Result::Err(
                            "Parsing a container in this style is not supported in TableMetadata"
                                .to_string(),
                        )
                    }
                    "schemas" => {
                        return std::result::Result::Err(
                            "Parsing a container in this style is not supported in TableMetadata"
                                .to_string(),
                        )
                    }
                    #[allow(clippy::redundant_clone)]
                    "current-schema-id" => intermediate_rep.current_schema_id.push(
                        <isize as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "last-column-id" => intermediate_rep.last_column_id.push(
                        <isize as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    "partition-specs" => {
                        return std::result::Result::Err(
                            "Parsing a container in this style is not supported in TableMetadata"
                                .to_string(),
                        )
                    }
                    #[allow(clippy::redundant_clone)]
                    "default-spec-id" => intermediate_rep.default_spec_id.push(
                        <isize as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "last-partition-id" => intermediate_rep.last_partition_id.push(
                        <isize as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    "sort-orders" => {
                        return std::result::Result::Err(
                            "Parsing a container in this style is not supported in TableMetadata"
                                .to_string(),
                        )
                    }
                    #[allow(clippy::redundant_clone)]
                    "default-sort-order-id" => intermediate_rep.default_sort_order_id.push(
                        <isize as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    "snapshots" => {
                        return std::result::Result::Err(
                            "Parsing a container in this style is not supported in TableMetadata"
                                .to_string(),
                        )
                    }
                    "refs" => {
                        return std::result::Result::Err(
                            "Parsing a container in this style is not supported in TableMetadata"
                                .to_string(),
                        )
                    }
                    #[allow(clippy::redundant_clone)]
                    "current-snapshot-id" => intermediate_rep.current_snapshot_id.push(
                        <isize as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    "snapshot-log" => {
                        return std::result::Result::Err(
                            "Parsing a container in this style is not supported in TableMetadata"
                                .to_string(),
                        )
                    }
                    "metadata-log" => {
                        return std::result::Result::Err(
                            "Parsing a container in this style is not supported in TableMetadata"
                                .to_string(),
                        )
                    }
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing TableMetadata".to_string(),
                        )
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(TableMetadata {
            format_version: intermediate_rep
                .format_version
                .into_iter()
                .next()
                .ok_or_else(|| "format-version missing in TableMetadata".to_string())?,
            table_uuid: intermediate_rep
                .table_uuid
                .into_iter()
                .next()
                .ok_or_else(|| "table-uuid missing in TableMetadata".to_string())?,
            location: intermediate_rep.location.into_iter().next(),
            last_updated_ms: intermediate_rep.last_updated_ms.into_iter().next(),
            properties: intermediate_rep.properties.into_iter().next(),
            schemas: intermediate_rep.schemas.into_iter().next(),
            current_schema_id: intermediate_rep.current_schema_id.into_iter().next(),
            last_column_id: intermediate_rep.last_column_id.into_iter().next(),
            partition_specs: intermediate_rep.partition_specs.into_iter().next(),
            default_spec_id: intermediate_rep.default_spec_id.into_iter().next(),
            last_partition_id: intermediate_rep.last_partition_id.into_iter().next(),
            sort_orders: intermediate_rep.sort_orders.into_iter().next(),
            default_sort_order_id: intermediate_rep.default_sort_order_id.into_iter().next(),
            snapshots: intermediate_rep.snapshots.into_iter().next(),
            refs: intermediate_rep.refs.into_iter().next(),
            current_snapshot_id: intermediate_rep.current_snapshot_id.into_iter().next(),
            snapshot_log: intermediate_rep.snapshot_log.into_iter().next(),
            metadata_log: intermediate_rep.metadata_log.into_iter().next(),
        })
    }
}

// Methods for converting between header::IntoHeaderValue<TableMetadata> and hyper::header::HeaderValue

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<header::IntoHeaderValue<TableMetadata>> for hyper::header::HeaderValue {
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<TableMetadata>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match hyper::header::HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Invalid header value for TableMetadata - value: {} is invalid {}",
                hdr_value, e
            )),
        }
    }
}

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<hyper::header::HeaderValue> for header::IntoHeaderValue<TableMetadata> {
    type Error = String;

    fn try_from(hdr_value: hyper::header::HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <TableMetadata as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        "Unable to convert header value '{}' into TableMetadata - {}",
                        value, err
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Unable to convert header: {:?} to string: {}",
                hdr_value, e
            )),
        }
    }
}

/// Assertions from the client that must be valid for the commit to succeed. Assertions are identified by `type` - - `assert-create` - the table must not already exist; used for create transactions - `assert-table-uuid` - the table UUID must match the requirement's `uuid` - `assert-ref-snapshot-id` - the table branch or tag identified by the requirement's `ref` must reference the requirement's `snapshot-id`; if `snapshot-id` is `null` or missing, the ref must not already exist - `assert-last-assigned-field-id` - the table's last assigned column id must match the requirement's `last-assigned-field-id` - `assert-current-schema-id` - the table's current schema id must match the requirement's `current-schema-id` - `assert-last-assigned-partition-id` - the table's last assigned partition id must match the requirement's `last-assigned-partition-id` - `assert-default-spec-id` - the table's default spec id must match the requirement's `default-spec-id` - `assert-default-sort-order-id` - the table's default sort order id must match the requirement's `default-sort-order-id`
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct TableRequirement {
    // Note: inline enums are not fully supported by openapi-generator
    #[serde(rename = "requirement")]
    pub requirement: String,

    #[serde(rename = "ref")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub r#ref: Option<String>,

    #[serde(rename = "uuid")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uuid: Option<String>,

    #[serde(rename = "snapshot-id")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot_id: Option<i64>,

    #[serde(rename = "last-assigned-field-id")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_assigned_field_id: Option<isize>,

    #[serde(rename = "current-schema-id")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_schema_id: Option<isize>,

    #[serde(rename = "last-assigned-partition-id")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_assigned_partition_id: Option<isize>,

    #[serde(rename = "default-spec-id")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_spec_id: Option<isize>,

    #[serde(rename = "default-sort-order-id")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_sort_order_id: Option<isize>,
}

impl TableRequirement {
    #[allow(clippy::new_without_default)]
    pub fn new(requirement: String) -> TableRequirement {
        TableRequirement {
            requirement,
            r#ref: None,
            uuid: None,
            snapshot_id: None,
            last_assigned_field_id: None,
            current_schema_id: None,
            last_assigned_partition_id: None,
            default_spec_id: None,
            default_sort_order_id: None,
        }
    }
}

/// Converts the TableRequirement value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::string::ToString for TableRequirement {
    fn to_string(&self) -> String {
        let params: Vec<Option<String>> = vec![
            Some("requirement".to_string()),
            Some(self.requirement.to_string()),
            self.r#ref
                .as_ref()
                .map(|r#ref| vec!["ref".to_string(), r#ref.to_string()].join(",")),
            self.uuid
                .as_ref()
                .map(|uuid| vec!["uuid".to_string(), uuid.to_string()].join(",")),
            self.snapshot_id.as_ref().map(|snapshot_id| {
                vec!["snapshot-id".to_string(), snapshot_id.to_string()].join(",")
            }),
            self.last_assigned_field_id
                .as_ref()
                .map(|last_assigned_field_id| {
                    vec![
                        "last-assigned-field-id".to_string(),
                        last_assigned_field_id.to_string(),
                    ]
                    .join(",")
                }),
            self.current_schema_id.as_ref().map(|current_schema_id| {
                vec![
                    "current-schema-id".to_string(),
                    current_schema_id.to_string(),
                ]
                .join(",")
            }),
            self.last_assigned_partition_id
                .as_ref()
                .map(|last_assigned_partition_id| {
                    vec![
                        "last-assigned-partition-id".to_string(),
                        last_assigned_partition_id.to_string(),
                    ]
                    .join(",")
                }),
            self.default_spec_id.as_ref().map(|default_spec_id| {
                vec!["default-spec-id".to_string(), default_spec_id.to_string()].join(",")
            }),
            self.default_sort_order_id
                .as_ref()
                .map(|default_sort_order_id| {
                    vec![
                        "default-sort-order-id".to_string(),
                        default_sort_order_id.to_string(),
                    ]
                    .join(",")
                }),
        ];

        params.into_iter().flatten().collect::<Vec<_>>().join(",")
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a TableRequirement value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for TableRequirement {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub requirement: Vec<String>,
            pub r#ref: Vec<String>,
            pub uuid: Vec<String>,
            pub snapshot_id: Vec<i64>,
            pub last_assigned_field_id: Vec<isize>,
            pub current_schema_id: Vec<isize>,
            pub last_assigned_partition_id: Vec<isize>,
            pub default_spec_id: Vec<isize>,
            pub default_sort_order_id: Vec<isize>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing TableRequirement".to_string(),
                    )
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "requirement" => intermediate_rep.requirement.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "ref" => intermediate_rep.r#ref.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "uuid" => intermediate_rep.uuid.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "snapshot-id" => intermediate_rep.snapshot_id.push(
                        <i64 as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "last-assigned-field-id" => intermediate_rep.last_assigned_field_id.push(
                        <isize as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "current-schema-id" => intermediate_rep.current_schema_id.push(
                        <isize as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "last-assigned-partition-id" => {
                        intermediate_rep.last_assigned_partition_id.push(
                            <isize as std::str::FromStr>::from_str(val)
                                .map_err(|x| x.to_string())?,
                        )
                    }
                    #[allow(clippy::redundant_clone)]
                    "default-spec-id" => intermediate_rep.default_spec_id.push(
                        <isize as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "default-sort-order-id" => intermediate_rep.default_sort_order_id.push(
                        <isize as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing TableRequirement".to_string(),
                        )
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(TableRequirement {
            requirement: intermediate_rep
                .requirement
                .into_iter()
                .next()
                .ok_or_else(|| "requirement missing in TableRequirement".to_string())?,
            r#ref: intermediate_rep.r#ref.into_iter().next(),
            uuid: intermediate_rep.uuid.into_iter().next(),
            snapshot_id: intermediate_rep.snapshot_id.into_iter().next(),
            last_assigned_field_id: intermediate_rep.last_assigned_field_id.into_iter().next(),
            current_schema_id: intermediate_rep.current_schema_id.into_iter().next(),
            last_assigned_partition_id: intermediate_rep
                .last_assigned_partition_id
                .into_iter()
                .next(),
            default_spec_id: intermediate_rep.default_spec_id.into_iter().next(),
            default_sort_order_id: intermediate_rep.default_sort_order_id.into_iter().next(),
        })
    }
}

// Methods for converting between header::IntoHeaderValue<TableRequirement> and hyper::header::HeaderValue

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<header::IntoHeaderValue<TableRequirement>>
    for hyper::header::HeaderValue
{
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<TableRequirement>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match hyper::header::HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Invalid header value for TableRequirement - value: {} is invalid {}",
                hdr_value, e
            )),
        }
    }
}

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<hyper::header::HeaderValue>
    for header::IntoHeaderValue<TableRequirement>
{
    type Error = String;

    fn try_from(hdr_value: hyper::header::HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <TableRequirement as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        "Unable to convert header value '{}' into TableRequirement - {}",
                        value, err
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Unable to convert header: {:?} to string: {}",
                hdr_value, e
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct TableUpdate {
    // Note: inline enums are not fully supported by openapi-generator
    #[serde(rename = "action")]
    pub action: String,

    #[serde(rename = "format-version")]
    pub format_version: isize,

    #[serde(rename = "schema")]
    pub schema: models::Schema,

    /// Schema ID to set as current, or -1 to set last added schema
    #[serde(rename = "schema-id")]
    pub schema_id: isize,

    #[serde(rename = "spec")]
    pub spec: models::PartitionSpec,

    /// Partition spec ID to set as the default, or -1 to set last added spec
    #[serde(rename = "spec-id")]
    pub spec_id: isize,

    #[serde(rename = "sort-order")]
    pub sort_order: models::SortOrder,

    /// Sort order ID to set as the default, or -1 to set last added sort order
    #[serde(rename = "sort-order-id")]
    pub sort_order_id: isize,

    #[serde(rename = "snapshot")]
    pub snapshot: models::Snapshot,

    // Note: inline enums are not fully supported by openapi-generator
    #[serde(rename = "type")]
    pub r#type: String,

    #[serde(rename = "snapshot-id")]
    pub snapshot_id: i64,

    #[serde(rename = "max-ref-age-ms")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_ref_age_ms: Option<i64>,

    #[serde(rename = "max-snapshot-age-ms")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_snapshot_age_ms: Option<i64>,

    #[serde(rename = "min-snapshots-to-keep")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_snapshots_to_keep: Option<isize>,

    #[serde(rename = "ref-name")]
    pub ref_name: String,

    #[serde(rename = "snapshot-ids")]
    pub snapshot_ids: Vec<i64>,

    #[serde(rename = "location")]
    pub location: String,

    #[serde(rename = "updates")]
    pub updates: std::collections::HashMap<String, String>,

    #[serde(rename = "removals")]
    pub removals: Vec<String>,
}

impl TableUpdate {
    #[allow(clippy::new_without_default)]
    pub fn new(
        action: String,
        format_version: isize,
        schema: models::Schema,
        schema_id: isize,
        spec: models::PartitionSpec,
        spec_id: isize,
        sort_order: models::SortOrder,
        sort_order_id: isize,
        snapshot: models::Snapshot,
        r#type: String,
        snapshot_id: i64,
        ref_name: String,
        snapshot_ids: Vec<i64>,
        location: String,
        updates: std::collections::HashMap<String, String>,
        removals: Vec<String>,
    ) -> TableUpdate {
        TableUpdate {
            action,
            format_version,
            schema,
            schema_id,
            spec,
            spec_id,
            sort_order,
            sort_order_id,
            snapshot,
            r#type,
            snapshot_id,
            max_ref_age_ms: None,
            max_snapshot_age_ms: None,
            min_snapshots_to_keep: None,
            ref_name,
            snapshot_ids,
            location,
            updates,
            removals,
        }
    }
}

/// Converts the TableUpdate value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::string::ToString for TableUpdate {
    fn to_string(&self) -> String {
        let params: Vec<Option<String>> = vec![
            Some("action".to_string()),
            Some(self.action.to_string()),
            Some("format-version".to_string()),
            Some(self.format_version.to_string()),
            // Skipping schema in query parameter serialization
            Some("schema-id".to_string()),
            Some(self.schema_id.to_string()),
            // Skipping spec in query parameter serialization
            Some("spec-id".to_string()),
            Some(self.spec_id.to_string()),
            // Skipping sort-order in query parameter serialization
            Some("sort-order-id".to_string()),
            Some(self.sort_order_id.to_string()),
            // Skipping snapshot in query parameter serialization
            Some("type".to_string()),
            Some(self.r#type.to_string()),
            Some("snapshot-id".to_string()),
            Some(self.snapshot_id.to_string()),
            self.max_ref_age_ms.as_ref().map(|max_ref_age_ms| {
                vec!["max-ref-age-ms".to_string(), max_ref_age_ms.to_string()].join(",")
            }),
            self.max_snapshot_age_ms
                .as_ref()
                .map(|max_snapshot_age_ms| {
                    vec![
                        "max-snapshot-age-ms".to_string(),
                        max_snapshot_age_ms.to_string(),
                    ]
                    .join(",")
                }),
            self.min_snapshots_to_keep
                .as_ref()
                .map(|min_snapshots_to_keep| {
                    vec![
                        "min-snapshots-to-keep".to_string(),
                        min_snapshots_to_keep.to_string(),
                    ]
                    .join(",")
                }),
            Some("ref-name".to_string()),
            Some(self.ref_name.to_string()),
            Some("snapshot-ids".to_string()),
            Some(
                self.snapshot_ids
                    .iter()
                    .map(|x| x.to_string())
                    .collect::<Vec<_>>()
                    .join(","),
            ),
            Some("location".to_string()),
            Some(self.location.to_string()),
            // Skipping updates in query parameter serialization
            Some("removals".to_string()),
            Some(
                self.removals
                    .iter()
                    .map(|x| x.to_string())
                    .collect::<Vec<_>>()
                    .join(","),
            ),
        ];

        params.into_iter().flatten().collect::<Vec<_>>().join(",")
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a TableUpdate value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for TableUpdate {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub action: Vec<String>,
            pub format_version: Vec<isize>,
            pub schema: Vec<models::Schema>,
            pub schema_id: Vec<isize>,
            pub spec: Vec<models::PartitionSpec>,
            pub spec_id: Vec<isize>,
            pub sort_order: Vec<models::SortOrder>,
            pub sort_order_id: Vec<isize>,
            pub snapshot: Vec<models::Snapshot>,
            pub r#type: Vec<String>,
            pub snapshot_id: Vec<i64>,
            pub max_ref_age_ms: Vec<i64>,
            pub max_snapshot_age_ms: Vec<i64>,
            pub min_snapshots_to_keep: Vec<isize>,
            pub ref_name: Vec<String>,
            pub snapshot_ids: Vec<Vec<i64>>,
            pub location: Vec<String>,
            pub updates: Vec<std::collections::HashMap<String, String>>,
            pub removals: Vec<Vec<String>>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing TableUpdate".to_string(),
                    )
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "action" => intermediate_rep.action.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "format-version" => intermediate_rep.format_version.push(
                        <isize as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "schema" => intermediate_rep.schema.push(
                        <models::Schema as std::str::FromStr>::from_str(val)
                            .map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "schema-id" => intermediate_rep.schema_id.push(
                        <isize as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "spec" => intermediate_rep.spec.push(
                        <models::PartitionSpec as std::str::FromStr>::from_str(val)
                            .map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "spec-id" => intermediate_rep.spec_id.push(
                        <isize as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "sort-order" => intermediate_rep.sort_order.push(
                        <models::SortOrder as std::str::FromStr>::from_str(val)
                            .map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "sort-order-id" => intermediate_rep.sort_order_id.push(
                        <isize as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "snapshot" => intermediate_rep.snapshot.push(
                        <models::Snapshot as std::str::FromStr>::from_str(val)
                            .map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "type" => intermediate_rep.r#type.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "snapshot-id" => intermediate_rep.snapshot_id.push(
                        <i64 as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "max-ref-age-ms" => intermediate_rep.max_ref_age_ms.push(
                        <i64 as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "max-snapshot-age-ms" => intermediate_rep.max_snapshot_age_ms.push(
                        <i64 as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "min-snapshots-to-keep" => intermediate_rep.min_snapshots_to_keep.push(
                        <isize as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "ref-name" => intermediate_rep.ref_name.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    "snapshot-ids" => {
                        return std::result::Result::Err(
                            "Parsing a container in this style is not supported in TableUpdate"
                                .to_string(),
                        )
                    }
                    #[allow(clippy::redundant_clone)]
                    "location" => intermediate_rep.location.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    "updates" => {
                        return std::result::Result::Err(
                            "Parsing a container in this style is not supported in TableUpdate"
                                .to_string(),
                        )
                    }
                    "removals" => {
                        return std::result::Result::Err(
                            "Parsing a container in this style is not supported in TableUpdate"
                                .to_string(),
                        )
                    }
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing TableUpdate".to_string(),
                        )
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(TableUpdate {
            action: intermediate_rep
                .action
                .into_iter()
                .next()
                .ok_or_else(|| "action missing in TableUpdate".to_string())?,
            format_version: intermediate_rep
                .format_version
                .into_iter()
                .next()
                .ok_or_else(|| "format-version missing in TableUpdate".to_string())?,
            schema: intermediate_rep
                .schema
                .into_iter()
                .next()
                .ok_or_else(|| "schema missing in TableUpdate".to_string())?,
            schema_id: intermediate_rep
                .schema_id
                .into_iter()
                .next()
                .ok_or_else(|| "schema-id missing in TableUpdate".to_string())?,
            spec: intermediate_rep
                .spec
                .into_iter()
                .next()
                .ok_or_else(|| "spec missing in TableUpdate".to_string())?,
            spec_id: intermediate_rep
                .spec_id
                .into_iter()
                .next()
                .ok_or_else(|| "spec-id missing in TableUpdate".to_string())?,
            sort_order: intermediate_rep
                .sort_order
                .into_iter()
                .next()
                .ok_or_else(|| "sort-order missing in TableUpdate".to_string())?,
            sort_order_id: intermediate_rep
                .sort_order_id
                .into_iter()
                .next()
                .ok_or_else(|| "sort-order-id missing in TableUpdate".to_string())?,
            snapshot: intermediate_rep
                .snapshot
                .into_iter()
                .next()
                .ok_or_else(|| "snapshot missing in TableUpdate".to_string())?,
            r#type: intermediate_rep
                .r#type
                .into_iter()
                .next()
                .ok_or_else(|| "type missing in TableUpdate".to_string())?,
            snapshot_id: intermediate_rep
                .snapshot_id
                .into_iter()
                .next()
                .ok_or_else(|| "snapshot-id missing in TableUpdate".to_string())?,
            max_ref_age_ms: intermediate_rep.max_ref_age_ms.into_iter().next(),
            max_snapshot_age_ms: intermediate_rep.max_snapshot_age_ms.into_iter().next(),
            min_snapshots_to_keep: intermediate_rep.min_snapshots_to_keep.into_iter().next(),
            ref_name: intermediate_rep
                .ref_name
                .into_iter()
                .next()
                .ok_or_else(|| "ref-name missing in TableUpdate".to_string())?,
            snapshot_ids: intermediate_rep
                .snapshot_ids
                .into_iter()
                .next()
                .ok_or_else(|| "snapshot-ids missing in TableUpdate".to_string())?,
            location: intermediate_rep
                .location
                .into_iter()
                .next()
                .ok_or_else(|| "location missing in TableUpdate".to_string())?,
            updates: intermediate_rep
                .updates
                .into_iter()
                .next()
                .ok_or_else(|| "updates missing in TableUpdate".to_string())?,
            removals: intermediate_rep
                .removals
                .into_iter()
                .next()
                .ok_or_else(|| "removals missing in TableUpdate".to_string())?,
        })
    }
}

// Methods for converting between header::IntoHeaderValue<TableUpdate> and hyper::header::HeaderValue

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<header::IntoHeaderValue<TableUpdate>> for hyper::header::HeaderValue {
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<TableUpdate>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match hyper::header::HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Invalid header value for TableUpdate - value: {} is invalid {}",
                hdr_value, e
            )),
        }
    }
}

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<hyper::header::HeaderValue> for header::IntoHeaderValue<TableUpdate> {
    type Error = String;

    fn try_from(hdr_value: hyper::header::HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <TableUpdate as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        "Unable to convert header value '{}' into TableUpdate - {}",
                        value, err
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Unable to convert header: {:?} to string: {}",
                hdr_value, e
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct Term {
    // Note: inline enums are not fully supported by openapi-generator
    #[serde(rename = "type")]
    pub r#type: String,

    #[serde(rename = "transform")]
    pub transform: String,

    #[serde(rename = "term")]
    pub term: String,
}

impl Term {
    #[allow(clippy::new_without_default)]
    pub fn new(r#type: String, transform: String, term: String) -> Term {
        Term {
            r#type,
            transform,
            term,
        }
    }
}

/// Converts the Term value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::string::ToString for Term {
    fn to_string(&self) -> String {
        let params: Vec<Option<String>> = vec![
            Some("type".to_string()),
            Some(self.r#type.to_string()),
            Some("transform".to_string()),
            Some(self.transform.to_string()),
            Some("term".to_string()),
            Some(self.term.to_string()),
        ];

        params.into_iter().flatten().collect::<Vec<_>>().join(",")
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a Term value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for Term {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub r#type: Vec<String>,
            pub transform: Vec<String>,
            pub term: Vec<String>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err("Missing value while parsing Term".to_string())
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "type" => intermediate_rep.r#type.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "transform" => intermediate_rep.transform.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "term" => intermediate_rep.term.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing Term".to_string(),
                        )
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(Term {
            r#type: intermediate_rep
                .r#type
                .into_iter()
                .next()
                .ok_or_else(|| "type missing in Term".to_string())?,
            transform: intermediate_rep
                .transform
                .into_iter()
                .next()
                .ok_or_else(|| "transform missing in Term".to_string())?,
            term: intermediate_rep
                .term
                .into_iter()
                .next()
                .ok_or_else(|| "term missing in Term".to_string())?,
        })
    }
}

// Methods for converting between header::IntoHeaderValue<Term> and hyper::header::HeaderValue

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<header::IntoHeaderValue<Term>> for hyper::header::HeaderValue {
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<Term>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match hyper::header::HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Invalid header value for Term - value: {} is invalid {}",
                hdr_value, e
            )),
        }
    }
}

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<hyper::header::HeaderValue> for header::IntoHeaderValue<Term> {
    type Error = String;

    fn try_from(hdr_value: hyper::header::HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => match <Term as std::str::FromStr>::from_str(value) {
                std::result::Result::Ok(value) => {
                    std::result::Result::Ok(header::IntoHeaderValue(value))
                }
                std::result::Result::Err(err) => std::result::Result::Err(format!(
                    "Unable to convert header value '{}' into Term - {}",
                    value, err
                )),
            },
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Unable to convert header: {:?} to string: {}",
                hdr_value, e
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct TimerResult {
    #[serde(rename = "time-unit")]
    pub time_unit: String,

    #[serde(rename = "count")]
    pub count: i64,

    #[serde(rename = "total-duration")]
    pub total_duration: i64,
}

impl TimerResult {
    #[allow(clippy::new_without_default)]
    pub fn new(time_unit: String, count: i64, total_duration: i64) -> TimerResult {
        TimerResult {
            time_unit,
            count,
            total_duration,
        }
    }
}

/// Converts the TimerResult value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::string::ToString for TimerResult {
    fn to_string(&self) -> String {
        let params: Vec<Option<String>> = vec![
            Some("time-unit".to_string()),
            Some(self.time_unit.to_string()),
            Some("count".to_string()),
            Some(self.count.to_string()),
            Some("total-duration".to_string()),
            Some(self.total_duration.to_string()),
        ];

        params.into_iter().flatten().collect::<Vec<_>>().join(",")
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a TimerResult value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for TimerResult {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub time_unit: Vec<String>,
            pub count: Vec<i64>,
            pub total_duration: Vec<i64>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing TimerResult".to_string(),
                    )
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "time-unit" => intermediate_rep.time_unit.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "count" => intermediate_rep.count.push(
                        <i64 as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "total-duration" => intermediate_rep.total_duration.push(
                        <i64 as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing TimerResult".to_string(),
                        )
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(TimerResult {
            time_unit: intermediate_rep
                .time_unit
                .into_iter()
                .next()
                .ok_or_else(|| "time-unit missing in TimerResult".to_string())?,
            count: intermediate_rep
                .count
                .into_iter()
                .next()
                .ok_or_else(|| "count missing in TimerResult".to_string())?,
            total_duration: intermediate_rep
                .total_duration
                .into_iter()
                .next()
                .ok_or_else(|| "total-duration missing in TimerResult".to_string())?,
        })
    }
}

// Methods for converting between header::IntoHeaderValue<TimerResult> and hyper::header::HeaderValue

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<header::IntoHeaderValue<TimerResult>> for hyper::header::HeaderValue {
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<TimerResult>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match hyper::header::HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Invalid header value for TimerResult - value: {} is invalid {}",
                hdr_value, e
            )),
        }
    }
}

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<hyper::header::HeaderValue> for header::IntoHeaderValue<TimerResult> {
    type Error = String;

    fn try_from(hdr_value: hyper::header::HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <TimerResult as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        "Unable to convert header value '{}' into TimerResult - {}",
                        value, err
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Unable to convert header: {:?} to string: {}",
                hdr_value, e
            )),
        }
    }
}

/// Token type identifier, from RFC 8693 Section 3  See https://datatracker.ietf.org/doc/html/rfc8693#section-3
/// Enumeration of values.
/// Since this enum's variants do not hold data, we can easily define them them as `#[repr(C)]`
/// which helps with FFI.
#[allow(non_camel_case_types)]
#[repr(C)]
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
#[cfg_attr(feature = "conversion", derive(frunk_enum_derive::LabelledGenericEnum))]
pub enum TokenType {
    #[serde(rename = "urn:ietf:params:oauth:token-type:access_token")]
    AccessToken,
    #[serde(rename = "urn:ietf:params:oauth:token-type:refresh_token")]
    RefreshToken,
    #[serde(rename = "urn:ietf:params:oauth:token-type:id_token")]
    IdToken,
    #[serde(rename = "urn:ietf:params:oauth:token-type:saml1")]
    Saml1,
    #[serde(rename = "urn:ietf:params:oauth:token-type:saml2")]
    Saml2,
    #[serde(rename = "urn:ietf:params:oauth:token-type:jwt")]
    Jwt,
}

impl std::fmt::Display for TokenType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            TokenType::AccessToken => write!(f, "urn:ietf:params:oauth:token-type:access_token"),
            TokenType::RefreshToken => write!(f, "urn:ietf:params:oauth:token-type:refresh_token"),
            TokenType::IdToken => write!(f, "urn:ietf:params:oauth:token-type:id_token"),
            TokenType::Saml1 => write!(f, "urn:ietf:params:oauth:token-type:saml1"),
            TokenType::Saml2 => write!(f, "urn:ietf:params:oauth:token-type:saml2"),
            TokenType::Jwt => write!(f, "urn:ietf:params:oauth:token-type:jwt"),
        }
    }
}

impl std::str::FromStr for TokenType {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "urn:ietf:params:oauth:token-type:access_token" => {
                std::result::Result::Ok(TokenType::AccessToken)
            }
            "urn:ietf:params:oauth:token-type:refresh_token" => {
                std::result::Result::Ok(TokenType::RefreshToken)
            }
            "urn:ietf:params:oauth:token-type:id_token" => {
                std::result::Result::Ok(TokenType::IdToken)
            }
            "urn:ietf:params:oauth:token-type:saml1" => std::result::Result::Ok(TokenType::Saml1),
            "urn:ietf:params:oauth:token-type:saml2" => std::result::Result::Ok(TokenType::Saml2),
            "urn:ietf:params:oauth:token-type:jwt" => std::result::Result::Ok(TokenType::Jwt),
            _ => std::result::Result::Err(format!("Value not valid: {}", s)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct Transform(String);

impl std::convert::From<String> for Transform {
    fn from(x: String) -> Self {
        Transform(x)
    }
}

impl std::string::ToString for Transform {
    fn to_string(&self) -> String {
        self.0.to_string()
    }
}

impl std::str::FromStr for Transform {
    type Err = std::string::ParseError;
    fn from_str(x: &str) -> std::result::Result<Self, Self::Err> {
        std::result::Result::Ok(Transform(x.to_string()))
    }
}

impl std::convert::From<Transform> for String {
    fn from(x: Transform) -> Self {
        x.0
    }
}

impl std::ops::Deref for Transform {
    type Target = String;
    fn deref(&self) -> &String {
        &self.0
    }
}

impl std::ops::DerefMut for Transform {
    fn deref_mut(&mut self) -> &mut String {
        &mut self.0
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct TransformTerm {
    // Note: inline enums are not fully supported by openapi-generator
    #[serde(rename = "type")]
    pub r#type: String,

    #[serde(rename = "transform")]
    pub transform: String,

    #[serde(rename = "term")]
    pub term: String,
}

impl TransformTerm {
    #[allow(clippy::new_without_default)]
    pub fn new(r#type: String, transform: String, term: String) -> TransformTerm {
        TransformTerm {
            r#type,
            transform,
            term,
        }
    }
}

/// Converts the TransformTerm value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::string::ToString for TransformTerm {
    fn to_string(&self) -> String {
        let params: Vec<Option<String>> = vec![
            Some("type".to_string()),
            Some(self.r#type.to_string()),
            Some("transform".to_string()),
            Some(self.transform.to_string()),
            Some("term".to_string()),
            Some(self.term.to_string()),
        ];

        params.into_iter().flatten().collect::<Vec<_>>().join(",")
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a TransformTerm value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for TransformTerm {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub r#type: Vec<String>,
            pub transform: Vec<String>,
            pub term: Vec<String>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing TransformTerm".to_string(),
                    )
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "type" => intermediate_rep.r#type.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "transform" => intermediate_rep.transform.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "term" => intermediate_rep.term.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing TransformTerm".to_string(),
                        )
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(TransformTerm {
            r#type: intermediate_rep
                .r#type
                .into_iter()
                .next()
                .ok_or_else(|| "type missing in TransformTerm".to_string())?,
            transform: intermediate_rep
                .transform
                .into_iter()
                .next()
                .ok_or_else(|| "transform missing in TransformTerm".to_string())?,
            term: intermediate_rep
                .term
                .into_iter()
                .next()
                .ok_or_else(|| "term missing in TransformTerm".to_string())?,
        })
    }
}

// Methods for converting between header::IntoHeaderValue<TransformTerm> and hyper::header::HeaderValue

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<header::IntoHeaderValue<TransformTerm>> for hyper::header::HeaderValue {
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<TransformTerm>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match hyper::header::HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Invalid header value for TransformTerm - value: {} is invalid {}",
                hdr_value, e
            )),
        }
    }
}

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<hyper::header::HeaderValue> for header::IntoHeaderValue<TransformTerm> {
    type Error = String;

    fn try_from(hdr_value: hyper::header::HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <TransformTerm as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        "Unable to convert header value '{}' into TransformTerm - {}",
                        value, err
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Unable to convert header: {:?} to string: {}",
                hdr_value, e
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct Type {
    // Note: inline enums are not fully supported by openapi-generator
    #[serde(rename = "type")]
    pub r#type: String,

    #[serde(rename = "fields")]
    pub fields: Vec<models::StructField>,

    #[serde(rename = "element-id")]
    pub element_id: isize,

    #[serde(rename = "element")]
    pub element: Box<Type>,

    #[serde(rename = "element-required")]
    pub element_required: bool,

    #[serde(rename = "key-id")]
    pub key_id: isize,

    #[serde(rename = "key")]
    pub key: Box<Type>,

    #[serde(rename = "value-id")]
    pub value_id: isize,

    #[serde(rename = "value")]
    pub value: Box<Type>,

    #[serde(rename = "value-required")]
    pub value_required: bool,
}

impl Type {
    #[allow(clippy::new_without_default)]
    pub fn new(
        r#type: String,
        fields: Vec<models::StructField>,
        element_id: isize,
        element: models::Type,
        element_required: bool,
        key_id: isize,
        key: models::Type,
        value_id: isize,
        value: models::Type,
        value_required: bool,
    ) -> Type {
        Type {
            r#type,
            fields,
            element_id,
            element: Box::new(element),
            element_required,
            key_id,
            key: Box::new(key),
            value_id,
            value: Box::new(value),
            value_required,
        }
    }
}

/// Converts the Type value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::string::ToString for Type {
    fn to_string(&self) -> String {
        let params: Vec<Option<String>> = vec![
            Some("type".to_string()),
            Some(self.r#type.to_string()),
            // Skipping fields in query parameter serialization
            Some("element-id".to_string()),
            Some(self.element_id.to_string()),
            // Skipping element in query parameter serialization
            Some("element-required".to_string()),
            Some(self.element_required.to_string()),
            Some("key-id".to_string()),
            Some(self.key_id.to_string()),
            // Skipping key in query parameter serialization
            Some("value-id".to_string()),
            Some(self.value_id.to_string()),
            // Skipping value in query parameter serialization
            Some("value-required".to_string()),
            Some(self.value_required.to_string()),
        ];

        params.into_iter().flatten().collect::<Vec<_>>().join(",")
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a Type value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for Type {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub r#type: Vec<String>,
            pub fields: Vec<Vec<models::StructField>>,
            pub element_id: Vec<isize>,
            pub element: Vec<models::Type>,
            pub element_required: Vec<bool>,
            pub key_id: Vec<isize>,
            pub key: Vec<models::Type>,
            pub value_id: Vec<isize>,
            pub value: Vec<models::Type>,
            pub value_required: Vec<bool>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err("Missing value while parsing Type".to_string())
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "type" => intermediate_rep.r#type.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    "fields" => {
                        return std::result::Result::Err(
                            "Parsing a container in this style is not supported in Type"
                                .to_string(),
                        )
                    }
                    #[allow(clippy::redundant_clone)]
                    "element-id" => intermediate_rep.element_id.push(
                        <isize as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "element" => intermediate_rep.element.push(
                        <models::Type as std::str::FromStr>::from_str(val)
                            .map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "element-required" => intermediate_rep.element_required.push(
                        <bool as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "key-id" => intermediate_rep.key_id.push(
                        <isize as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "key" => intermediate_rep.key.push(
                        <models::Type as std::str::FromStr>::from_str(val)
                            .map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "value-id" => intermediate_rep.value_id.push(
                        <isize as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "value" => intermediate_rep.value.push(
                        <models::Type as std::str::FromStr>::from_str(val)
                            .map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "value-required" => intermediate_rep.value_required.push(
                        <bool as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing Type".to_string(),
                        )
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(Type {
            r#type: intermediate_rep
                .r#type
                .into_iter()
                .next()
                .ok_or_else(|| "type missing in Type".to_string())?,
            fields: intermediate_rep
                .fields
                .into_iter()
                .next()
                .ok_or_else(|| "fields missing in Type".to_string())?,
            element_id: intermediate_rep
                .element_id
                .into_iter()
                .next()
                .ok_or_else(|| "element-id missing in Type".to_string())?,
            element: Box::new(
                intermediate_rep
                    .element
                    .into_iter()
                    .next()
                    .ok_or_else(|| "element missing in Type".to_string())?,
            ),
            element_required: intermediate_rep
                .element_required
                .into_iter()
                .next()
                .ok_or_else(|| "element-required missing in Type".to_string())?,
            key_id: intermediate_rep
                .key_id
                .into_iter()
                .next()
                .ok_or_else(|| "key-id missing in Type".to_string())?,
            key: Box::new(
                intermediate_rep
                    .key
                    .into_iter()
                    .next()
                    .ok_or_else(|| "key missing in Type".to_string())?,
            ),
            value_id: intermediate_rep
                .value_id
                .into_iter()
                .next()
                .ok_or_else(|| "value-id missing in Type".to_string())?,
            value: Box::new(
                intermediate_rep
                    .value
                    .into_iter()
                    .next()
                    .ok_or_else(|| "value missing in Type".to_string())?,
            ),
            value_required: intermediate_rep
                .value_required
                .into_iter()
                .next()
                .ok_or_else(|| "value-required missing in Type".to_string())?,
        })
    }
}

// Methods for converting between header::IntoHeaderValue<Type> and hyper::header::HeaderValue

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<header::IntoHeaderValue<Type>> for hyper::header::HeaderValue {
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<Type>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match hyper::header::HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Invalid header value for Type - value: {} is invalid {}",
                hdr_value, e
            )),
        }
    }
}

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<hyper::header::HeaderValue> for header::IntoHeaderValue<Type> {
    type Error = String;

    fn try_from(hdr_value: hyper::header::HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => match <Type as std::str::FromStr>::from_str(value) {
                std::result::Result::Ok(value) => {
                    std::result::Result::Ok(header::IntoHeaderValue(value))
                }
                std::result::Result::Err(err) => std::result::Result::Err(format!(
                    "Unable to convert header value '{}' into Type - {}",
                    value, err
                )),
            },
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Unable to convert header: {:?} to string: {}",
                hdr_value, e
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct UnaryExpression {
    #[serde(rename = "type")]
    pub r#type: String,

    #[serde(rename = "term")]
    pub term: models::Term,

    #[serde(rename = "value")]
    pub value: serde_json::Value,
}

impl UnaryExpression {
    #[allow(clippy::new_without_default)]
    pub fn new(r#type: String, term: models::Term, value: serde_json::Value) -> UnaryExpression {
        UnaryExpression {
            r#type,
            term,
            value,
        }
    }
}

/// Converts the UnaryExpression value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::string::ToString for UnaryExpression {
    fn to_string(&self) -> String {
        let params: Vec<Option<String>> = vec![
            Some("type".to_string()),
            Some(self.r#type.to_string()),
            // Skipping term in query parameter serialization

            // Skipping value in query parameter serialization
        ];

        params.into_iter().flatten().collect::<Vec<_>>().join(",")
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a UnaryExpression value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for UnaryExpression {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub r#type: Vec<String>,
            pub term: Vec<models::Term>,
            pub value: Vec<serde_json::Value>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing UnaryExpression".to_string(),
                    )
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "type" => intermediate_rep.r#type.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "term" => intermediate_rep.term.push(
                        <models::Term as std::str::FromStr>::from_str(val)
                            .map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "value" => intermediate_rep.value.push(
                        <serde_json::Value as std::str::FromStr>::from_str(val)
                            .map_err(|x| x.to_string())?,
                    ),
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing UnaryExpression".to_string(),
                        )
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(UnaryExpression {
            r#type: intermediate_rep
                .r#type
                .into_iter()
                .next()
                .ok_or_else(|| "type missing in UnaryExpression".to_string())?,
            term: intermediate_rep
                .term
                .into_iter()
                .next()
                .ok_or_else(|| "term missing in UnaryExpression".to_string())?,
            value: intermediate_rep
                .value
                .into_iter()
                .next()
                .ok_or_else(|| "value missing in UnaryExpression".to_string())?,
        })
    }
}

// Methods for converting between header::IntoHeaderValue<UnaryExpression> and hyper::header::HeaderValue

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<header::IntoHeaderValue<UnaryExpression>>
    for hyper::header::HeaderValue
{
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<UnaryExpression>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match hyper::header::HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Invalid header value for UnaryExpression - value: {} is invalid {}",
                hdr_value, e
            )),
        }
    }
}

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<hyper::header::HeaderValue>
    for header::IntoHeaderValue<UnaryExpression>
{
    type Error = String;

    fn try_from(hdr_value: hyper::header::HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <UnaryExpression as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        "Unable to convert header value '{}' into UnaryExpression - {}",
                        value, err
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Unable to convert header: {:?} to string: {}",
                hdr_value, e
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct UpdateNamespacePropertiesRequest {
    #[serde(rename = "removals")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub removals: Option<Vec<String>>,

    #[serde(rename = "updates")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updates: Option<Vec<String>>,
}

impl UpdateNamespacePropertiesRequest {
    #[allow(clippy::new_without_default)]
    pub fn new() -> UpdateNamespacePropertiesRequest {
        UpdateNamespacePropertiesRequest {
            removals: None,
            updates: None,
        }
    }
}

/// Converts the UpdateNamespacePropertiesRequest value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::string::ToString for UpdateNamespacePropertiesRequest {
    fn to_string(&self) -> String {
        let params: Vec<Option<String>> = vec![
            self.removals.as_ref().map(|removals| {
                vec![
                    "removals".to_string(),
                    removals
                        .iter()
                        .map(|x| x.to_string())
                        .collect::<Vec<_>>()
                        .join(","),
                ]
                .join(",")
            }),
            self.updates.as_ref().map(|updates| {
                vec![
                    "updates".to_string(),
                    updates
                        .iter()
                        .map(|x| x.to_string())
                        .collect::<Vec<_>>()
                        .join(","),
                ]
                .join(",")
            }),
        ];

        params.into_iter().flatten().collect::<Vec<_>>().join(",")
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a UpdateNamespacePropertiesRequest value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for UpdateNamespacePropertiesRequest {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub removals: Vec<Vec<String>>,
            pub updates: Vec<Vec<String>>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing UpdateNamespacePropertiesRequest".to_string(),
                    )
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    "removals" => return std::result::Result::Err("Parsing a container in this style is not supported in UpdateNamespacePropertiesRequest".to_string()),
                    "updates" => return std::result::Result::Err("Parsing a container in this style is not supported in UpdateNamespacePropertiesRequest".to_string()),
                    _ => return std::result::Result::Err("Unexpected key while parsing UpdateNamespacePropertiesRequest".to_string())
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(UpdateNamespacePropertiesRequest {
            removals: intermediate_rep.removals.into_iter().next(),
            updates: intermediate_rep.updates.into_iter().next(),
        })
    }
}

// Methods for converting between header::IntoHeaderValue<UpdateNamespacePropertiesRequest> and hyper::header::HeaderValue

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<header::IntoHeaderValue<UpdateNamespacePropertiesRequest>>
    for hyper::header::HeaderValue
{
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<UpdateNamespacePropertiesRequest>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match hyper::header::HeaderValue::from_str(&hdr_value) {
             std::result::Result::Ok(value) => std::result::Result::Ok(value),
             std::result::Result::Err(e) => std::result::Result::Err(
                 format!("Invalid header value for UpdateNamespacePropertiesRequest - value: {} is invalid {}",
                     hdr_value, e))
        }
    }
}

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<hyper::header::HeaderValue>
    for header::IntoHeaderValue<UpdateNamespacePropertiesRequest>
{
    type Error = String;

    fn try_from(hdr_value: hyper::header::HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
             std::result::Result::Ok(value) => {
                    match <UpdateNamespacePropertiesRequest as std::str::FromStr>::from_str(value) {
                        std::result::Result::Ok(value) => std::result::Result::Ok(header::IntoHeaderValue(value)),
                        std::result::Result::Err(err) => std::result::Result::Err(
                            format!("Unable to convert header value '{}' into UpdateNamespacePropertiesRequest - {}",
                                value, err))
                    }
             },
             std::result::Result::Err(e) => std::result::Result::Err(
                 format!("Unable to convert header: {:?} to string: {}",
                     hdr_value, e))
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct UpdateProperties200Response {
    /// List of property keys that were added or updated
    #[serde(rename = "updated")]
    pub updated: Vec<String>,

    /// List of properties that were removed
    #[serde(rename = "removed")]
    pub removed: Vec<String>,

    /// List of properties requested for removal that were not found in the namespace's properties. Represents a partial success response. Server's do not need to implement this.
    #[serde(rename = "missing")]
    #[serde(deserialize_with = "swagger::nullable_format::deserialize_optional_nullable")]
    #[serde(default = "swagger::nullable_format::default_optional_nullable")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub missing: Option<swagger::Nullable<Vec<String>>>,
}

impl UpdateProperties200Response {
    #[allow(clippy::new_without_default)]
    pub fn new(updated: Vec<String>, removed: Vec<String>) -> UpdateProperties200Response {
        UpdateProperties200Response {
            updated,
            removed,
            missing: None,
        }
    }
}

/// Converts the UpdateProperties200Response value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::string::ToString for UpdateProperties200Response {
    fn to_string(&self) -> String {
        let params: Vec<Option<String>> = vec![
            Some("updated".to_string()),
            Some(
                self.updated
                    .iter()
                    .map(|x| x.to_string())
                    .collect::<Vec<_>>()
                    .join(","),
            ),
            Some("removed".to_string()),
            Some(
                self.removed
                    .iter()
                    .map(|x| x.to_string())
                    .collect::<Vec<_>>()
                    .join(","),
            ),
            self.missing.as_ref().map(|missing| {
                vec![
                    "missing".to_string(),
                    missing.as_ref().map_or("null".to_string(), |x| {
                        x.iter()
                            .map(|x| x.to_string())
                            .collect::<Vec<_>>()
                            .join(",")
                    }),
                ]
                .join(",")
            }),
        ];

        params.into_iter().flatten().collect::<Vec<_>>().join(",")
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a UpdateProperties200Response value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for UpdateProperties200Response {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub updated: Vec<Vec<String>>,
            pub removed: Vec<Vec<String>>,
            pub missing: Vec<Vec<String>>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing UpdateProperties200Response".to_string(),
                    )
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    "updated" => return std::result::Result::Err("Parsing a container in this style is not supported in UpdateProperties200Response".to_string()),
                    "removed" => return std::result::Result::Err("Parsing a container in this style is not supported in UpdateProperties200Response".to_string()),
                    "missing" => return std::result::Result::Err("Parsing a container in this style is not supported in UpdateProperties200Response".to_string()),
                    _ => return std::result::Result::Err("Unexpected key while parsing UpdateProperties200Response".to_string())
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(UpdateProperties200Response {
            updated: intermediate_rep
                .updated
                .into_iter()
                .next()
                .ok_or_else(|| "updated missing in UpdateProperties200Response".to_string())?,
            removed: intermediate_rep
                .removed
                .into_iter()
                .next()
                .ok_or_else(|| "removed missing in UpdateProperties200Response".to_string())?,
            missing: std::result::Result::Err(
                "Nullable types not supported in UpdateProperties200Response".to_string(),
            )?,
        })
    }
}

// Methods for converting between header::IntoHeaderValue<UpdateProperties200Response> and hyper::header::HeaderValue

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<header::IntoHeaderValue<UpdateProperties200Response>>
    for hyper::header::HeaderValue
{
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<UpdateProperties200Response>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match hyper::header::HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Invalid header value for UpdateProperties200Response - value: {} is invalid {}",
                hdr_value, e
            )),
        }
    }
}

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<hyper::header::HeaderValue>
    for header::IntoHeaderValue<UpdateProperties200Response>
{
    type Error = String;

    fn try_from(hdr_value: hyper::header::HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <UpdateProperties200Response as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        "Unable to convert header value '{}' into UpdateProperties200Response - {}",
                        value, err
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Unable to convert header: {:?} to string: {}",
                hdr_value, e
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct UpdateTable200Response {
    #[serde(rename = "metadata-location")]
    pub metadata_location: String,

    #[serde(rename = "metadata")]
    pub metadata: models::TableMetadata,
}

impl UpdateTable200Response {
    #[allow(clippy::new_without_default)]
    pub fn new(
        metadata_location: String,
        metadata: models::TableMetadata,
    ) -> UpdateTable200Response {
        UpdateTable200Response {
            metadata_location,
            metadata,
        }
    }
}

/// Converts the UpdateTable200Response value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::string::ToString for UpdateTable200Response {
    fn to_string(&self) -> String {
        let params: Vec<Option<String>> = vec![
            Some("metadata-location".to_string()),
            Some(self.metadata_location.to_string()),
            // Skipping metadata in query parameter serialization
        ];

        params.into_iter().flatten().collect::<Vec<_>>().join(",")
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a UpdateTable200Response value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for UpdateTable200Response {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub metadata_location: Vec<String>,
            pub metadata: Vec<models::TableMetadata>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing UpdateTable200Response".to_string(),
                    )
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "metadata-location" => intermediate_rep.metadata_location.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "metadata" => intermediate_rep.metadata.push(
                        <models::TableMetadata as std::str::FromStr>::from_str(val)
                            .map_err(|x| x.to_string())?,
                    ),
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing UpdateTable200Response".to_string(),
                        )
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(UpdateTable200Response {
            metadata_location: intermediate_rep
                .metadata_location
                .into_iter()
                .next()
                .ok_or_else(|| "metadata-location missing in UpdateTable200Response".to_string())?,
            metadata: intermediate_rep
                .metadata
                .into_iter()
                .next()
                .ok_or_else(|| "metadata missing in UpdateTable200Response".to_string())?,
        })
    }
}

// Methods for converting between header::IntoHeaderValue<UpdateTable200Response> and hyper::header::HeaderValue

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<header::IntoHeaderValue<UpdateTable200Response>>
    for hyper::header::HeaderValue
{
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<UpdateTable200Response>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match hyper::header::HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Invalid header value for UpdateTable200Response - value: {} is invalid {}",
                hdr_value, e
            )),
        }
    }
}

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<hyper::header::HeaderValue>
    for header::IntoHeaderValue<UpdateTable200Response>
{
    type Error = String;

    fn try_from(hdr_value: hyper::header::HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <UpdateTable200Response as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        "Unable to convert header value '{}' into UpdateTable200Response - {}",
                        value, err
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Unable to convert header: {:?} to string: {}",
                hdr_value, e
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct UpgradeFormatVersionUpdate {
    // Note: inline enums are not fully supported by openapi-generator
    #[serde(rename = "action")]
    pub action: String,

    #[serde(rename = "format-version")]
    pub format_version: isize,
}

impl UpgradeFormatVersionUpdate {
    #[allow(clippy::new_without_default)]
    pub fn new(action: String, format_version: isize) -> UpgradeFormatVersionUpdate {
        UpgradeFormatVersionUpdate {
            action,
            format_version,
        }
    }
}

/// Converts the UpgradeFormatVersionUpdate value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::string::ToString for UpgradeFormatVersionUpdate {
    fn to_string(&self) -> String {
        let params: Vec<Option<String>> = vec![
            Some("action".to_string()),
            Some(self.action.to_string()),
            Some("format-version".to_string()),
            Some(self.format_version.to_string()),
        ];

        params.into_iter().flatten().collect::<Vec<_>>().join(",")
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a UpgradeFormatVersionUpdate value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for UpgradeFormatVersionUpdate {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub action: Vec<String>,
            pub format_version: Vec<isize>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing UpgradeFormatVersionUpdate".to_string(),
                    )
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "action" => intermediate_rep.action.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "format-version" => intermediate_rep.format_version.push(
                        <isize as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing UpgradeFormatVersionUpdate".to_string(),
                        )
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(UpgradeFormatVersionUpdate {
            action: intermediate_rep
                .action
                .into_iter()
                .next()
                .ok_or_else(|| "action missing in UpgradeFormatVersionUpdate".to_string())?,
            format_version: intermediate_rep
                .format_version
                .into_iter()
                .next()
                .ok_or_else(|| {
                    "format-version missing in UpgradeFormatVersionUpdate".to_string()
                })?,
        })
    }
}

// Methods for converting between header::IntoHeaderValue<UpgradeFormatVersionUpdate> and hyper::header::HeaderValue

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<header::IntoHeaderValue<UpgradeFormatVersionUpdate>>
    for hyper::header::HeaderValue
{
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<UpgradeFormatVersionUpdate>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match hyper::header::HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Invalid header value for UpgradeFormatVersionUpdate - value: {} is invalid {}",
                hdr_value, e
            )),
        }
    }
}

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<hyper::header::HeaderValue>
    for header::IntoHeaderValue<UpgradeFormatVersionUpdate>
{
    type Error = String;

    fn try_from(hdr_value: hyper::header::HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <UpgradeFormatVersionUpdate as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        "Unable to convert header value '{}' into UpgradeFormatVersionUpdate - {}",
                        value, err
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Unable to convert header: {:?} to string: {}",
                hdr_value, e
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct UpgradeFormatVersionUpdateAllOf {
    #[serde(rename = "format-version")]
    pub format_version: isize,
}

impl UpgradeFormatVersionUpdateAllOf {
    #[allow(clippy::new_without_default)]
    pub fn new(format_version: isize) -> UpgradeFormatVersionUpdateAllOf {
        UpgradeFormatVersionUpdateAllOf { format_version }
    }
}

/// Converts the UpgradeFormatVersionUpdateAllOf value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::string::ToString for UpgradeFormatVersionUpdateAllOf {
    fn to_string(&self) -> String {
        let params: Vec<Option<String>> = vec![
            Some("format-version".to_string()),
            Some(self.format_version.to_string()),
        ];

        params.into_iter().flatten().collect::<Vec<_>>().join(",")
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a UpgradeFormatVersionUpdateAllOf value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for UpgradeFormatVersionUpdateAllOf {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub format_version: Vec<isize>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing UpgradeFormatVersionUpdateAllOf".to_string(),
                    )
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "format-version" => intermediate_rep.format_version.push(
                        <isize as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing UpgradeFormatVersionUpdateAllOf"
                                .to_string(),
                        )
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(UpgradeFormatVersionUpdateAllOf {
            format_version: intermediate_rep
                .format_version
                .into_iter()
                .next()
                .ok_or_else(|| {
                    "format-version missing in UpgradeFormatVersionUpdateAllOf".to_string()
                })?,
        })
    }
}

// Methods for converting between header::IntoHeaderValue<UpgradeFormatVersionUpdateAllOf> and hyper::header::HeaderValue

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<header::IntoHeaderValue<UpgradeFormatVersionUpdateAllOf>>
    for hyper::header::HeaderValue
{
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<UpgradeFormatVersionUpdateAllOf>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match hyper::header::HeaderValue::from_str(&hdr_value) {
             std::result::Result::Ok(value) => std::result::Result::Ok(value),
             std::result::Result::Err(e) => std::result::Result::Err(
                 format!("Invalid header value for UpgradeFormatVersionUpdateAllOf - value: {} is invalid {}",
                     hdr_value, e))
        }
    }
}

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<hyper::header::HeaderValue>
    for header::IntoHeaderValue<UpgradeFormatVersionUpdateAllOf>
{
    type Error = String;

    fn try_from(hdr_value: hyper::header::HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
             std::result::Result::Ok(value) => {
                    match <UpgradeFormatVersionUpdateAllOf as std::str::FromStr>::from_str(value) {
                        std::result::Result::Ok(value) => std::result::Result::Ok(header::IntoHeaderValue(value)),
                        std::result::Result::Err(err) => std::result::Result::Err(
                            format!("Unable to convert header value '{}' into UpgradeFormatVersionUpdateAllOf - {}",
                                value, err))
                    }
             },
             std::result::Result::Err(e) => std::result::Result::Err(
                 format!("Unable to convert header: {:?} to string: {}",
                     hdr_value, e))
        }
    }
}

/*!
 * Helper functions to handle bytes
*/

use std::any::Any;

use anyhow::{anyhow, Result};

use super::data_types::{PrimitiveType, Type};

/// Convert bytes to concrete type and return it as any
pub fn bytes_to_any(bytes: &[u8], data_type: &Type) -> Result<Box<dyn Any>> {
    match data_type {
        Type::Primitive(primitive) => match primitive {
            PrimitiveType::Boolean => {
                if bytes.len() == 1 && bytes[0] == 0u8 {
                    Ok(Box::new(false))
                } else {
                    Ok(Box::new(true))
                }
            }
            PrimitiveType::Int => Ok(Box::new(i32::from_le_bytes(bytes.try_into()?))),
            PrimitiveType::Long => Ok(Box::new(i64::from_le_bytes(bytes.try_into()?))),
            PrimitiveType::Float => Ok(Box::new(f32::from_le_bytes(bytes.try_into()?))),
            PrimitiveType::Double => Ok(Box::new(f64::from_le_bytes(bytes.try_into()?))),
            PrimitiveType::Date => Ok(Box::new(i32::from_le_bytes(bytes.try_into()?))),
            PrimitiveType::Time => Ok(Box::new(i64::from_le_bytes(bytes.try_into()?))),
            PrimitiveType::Timestamp => Ok(Box::new(i64::from_le_bytes(bytes.try_into()?))),
            PrimitiveType::Timestampz => Ok(Box::new(i64::from_le_bytes(bytes.try_into()?))),
            PrimitiveType::String => Ok(Box::new(std::str::from_utf8(bytes)?.to_string())),
            PrimitiveType::Uuid => Ok(Box::new(i128::from_le_bytes(bytes.try_into()?))),
            PrimitiveType::Fixed(_len) => Ok(Box::new(Vec::from(bytes))),
            PrimitiveType::Binary => Ok(Box::new(Vec::from(bytes))),
            PrimitiveType::Decimal {
                precision: _,
                scale: _,
            } => Ok(Box::new(i128::from_le_bytes(bytes.try_into()?))),
        },
        _ => Err(anyhow!("Only primitive types can be stored as bytes.")),
    }
}

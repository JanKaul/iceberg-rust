use aws_sdk_glue::types::Column;
use iceberg_rust::spec::types::{PrimitiveType, StructType, Type};

use crate::error::Error;

pub(crate) fn schema_to_glue(fields: &StructType) -> Result<Vec<Column>, Error> {
    fields
        .iter()
        .map(|x| {
            Ok(Column::builder()
                .name(x.name.clone())
                .set_type(type_to_glue(&x.field_type).ok())
                .set_comment(x.doc.clone())
                .build()?)
        })
        .collect::<Result<_, Error>>()
}

pub(crate) fn type_to_glue(datatype: &Type) -> Result<String, Error> {
    match datatype {
        Type::Primitive(prim) => match prim {
            PrimitiveType::Boolean => Ok("boolean".to_owned()),
            PrimitiveType::Int => Ok("int".to_owned()),
            PrimitiveType::Long => Ok("bigint".to_owned()),
            PrimitiveType::Float => Ok("float".to_owned()),
            PrimitiveType::Double => Ok("double".to_owned()),
            PrimitiveType::Date => Ok("date".to_owned()),
            PrimitiveType::Timestamp => Ok("timestamp".to_owned()),
            PrimitiveType::String | PrimitiveType::Uuid => Ok("string".to_owned()),
            PrimitiveType::Binary | PrimitiveType::Fixed(_) => Ok("binary".to_owned()),
            x => Err(Error::Text(format!(
                "Type {} cannot be converted to glue type",
                x
            ))),
        },
        x => Err(Error::Text(format!(
            "Type {} cannot be converted to glue type",
            x
        ))),
    }
}

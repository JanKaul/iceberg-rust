//! Decimal helpers for Iceberg's maximum 38-digit precision.

use fastnum::{
    decimal::{Context, Sign},
    D128, U128,
};

use crate::error::Error;

/// Decimal representation capable of storing every Iceberg decimal value.
pub type Decimal = D128;

/// Creates a decimal from an unscaled value and scale.
#[must_use]
pub fn decimal_from_i128_with_scale(mantissa: i128, scale: u32) -> Decimal {
    let sign = if mantissa < 0 {
        Sign::Minus
    } else {
        Sign::Plus
    };
    let exponent = i32::try_from(scale)
        .expect("Iceberg decimal scale fits in i32")
        .checked_neg()
        .expect("Iceberg decimal scale can be negated");

    D128::from_parts(
        U128::from_u128(mantissa.unsigned_abs()).expect("an i128 magnitude always fits in U128"),
        exponent,
        sign,
        Context::default(),
    )
}

/// Parses an exact decimal value.
pub fn decimal_from_str_exact(value: &str) -> Result<Decimal, Error> {
    D128::from_str(value, Context::default())
        .map_err(|_| Error::Conversion(value.to_string(), "decimal".to_string()))
}

/// Returns the signed unscaled value.
#[must_use]
pub fn decimal_mantissa(decimal: &Decimal) -> i128 {
    let magnitude = decimal
        .digits()
        .to_u128()
        .expect("an Iceberg decimal has at most 38 digits");
    let magnitude = i128::try_from(magnitude).expect("38 decimal digits fit in i128");
    if decimal.is_sign_negative() {
        -magnitude
    } else {
        magnitude
    }
}

/// Returns the number of digits after the decimal point.
#[must_use]
pub fn decimal_scale(decimal: &Decimal) -> u32 {
    decimal.fractional_digits_count().max(0) as u32
}

/// Encodes an i128 using the minimum-length big-endian two's-complement form.
#[must_use]
pub fn i128_to_be_bytes_min(value: i128) -> Vec<u8> {
    let bytes = value.to_be_bytes();
    let is_negative = value < 0;
    let padding = if is_negative { 0xff } else { 0x00 };
    let mut start = 0;

    while start < bytes.len() - 1 && bytes[start] == padding {
        let next_is_negative = bytes[start + 1] & 0x80 != 0;
        if next_is_negative != is_negative {
            break;
        }
        start += 1;
    }

    bytes[start..].to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn supports_iceberg_precision_38() {
        for value in [
            "99999999999999999999999999999999999999",
            "-99999999999999999999999999999999999999",
        ] {
            let decimal = decimal_from_str_exact(value).unwrap();
            assert_eq!(decimal.to_string(), value);
        }
    }

    #[test]
    fn mantissa_and_scale_round_trip() {
        for (mantissa, scale) in [
            (99_999_999_999_999_999_999_999_999_999_999_999_999_i128, 0),
            (-99_999_999_999_999_999_999_999_999_999_999_999_999_i128, 7),
            (1, 38),
        ] {
            let decimal = decimal_from_i128_with_scale(mantissa, scale);
            assert_eq!(decimal_mantissa(&decimal), mantissa);
            assert_eq!(decimal_scale(&decimal), scale);
        }
    }

    #[test]
    fn minimal_big_endian_encoding_preserves_sign() {
        assert_eq!(i128_to_be_bytes_min(127), vec![0x7f]);
        assert_eq!(i128_to_be_bytes_min(128), vec![0x00, 0x80]);
        assert_eq!(i128_to_be_bytes_min(-128), vec![0x80]);
        assert_eq!(i128_to_be_bytes_min(-129), vec![0xff, 0x7f]);
    }
}

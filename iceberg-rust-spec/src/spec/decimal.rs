//! Decimal helpers for Iceberg's maximum 38-digit precision.

use fastnum::{
    decimal::{Context, Sign},
    D128, U128,
};

use crate::error::Error;

/// Decimal representation capable of storing every Iceberg decimal value.
pub type Decimal = D128;

/// Creates a decimal from an unscaled value and scale.
pub fn decimal_from_i128_with_scale(mantissa: i128, scale: u32) -> Result<Decimal, Error> {
    let sign = if mantissa < 0 {
        Sign::Minus
    } else {
        Sign::Plus
    };
    let exponent = -i32::try_from(scale)?;
    let magnitude = U128::from_u128(mantissa.unsigned_abs())
        .map_err(|_| Error::Conversion(mantissa.to_string(), "decimal mantissa".to_string()))?;

    Ok(D128::from_parts(
        magnitude,
        exponent,
        sign,
        Context::default(),
    ))
}

/// Parses an exact decimal value.
pub fn decimal_from_str_exact(value: &str) -> Result<Decimal, Error> {
    D128::from_str(value, Context::default())
        .map_err(|_| Error::Conversion(value.to_string(), "decimal".to_string()))
}

/// Returns the signed unscaled value.
pub fn decimal_mantissa(decimal: &Decimal) -> Result<i128, Error> {
    let magnitude = decimal
        .digits()
        .to_u128()
        .map_err(|_| Error::Conversion(decimal.to_string(), "i128 decimal mantissa".to_string()))?;

    if decimal.is_sign_negative() {
        if magnitude == i128::MIN.unsigned_abs() {
            Ok(i128::MIN)
        } else {
            Ok(-i128::try_from(magnitude)?)
        }
    } else {
        Ok(i128::try_from(magnitude)?)
    }
}

/// Encodes a decimal using the minimum-length big-endian two's-complement form.
#[must_use]
pub(crate) fn decimal_to_be_bytes_min(decimal: &Decimal) -> Vec<u8> {
    let mut bytes = decimal.digits().to_radix_be(256);
    if bytes.is_empty() || bytes.iter().all(|byte| *byte == 0) {
        return vec![0];
    }

    if decimal.is_sign_negative() {
        bytes.insert(0, 0);
        bytes.iter_mut().for_each(|byte| *byte = !*byte);

        for byte in bytes.iter_mut().rev() {
            let (value, carry) = byte.overflowing_add(1);
            *byte = value;
            if !carry {
                break;
            }
        }

        if bytes[0] == 0xff && bytes[1] & 0x80 != 0 {
            bytes.remove(0);
        }
    } else if bytes[0] & 0x80 != 0 {
        bytes.insert(0, 0);
    }

    bytes
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
            let decimal = decimal_from_i128_with_scale(mantissa, scale).unwrap();
            assert_eq!(decimal_mantissa(&decimal).unwrap(), mantissa);
            assert_eq!(decimal_scale(&decimal), scale);
        }
    }

    #[test]
    fn rejects_scale_outside_i32() {
        assert!(decimal_from_i128_with_scale(1, i32::MAX as u32 + 1).is_err());
    }

    #[test]
    fn rejects_mantissa_outside_i128() {
        let decimal = decimal_from_str_exact("170141183460469231731687303715884105728").unwrap();
        assert!(decimal_mantissa(&decimal).is_err());
    }

    #[test]
    fn minimal_big_endian_encoding_preserves_sign() {
        assert_eq!(i128_to_be_bytes_min(127), vec![0x7f]);
        assert_eq!(i128_to_be_bytes_min(128), vec![0x00, 0x80]);
        assert_eq!(i128_to_be_bytes_min(-128), vec![0x80]);
        assert_eq!(i128_to_be_bytes_min(-129), vec![0xff, 0x7f]);
    }

    #[test]
    fn decimal_big_endian_encoding_does_not_require_i128() {
        let positive = decimal_from_str_exact("170141183460469231731687303715884105728").unwrap();
        let negative = decimal_from_str_exact("-170141183460469231731687303715884105729").unwrap();

        assert_eq!(
            decimal_to_be_bytes_min(&positive),
            vec![0, 0x80, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        );
        assert_eq!(
            decimal_to_be_bytes_min(&negative),
            vec![
                0xff, 0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
                0xff, 0xff, 0xff,
            ]
        );
    }

    #[test]
    fn decimal_big_endian_encoding_matches_i128_encoding() {
        for value in [i128::MIN, -129, -128, -1, 0, 1, 127, 128, i128::MAX] {
            let decimal = decimal_from_i128_with_scale(value, 0).unwrap();
            assert_eq!(
                decimal_to_be_bytes_min(&decimal),
                i128_to_be_bytes_min(value)
            );
        }
    }
}

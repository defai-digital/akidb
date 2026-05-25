//! IEEE 754 half-precision (FP16) encoding and decoding.
//!
//! Mirrors the TypeScript `fp16.ts` codec. Denormal values flush to zero.

const FP16_EXPONENT_BIAS: i32 = 15;
const FP32_EXPONENT_BIAS: i32 = 127;
const FP16_MAX_EXPONENT: u32 = 31;
const FP16_MANTISSA_BITS: u32 = 10;
const FP32_MANTISSA_BITS: u32 = 23;
const MANTISSA_SHIFT: u32 = FP32_MANTISSA_BITS - FP16_MANTISSA_BITS; // 13

/// Encode a single f32 to FP16 (u16).
#[inline]
pub fn encode_float16(value: f32) -> u16 {
    let bits = value.to_bits();
    let sign = (bits >> 31) & 1;
    let exponent = ((bits >> FP32_MANTISSA_BITS) & 0xff) as i32;
    let mantissa = bits & 0x7fffff;

    // NaN
    if exponent == 0xff && mantissa != 0 {
        return ((sign << 15) | (FP16_MAX_EXPONENT << FP16_MANTISSA_BITS) | 0x200) as u16;
    }

    // Infinity
    if exponent == 0xff {
        return ((sign << 15) | (FP16_MAX_EXPONENT << FP16_MANTISSA_BITS)) as u16;
    }

    let fp16_exp = exponent - FP32_EXPONENT_BIAS + FP16_EXPONENT_BIAS;

    // Underflow or denormal — flush to zero.
    if fp16_exp <= 0 {
        return (sign << 15) as u16;
    }

    // Overflow — clamp to infinity.
    if fp16_exp >= FP16_MAX_EXPONENT as i32 {
        return ((sign << 15) | (FP16_MAX_EXPONENT << FP16_MANTISSA_BITS)) as u16;
    }

    let fp16_mantissa = mantissa >> MANTISSA_SHIFT;
    ((sign << 15) | ((fp16_exp as u32) << FP16_MANTISSA_BITS) | fp16_mantissa) as u16
}

/// Decode a single FP16 (u16) to f32.
#[inline]
pub fn decode_float16(half: u16) -> f32 {
    let sign = ((half >> 15) & 1) as u32;
    let exponent = ((half >> FP16_MANTISSA_BITS as u16) & 0x1f) as u32;
    let mantissa = (half & 0x3ff) as u32;

    // Zero or denormal (flushed to zero).
    if exponent == 0 {
        return if sign == 1 { -0.0 } else { 0.0 };
    }

    // Infinity or NaN.
    if exponent == FP16_MAX_EXPONENT {
        let fp32_bits = (sign << 31) | (0xff << FP32_MANTISSA_BITS) | (mantissa << MANTISSA_SHIFT);
        return f32::from_bits(fp32_bits);
    }

    // Normal number.
    let fp32_exp = (exponent as i32 - FP16_EXPONENT_BIAS + FP32_EXPONENT_BIAS) as u32;
    let fp32_bits = (sign << 31) | (fp32_exp << FP32_MANTISSA_BITS) | (mantissa << MANTISSA_SHIFT);
    f32::from_bits(fp32_bits)
}

/// Encode a slice of f32 values to FP16 bytes (little-endian).
pub fn encode_fp16_vector(values: &[f32]) -> Vec<u8> {
    let mut out = vec![0u8; values.len() * 2];
    for (i, &v) in values.iter().enumerate() {
        let half = encode_float16(v);
        out[i * 2] = (half & 0xff) as u8;
        out[i * 2 + 1] = (half >> 8) as u8;
    }
    out
}

/// Decode FP16 bytes (little-endian) back to f32.
pub fn decode_fp16_vector(bytes: &[u8]) -> Vec<f32> {
    let count = bytes.len() / 2;
    let mut result = vec![0.0_f32; count];
    for i in 0..count {
        let half = (bytes[i * 2] as u16) | ((bytes[i * 2 + 1] as u16) << 8);
        result[i] = decode_float16(half);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_roundtrip_normal() {
        let values = [0.5_f32, -1.0, 0.0, std::f32::consts::PI, 100.0, -0.001];
        for &v in &values {
            let half = encode_float16(v);
            let back = decode_float16(half);
            // FP16 has ~3 decimal digits of precision.
            let rel_err = if v.abs() > 1e-4 {
                ((back - v) / v).abs()
            } else {
                (back - v).abs()
            };
            assert!(rel_err < 0.01, "v={v}, back={back}, err={rel_err}");
        }
    }

    #[test]
    fn test_zero() {
        let half = encode_float16(0.0);
        let back = decode_float16(half);
        assert_eq!(back, 0.0);
    }

    #[test]
    fn test_infinity() {
        let half = encode_float16(f32::INFINITY);
        let back = decode_float16(half);
        assert!(back.is_infinite() && back > 0.0);

        let half_neg = encode_float16(f32::NEG_INFINITY);
        let back_neg = decode_float16(half_neg);
        assert!(back_neg.is_infinite() && back_neg < 0.0);
    }

    #[test]
    fn test_nan() {
        let half = encode_float16(f32::NAN);
        let back = decode_float16(half);
        assert!(back.is_nan());
    }

    #[test]
    fn test_vector_roundtrip() {
        let values = vec![1.0_f32, 0.5, -0.25, 0.0, 2.0, -3.0, 0.125, 100.0];
        let encoded = encode_fp16_vector(&values);
        assert_eq!(encoded.len(), values.len() * 2);
        let decoded = decode_fp16_vector(&encoded);
        assert_eq!(decoded.len(), values.len());
        for (i, (&orig, &dec)) in values.iter().zip(decoded.iter()).enumerate() {
            let err = if orig.abs() > 1e-4 {
                ((dec - orig) / orig).abs()
            } else {
                (dec - orig).abs()
            };
            assert!(err < 0.01, "index={i}, orig={orig}, decoded={dec}");
        }
    }
}

//! SIMD-friendly distance functions for vector similarity search.
//!
//! All distance functions return values where **lower = closer**.
//! Score conversion to "higher = more relevant" happens at the NAPI boundary.

/// Cosine distance = 1 - cosine_similarity.
/// Expects **pre-normalized** vectors (norm = 1).
#[inline]
pub fn cosine_distance(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len());
    1.0 - dot_product(a, b)
}

/// Euclidean (L2) distance (not squared — takes sqrt).
#[inline]
pub fn l2_distance(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len());
    let mut sum = 0.0_f32;
    // Process in chunks of 4 for auto-vectorization.
    let chunks = a.len() / 4;
    let remainder = a.len() % 4;

    for i in 0..chunks {
        let base = i * 4;
        let d0 = a[base] - b[base];
        let d1 = a[base + 1] - b[base + 1];
        let d2 = a[base + 2] - b[base + 2];
        let d3 = a[base + 3] - b[base + 3];
        sum += d0 * d0 + d1 * d1 + d2 * d2 + d3 * d3;
    }

    let base = chunks * 4;
    for i in 0..remainder {
        let d = a[base + i] - b[base + i];
        sum += d * d;
    }

    sum.sqrt()
}

/// Dot product distance = -dot(a, b).
/// For dot metric, higher dot = more similar, so distance = -dot.
#[inline]
pub fn dot_distance(a: &[f32], b: &[f32]) -> f32 {
    -dot_product(a, b)
}

/// Raw dot product, structured for auto-vectorization.
#[inline]
pub fn dot_product(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len());
    let mut sum = 0.0_f32;
    let chunks = a.len() / 4;
    let remainder = a.len() % 4;

    for i in 0..chunks {
        let base = i * 4;
        sum += a[base] * b[base]
            + a[base + 1] * b[base + 1]
            + a[base + 2] * b[base + 2]
            + a[base + 3] * b[base + 3];
    }

    let base = chunks * 4;
    for i in 0..remainder {
        sum += a[base + i] * b[base + i];
    }

    sum
}

/// Normalize a vector to unit length.
pub fn normalize(v: &[f32]) -> Vec<f32> {
    let n = dot_product(v, v).sqrt();
    if n == 0.0 {
        return v.to_vec();
    }
    v.iter().map(|x| x / n).collect()
}

/// Convert a raw distance to a similarity score (higher = more relevant).
#[inline]
pub fn distance_to_score(metric: &str, distance: f32) -> f32 {
    match metric {
        "cosine" => 1.0 - distance,
        "l2" => 1.0 / (1.0 + distance),
        "dot" => -distance,
        _ => 0.0,
    }
}

/// Get the distance function for a metric name.
pub fn get_distance_fn(metric: &str) -> fn(&[f32], &[f32]) -> f32 {
    match metric {
        "cosine" => cosine_distance,
        "l2" => l2_distance,
        "dot" => dot_distance,
        _ => l2_distance,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cosine_identical() {
        let a = normalize(&[1.0, 0.0, 0.0, 0.0]);
        let d = cosine_distance(&a, &a);
        assert!((d - 0.0).abs() < 1e-6);
    }

    #[test]
    fn test_cosine_orthogonal() {
        let a = normalize(&[1.0, 0.0, 0.0, 0.0]);
        let b = normalize(&[0.0, 1.0, 0.0, 0.0]);
        let d = cosine_distance(&a, &b);
        assert!((d - 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_l2_identical() {
        let a = [1.0, 2.0, 3.0, 4.0];
        let d = l2_distance(&a, &a);
        assert!((d - 0.0).abs() < 1e-6);
    }

    #[test]
    fn test_l2_known() {
        let a = [0.0, 0.0, 0.0, 0.0];
        let b = [3.0, 4.0, 0.0, 0.0];
        let d = l2_distance(&a, &b);
        assert!((d - 5.0).abs() < 1e-6);
    }

    #[test]
    fn test_dot_product_basic() {
        let a = [1.0, 2.0, 3.0, 4.0];
        let b = [4.0, 3.0, 2.0, 1.0];
        let dp = dot_product(&a, &b);
        assert!((dp - 20.0).abs() < 1e-6);
    }

    #[test]
    fn test_normalize() {
        let v = normalize(&[3.0, 4.0, 0.0, 0.0]);
        let n = dot_product(&v, &v).sqrt();
        assert!((n - 1.0).abs() < 1e-6);
    }
}

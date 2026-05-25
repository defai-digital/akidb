//! SHA-256 checksum utilities for segment integrity validation.

use sha2::{Digest, Sha256};

/// Length of a SHA-256 digest in bytes.
pub const SHA256_BYTES: usize = 32;

/// Compute the SHA-256 digest of the given data. Returns a 32-byte array.
pub fn compute_checksum(data: &[u8]) -> [u8; SHA256_BYTES] {
    let mut hasher = Sha256::new();
    hasher.update(data);
    let result = hasher.finalize();
    let mut out = [0u8; SHA256_BYTES];
    out.copy_from_slice(&result);
    out
}

/// Return the hex-encoded SHA-256 digest of the given data.
pub fn checksum_hex(data: &[u8]) -> String {
    let hash = compute_checksum(data);
    hex::encode(&hash)
}

/// Simple hex encoding (avoids adding a `hex` crate dependency).
mod hex {
    pub fn encode(data: &[u8]) -> String {
        let mut s = String::with_capacity(data.len() * 2);
        for &b in data {
            s.push_str(&format!("{b:02x}"));
        }
        s
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex_encoding() {
        let hash = compute_checksum(b"test");
        let hex = checksum_hex(b"test");
        assert_eq!(hex.len(), 64);
        assert_eq!(hex, super::hex::encode(&hash));
    }
}

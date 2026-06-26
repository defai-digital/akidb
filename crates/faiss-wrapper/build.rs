//! Build script for FAISS wrapper
//!
//! The active build path is macOS Apple Silicon CPU/portable mode.

fn main() {
    #[cfg(feature = "gpu")]
    compile_error!("AkiDB supports macOS Apple Silicon CPU/portable builds only; the gpu feature is unsupported.");

    println!("cargo:rerun-if-changed=build.rs");
}

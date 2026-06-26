//! Build script for FAISS wrapper
//!
//! When the `gpu` feature is enabled, this compiles the C++ wrapper
//! and links against FAISS GPU libraries.

fn main() {
    println!("cargo:rerun-if-env-changed=CUDA_PATH");
    println!("cargo:rerun-if-env-changed=CUDA_HOME");
    println!("cargo:rerun-if-env-changed=FAISS_PATH");
    println!("cargo:rerun-if-env-changed=AKIDB_ALLOW_CUDA_ON_UNSUPPORTED_TARGET");

    // Only build C++ wrapper when gpu feature is enabled
    #[cfg(feature = "gpu")]
    build_gpu_wrapper();

    // Always output rerun-if-changed for the build script
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=cpp/faiss_wrapper.h");
    println!("cargo:rerun-if-changed=cpp/faiss_wrapper.cpp");
}

#[cfg(feature = "gpu")]
fn build_gpu_wrapper() {
    use std::env;
    use std::path::PathBuf;

    let target_os = env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    let target_arch = env::var("CARGO_CFG_TARGET_ARCH").unwrap_or_default();
    let allow_unsupported = env::var_os("AKIDB_ALLOW_CUDA_ON_UNSUPPORTED_TARGET").is_some();

    if target_os != "linux" && !allow_unsupported {
        panic!(
            "AkiDB gpu feature is supported only on Linux targets with NVIDIA CUDA. \
             Use --features cpu (or portable) on macOS Apple Silicon."
        );
    }

    if target_arch != "aarch64" && !allow_unsupported {
        println!(
            "cargo:warning=AkiDB gpu feature is intended for NVIDIA Jetson Thor \
             (aarch64 Linux). Building for {target_arch}-{target_os}; set \
             AKIDB_ALLOW_CUDA_ON_UNSUPPORTED_TARGET=1 to acknowledge this target."
        );
    }

    // Get CUDA path
    let cuda_path = env::var("CUDA_PATH")
        .or_else(|_| env::var("CUDA_HOME"))
        .unwrap_or_else(|_| {
            // Try common paths
            if PathBuf::from("/usr/local/cuda").exists() {
                "/usr/local/cuda".to_string()
            } else if PathBuf::from("/usr/local/cuda-13.0").exists() {
                "/usr/local/cuda-13.0".to_string()
            } else {
                panic!("CUDA not found. Set CUDA_PATH or CUDA_HOME environment variable.");
            }
        });

    // Get FAISS path
    let faiss_path = env::var("FAISS_PATH").unwrap_or_else(|_| {
        // Try common paths
        if PathBuf::from("/usr/local/lib/libfaiss.a").exists() {
            "/usr/local".to_string()
        } else if PathBuf::from("/opt/faiss").exists() {
            "/opt/faiss".to_string()
        } else {
            panic!("FAISS not found. Set FAISS_PATH environment variable.");
        }
    });

    // Build C++ wrapper
    cc::Build::new()
        .cpp(true)
        .file("cpp/faiss_wrapper.cpp")
        .include("cpp")
        .include(format!("{}/include", cuda_path))
        .include(format!("{}/include", faiss_path))
        .flag("-std=c++17")
        .flag("-O3")
        .flag("-fPIC")
        .compile("faiss_wrapper");

    // Link FAISS libraries (FAISS 1.8.0+ includes GPU in main library)
    println!("cargo:rustc-link-search=native={}/lib", faiss_path);
    println!("cargo:rustc-link-search=native={}/lib64", faiss_path);
    // Use dylib to link against shared library (libfaiss.so)
    println!("cargo:rustc-link-lib=dylib=faiss");

    // Link CUDA libraries
    println!("cargo:rustc-link-search={}/lib64", cuda_path);
    println!("cargo:rustc-link-lib=cudart");
    println!("cargo:rustc-link-lib=cublas");

    // Link C++ standard library
    println!("cargo:rustc-link-lib=stdc++");

    // OpenMP for parallel operations
    println!("cargo:rustc-link-lib=gomp");
}

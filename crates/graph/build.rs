fn main() {
    if std::env::var("CARGO_FEATURE_KUZU").is_err() {
        return;
    }

    println!("cargo:rerun-if-env-changed=CARGO_FEATURE_KUZU");
    // The kuzu crate builds the CXX bridge as a native archive that the final
    // graph test/binary link step must pull in explicitly.
    println!("cargo:rustc-link-lib=static=kuzu_rs");
}

fn main() {
    cbindgen::Builder::new()
        .with_crate(".")
        .with_language(cbindgen::Language::C)
        .generate()
        .expect("Unable to generate bindings")
        .write_to_file("iceberg_c.h");
}

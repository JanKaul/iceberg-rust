fn main() {
    cbindgen::Builder::new()
        .with_crate(".")
        .with_language(cbindgen::Language::C)
        .with_header(
            "typedef void* Relation;\ntypedef void* Table;\ntypedef void* TableTransaction;\ntypedef void* TableBuilder;\n"
                .to_owned(),
        )
        .generate()
        .expect("Unable to generate bindings")
        .write_to_file("iceberg_c.h");
}

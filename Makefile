test: test-iceberg-rust-spec test-iceberg-rust test-datafusion_iceberg

test-iceberg-rust-spec:
	cargo test -p iceberg-rust-spec --lib

test-iceberg-rust:
	cargo test -p iceberg-rust --lib

test-datafusion_iceberg:
	cargo test -p datafusion_iceberg --tests -j 2 && cargo clean -p datafusion_iceberg

test-rest-catalog:
	cargo test -p iceberg-rest-catalog --lib && cargo clean -p iceberg-rest-catalog

test-file-catalog:
	cargo test -p iceberg-file-catalog --lib && cargo clean -p iceberg-file-catalog

test-sql-catalog:
	cargo test -p iceberg-sql-catalog --lib && cargo clean -p iceberg-sql-catalog
clippy:
	cargo clippy --all-targets --all-features -- -D warnings
fmt:
	cargo fmt --all -- --check

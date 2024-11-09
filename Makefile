test: test-iceberg-rust-spec test-iceberg-rust test-datafusion_iceberg

test-iceberg-rust-spec:
	cargo test -p iceberg-rust-spec --lib

test-iceberg-rust:
	cargo test -p iceberg-rust --lib

test-datafusion_iceberg:
	cargo test -p datafusion_iceberg --tests -j 2

clippy:
	cargo clippy --all-targets --all-features -- -D warnings

FROM rust:1.64-buster AS builder
COPY . .
RUN cargo build --release

FROM debian:buster-slim
COPY --from=builder ./target/release/iceberg_catalog_rest_rdbms_server ./target/release/iceberg_catalog_rest_rdbms_server
RUN apt update && apt install -y openssl
CMD ["/target/release/iceberg_catalog_rest_rdbms_server"]
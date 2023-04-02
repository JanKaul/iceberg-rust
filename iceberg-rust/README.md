# Iceberg-rust

Low level implementation of the Apache iceberg specification.

Featues:
- catalog trait as an interface to the different iceberg catalogs
- table structs to simplify the access to the iceberg table metadata
- table transactions to guarantee atomic changes to the iceberg table metadata
- view structs to define iceberg views
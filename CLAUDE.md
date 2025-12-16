# iceberg-rust: Design Principles & Patterns

This document provides design principles and coding patterns for the iceberg-rust project, optimized for AI coding assistants and human contributors. It documents existing patterns while suggesting improvements for future development.

## Project Architecture

**Layered Design:**
```
datafusion_iceberg (query engine integration)
        ↓
  iceberg-rust (table operations, catalogs)
        ↓
iceberg-rust-spec (pure specification types)
```

**Core Philosophy:** Deep modules with simple interfaces (John Ousterhout's "A Philosophy of Software Design")

### Deep vs Shallow Modules

**Deep Modules** = Powerful functionality + Simple interface
- **Best modules** hide significant complexity behind clean APIs
- **Goal:** Minimize interface size relative to implementation size (1:10+ ratio ideal)

**Example - Catalog Trait** (`iceberg-rust/src/catalog/mod.rs:46-64`):
```rust
#[async_trait::async_trait]
pub trait Catalog: Send + Sync + Debug {
    async fn load_tabular(self: Arc<Self>, identifier: &Identifier)
        -> Result<Tabular, Error>;
    // ~20 methods hiding 6 implementations with 5000+ lines total
    // Interface/Implementation ratio: 1:12+ ✓
}
```

**What this hides:** Connection pooling, metadata loading, object store interaction, format detection (table/view/materialized_view), caching, retry logic, error translation

**Shallow Modules to Avoid:**
- Many small methods that just wrap other calls
- Interfaces that expose internal complexity
- Documentation longer than implementation

## Trait Design Patterns

### When to Create Traits

**Decision Tree:**
```
Is it used by 3+ types? → YES → Consider trait
         ↓ NO
Does it hide significant complexity? → YES → Consider trait
         ↓ NO
Would From/Into/standard trait work? → YES → Use standard trait
         ↓ NO
         → Don't create trait, use generic functions or enum
```

### Guidelines

1. **Prefer Standard Traits:** Use `From`, `TryFrom`, `Into`, `Display`, `Debug`, `Error` over custom traits
2. **Interface/Implementation Ratio:** Aim for 1:10+ lines (interface:implementation)
3. **Required Bounds:** Always include `Send + Sync + Debug` for shared types
4. **Async Traits:** Use `#[async_trait]` for I/O operations
5. **Arc Receivers:** Use `Arc<Self>` for async trait methods needing shared ownership

**Example - Standard Traits in Action** (`iceberg-rust/src/catalog/create.rs:116`):
```rust
impl TryInto<TableMetadata> for CreateTable {
    type Error = Error;
    fn try_into(self) -> Result<TableMetadata, Self::Error> {
        // Validation and conversion logic (30+ lines)
        // Separates validation from construction
    }
}
```

### Documentation Standards for Traits

Every public trait method must document (`iceberg-rust/src/catalog/mod.rs:65-98`):
1. **Summary:** One-line behavior description
2. **Arguments:** Each parameter explained
3. **Returns:** Success case
4. **Errors:** All failure modes

## Builder Pattern & Configuration

### When to Use Builders

**Decision Tree:**
```
5+ fields OR complex optional config OR needs async setup
         → derive_builder
Otherwise
         → Regular struct with new()
```

### Pattern: derive_builder + async build()

**Example** (`iceberg-rust/src/catalog/create.rs:54-83`):
```rust
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Builder)]
#[builder(build_fn(name = "create", error = "Error"), setter(prefix = "with"))]
pub struct CreateTable {
    #[builder(setter(into))]
    pub name: String,  // Required, no default

    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(setter(into, strip_option), default)]
    pub location: Option<String>,  // Optional, explicit

    pub schema: Schema,  // Required

    #[builder(setter(strip_option), default)]
    pub partition_spec: Option<PartitionSpec>,  // Optional
}
```

### Builder Extension for Integration

**Pattern:** Custom async `build()` for catalog integration (`iceberg-rust/src/catalog/create.rs:85-114`):
```rust
impl CreateTableBuilder {
    pub async fn build(
        &mut self,
        namespace: &[String],
        catalog: Arc<dyn Catalog>,
    ) -> Result<Table, Error> {
        let identifier = Identifier::new(namespace, &self.name);
        catalog.clone().create_table(identifier, self.create()?).await
    }
}
```

**Why this works:**
- Builder handles field collection
- Extension handles integration (identifier creation, catalog call)
- Validation separated in `TryInto<TableMetadata>`
- Users see: `CreateTable::builder().with_name("x").with_schema(s).build(&ns, cat).await?`

### Builder Best Practices

1. **Use derive_builder:** Don't hand-roll builders
2. **Ergonomics:** Use `setter(into)` for `String` and common types
3. **Optional is Explicit:** Use `Option<T>` + `strip_option` for clarity
4. **Required Fields:** No defaults - force user to provide
5. **Validation Separate:** Use `TryInto` for validation logic
6. **Custom build():** Add async `build()` taking external dependencies

## Error Handling

### Pattern: Centralized Error Enum with thiserror

**Example** (`iceberg-rust/src/error.rs:8-95`):
```rust
#[derive(Error, Debug)]
pub enum Error {
    // Domain errors with context
    #[error("Column {0} not in schema {1}.")]
    Schema(String, String),  // What failed + why

    #[error("Feature {0} is not supported.")]
    NotSupported(String),

    // Wrapped external errors
    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),

    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),

    // Boxed large errors
    #[error(transparent)]
    Avro(Box<apache_avro::Error>),
}
```

### Guidelines

1. **Use thiserror:** Always derive `Error` trait, never hand-roll
2. **Contextual Messages:** Include what failed and why (column name + schema name)
3. **Transparent Wrapping:** Use `#[from]` for automatic conversion from external errors
4. **Box Large Errors:** `Box<T>` for errors >128 bytes to reduce enum size
5. **Bidirectional When Needed:** Implement `From<Error>` for external types (e.g., `ArrowError`)
6. **Group Related Failures:** One variant with parameter (e.g., `NotFound(String)`) vs many variants

### Error Flow Best Practice

```
Operation → Result<T, Error> with context
    ↓
Domain layer adds context
    ↓
Infrastructure errors wrapped transparently
```

## Functional Programming Patterns

### Prefer Iterators Over Loops

**Pattern 1: Iterator Chains** (`iceberg-rust/src/table/transaction/operation.rs:188-196`):
```rust
let new_datafile_iter = data_files.into_iter().map(|data_file| {
    ManifestEntry::builder()
        .with_format_version(table_metadata.format_version)
        .with_status(Status::Added)
        .with_data_file(data_file)
        .with_sequence_number(table_metadata.last_sequence_number + dsn_offset)
        .build()
        .map_err(Error::from)
});
```

**Benefits:**
- Lazy evaluation (only creates when consumed)
- Clear transformation pipeline
- Error handling inline with `map_err`
- No intermediate allocations until `collect()`

**Pattern 2: flat_map for Flattening** (`iceberg-rust/src/table/transaction/operation.rs:149-153`):
```rust
let all_files: Vec<DataFile> = sequence_groups
    .iter()
    .flat_map(|d| d.delete_files.iter().chain(d.data_files.iter()))
    .cloned()
    .collect();
```

**Pattern 3: Option/Result Combinators** (`iceberg-rust/src/catalog/create.rs:131-132`):
```rust
// Prefer this:
self.location.ok_or(Error::NotFound(format!("Location for table {}", self.name)))?

// Over this:
let location = match self.location {
    Some(loc) => loc,
    None => return Err(Error::NotFound(...)),
};
```

### Guidelines

1. **Use Iterator Methods:** `map`, `filter`, `flat_map`, `fold` over `for` loops
2. **Lazy When Possible:** Return `impl Iterator` for large transformations
3. **Combinators:** `ok_or`, `and_then`, `unwrap_or_default` for `Option`/`Result`
4. **Strategic collect():** Only use `.collect::<Vec<_>>()` when needed
5. **Chain Iterators:** Use `.chain()` instead of extending vecs

### When NOT to Use Iterators

- Complex state machines (use explicit loops)
- Performance-critical hot paths needing specific optimizations
- When mutation in place is clearer

## Async Patterns

### Pattern: async_trait for I/O

All catalog and I/O operations are async (`iceberg-rust/src/catalog/mod.rs:56-57`):
```rust
#[async_trait::async_trait]
pub trait Catalog: Send + Sync + Debug {
    async fn create_table(self: Arc<Self>, ...) -> Result<Table, Error>;
}
```

**Why `Arc<Self>`:**
- Catalog is shared across connections
- Methods need owned `self` for async execution
- Prevents lifetime issues in async contexts

### Pattern: Controlled Parallelism

**Parallel Async Operations:**
```rust
let manifests = stream::iter(manifest_entries)
    .map(|manifest| async move {
        object_store.get(&path).await
    })
    .buffer_unordered(10)  // 10 concurrent fetches
    .try_collect::<Vec<_>>()
    .await?;
```

**Techniques:**
- `stream::iter` for converting to async stream
- `buffer_unordered(N)` for limiting parallelism
- `try_collect` for error-aware aggregation

### Pattern: Instrumentation

**All performance-critical paths** (`iceberg-rust/src/table/transaction/mod.rs`):
```rust
#[instrument(
    name = "iceberg_rust::table::transaction::commit",
    level = "debug",
    skip(self),
    fields(table_identifier = %self.table.identifier)
)]
pub async fn commit(self) -> Result<(), Error> { ... }
```

### Guidelines

1. **async_trait Required:** For async methods in traits
2. **Send + Sync Bounds:** All async types crossing await points
3. **Arc for Shared State:** Prefer `Arc` over lifetimes in async
4. **Instrument Hot Paths:** Use `#[instrument]` on catalog/I/O operations
5. **Limit Concurrency:** `buffer_unordered(N)` to prevent resource exhaustion
6. **Tokio Runtime:** All integration tests use `#[tokio::main]`

## Module Organization

### Principle: Minimize Dependencies Through Layering

**Dependency Graph:**
```
datafusion_iceberg → iceberg-rust → iceberg-rust-spec
     (DataFusion)    (async, I/O)    (pure data, serde)
```

**Why This Works:**
- **Spec layer:** No external dependencies except serde/uuid
- **Implementation layer:** Adds object_store, async, catalogs
- **Integration layer:** Adds datafusion-specific code

### Pattern: Re-export for Clean APIs

**Example** (`iceberg-rust/src/catalog/mod.rs`):
```rust
pub mod identifier {
    pub use iceberg_rust_spec::identifier::Identifier;
}
```

**Benefits:**
- Users import from one crate: `use iceberg_rust::catalog::identifier::Identifier`
- Spec types remain separate internally
- Clear public API surface

### Information Hiding Checklist

- Is this type part of public API? → Re-export
- Is this complexity internal? → `pub(crate)` or private mod
- Can spec types be separate? → Move to iceberg-rust-spec
- Does this add dependencies? → Check layer appropriateness

## Documentation Standards

### Pattern: Comprehensive Module Documentation

**Every `mod.rs` starts with** (`iceberg-rust/src/catalog/mod.rs:1-23`):
```rust
//! Catalog module providing interfaces for managing Iceberg tables and metadata.
//!
//! The catalog system manages:
//! - Table metadata and schemas
//! - Namespace organization
//! - Storage locations
//!
//! # Key Components
//! - [`Catalog`]: Core trait...
//!
//! # Common Operations
//! - Creating and managing tables
```

### Public API Documentation Structure

1. **One-line summary** (what it does)
2. **Arguments section** (each parameter)
3. **Returns section** (success case)
4. **Errors section** (all failure modes)
5. **Examples section** (when helpful)

### Guidelines

1. **Enforce Missing Docs:** Consider `#![deny(missing_docs)]` for public APIs
2. **Document Complex Private Functions:** When logic is non-obvious
3. **Module-Level Docs:** Every `mod.rs` has `//!` header
4. **Examples in Docs:** For builder patterns and complex APIs
5. **Errors Are Contract:** Always document failure modes

## Performance & Complexity Trade-offs

### Known Optimizations

1. **Manifest List Caching:** Prefetch with `buffer_unordered(10)`
2. **Lazy Iteration:** Return iterators, not Vecs when possible
3. **Arc Instead of Clone:** For metadata, catalogs, object stores
4. **Controlled Concurrency:** Limit parallel I/O to prevent resource exhaustion

### Complexity Management

**Good Complexity (Hidden):**
- Transaction system: 516 lines implementation, simple fluent interface
- Manifest writing: Complex partitioning hidden from users

**Bad Complexity (Avoid):**
- Exposing transaction operations publicly
- Forcing users to understand manifest structure
- Leaking object store details to catalog users

### Before Adding Complexity

Ask these questions:
1. Can it be hidden behind existing interface?
2. Does it reduce complexity elsewhere?
3. Is the interface/implementation ratio maintained?
4. Can derive macros handle it? (Builder, Getters, Error)

## Quick Reference: Decision Trees

### Adding a New Trait?
```
Is it used by 3+ types? → YES → Consider trait
         ↓ NO
Does it hide significant complexity? → YES → Consider trait
         ↓ NO
Would From/Into/standard trait work? → YES → Use standard trait
         ↓ NO
         → Use generic functions or enum
```

### Choosing Error Handling?
```
External library error? → #[error(transparent)] + #[from]
Domain-specific error? → Custom variant with context
Multiple failure modes? → Single variant with String parameter
Size > 128 bytes? → Box<T>
```

### Async or Sync?
```
I/O operation (network, disk)? → async
CPU-bound computation? → sync
Called from async context? → async
Part of Catalog/trait? → async
```

### Builder or Not?
```
5+ fields OR complex optional config OR needs async setup
         → derive_builder
Otherwise
         → Regular struct with new()
```

## Critical Metrics

These metrics indicate deep modules (desirable):

- **Catalog trait:** ~410 lines interface → ~5000+ lines across implementations (1:12 ratio) ✓
- **Transaction:** ~100 public lines → ~500 implementation lines (1:5 ratio) ✓
- **Error handling:** 107 lines covering all codebase errors (centralized) ✓

**Target:** Interface/Implementation ratio of 1:10+ for major abstractions

## Key Takeaways

1. **Deep Modules:** Hide complexity behind simple interfaces (Catalog trait: 20 methods, 6 implementations)
2. **Standard Traits First:** Prefer `From`/`TryFrom`/`Error` over custom traits
3. **Builder Pattern:** Use `derive_builder` for 5+ fields or complex config
4. **Functional Style:** Iterator chains over loops, combinators over match
5. **Error Context:** Use `thiserror` with contextual messages
6. **Async for I/O:** All catalog/storage operations async with `Arc<Self>`
7. **Layered Architecture:** Keep spec pure, implementation separate, integration isolated
8. **Document Everything:** Public APIs need comprehensive docs
9. **Pull Complexity Down:** Make user's life easy, hide complexity in implementation

## Design Philosophy Summary

When adding features, ask: **"Am I making the interface simpler or more complex?"**

The best additions hide complexity from users while maintaining clear, well-documented interfaces.

---

**Based on:** John Ousterhout's "A Philosophy of Software Design"
**References:** [Philosophy of Software Design Review](https://blog.pragmaticengineer.com/a-philosophy-of-software-design-review/), [Summary by Carsten Behrens](https://carstenbehrens.com/a-philosophy-of-software-design-summary/)

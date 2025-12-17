# iceberg-rust: Design Principles & Patterns

Actionable guidelines for the iceberg-rust project, optimized for AI coding assistants.

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

## Deep vs Shallow Modules

**Deep Modules** = Powerful functionality + Simple interface
- **Best modules** hide significant complexity behind clean APIs
- **Goal:** Minimize interface size relative to implementation size (1:10+ ratio ideal)
- **Example:** Catalog trait has ~20 methods hiding 6 implementations with 5000+ lines (1:12 ratio)

**Shallow Modules to Avoid:**
- Many small methods that just wrap other calls
- Interfaces that expose internal complexity
- Documentation longer than implementation

## LSP-Based Codebase Navigation

**IMPORTANT:** When an LSP (Language Server Protocol) MCP server is available (such as `rust-analyzer`), **ALWAYS prefer LSP tools over text-based search** for code navigation and analysis.

### When to Use LSP Tools

Use LSP tools for:
- **Finding definitions:** `get_symbol_definitions` instead of grepping for function/type names
- **Finding references:** `get_symbol_references` instead of searching for usage
- **Type information:** `get_hover` for accurate type and documentation
- **Code structure:** `get_symbols` for understanding module organization
- **Implementations:** `get_implementations` for finding trait implementations
- **Call hierarchy:** `get_call_hierarchy` for understanding call relationships
- **Diagnostics:** `get_diagnostics` for compiler errors and warnings
- **Completions:** `get_completions` for valid code suggestions

### Decision Tree

```
Need to understand code structure? → get_symbols
Need to find where something is defined? → get_symbol_definitions
Need to find all usages? → get_symbol_references
Need to understand types? → get_hover
Need to find trait impls? → get_implementations
Searching for text/patterns? → Grep/text search
```

## Functional Programming Patterns

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

### Documentation Standards

Every public trait method must document:
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

### Best Practices

1. **Use derive_builder:** Don't hand-roll builders
2. **Ergonomics:** Use `setter(into)` for `String` and common types
3. **Optional is Explicit:** Use `Option<T>` + `strip_option` for clarity
4. **Required Fields:** No defaults - force user to provide
5. **Validation Separate:** Use `TryInto` for validation logic
6. **Custom build():** Add async `build()` taking external dependencies

## Error Handling

### Guidelines

1. **Use thiserror:** Always derive `Error` trait, never hand-roll
2. **Contextual Messages:** Include what failed and why (column name + schema name)
3. **Transparent Wrapping:** Use `#[from]` for automatic conversion from external errors
4. **Box Large Errors:** `Box<T>` for errors >128 bytes to reduce enum size
5. **Bidirectional When Needed:** Implement `From<Error>` for external types (e.g., `ArrowError`)
6. **Group Related Failures:** One variant with parameter (e.g., `NotFound(String)`) vs many variants

## Module Organization

### Layering

**Dependency Graph:**
```
datafusion_iceberg → iceberg-rust → iceberg-rust-spec
     (DataFusion)    (async, I/O)    (pure data, serde)
```

- **Spec layer:** No external dependencies except serde/uuid
- **Implementation layer:** Adds object_store, async, catalogs
- **Integration layer:** Adds datafusion-specific code

### Information Hiding Checklist

- Is this type part of public API? → Re-export
- Is this complexity internal? → `pub(crate)` or private mod
- Can spec types be separate? → Move to iceberg-rust-spec
- Does this add dependencies? → Check layer appropriateness

## Complexity Management

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


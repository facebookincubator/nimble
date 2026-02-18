# Nimble — Columnar File Format

## Overview

Nimble (formerly "Alpha") is Meta's next-generation columnar file format. It is optimized for wide tables (thousands of columns) common in ML feature engineering and training pipelines.


## Architecture

Nimble has a layered architecture (bottom-up):

1. **Encodings** (`encodings/`) — Low-level encode/decode of typed value streams. 12 encoding types (Trivial, RLE, Dictionary, FixedBitWidth, Sentinel, Nullable, SparseBool, Varint, Delta, Constant, MainlyConstant, Prefix). Encodings are recursive/cascading and decoupled from the physical layout.
2. **Tablet** (`tablet/`) — Physical file layout. Manages stripes, footer/postscript (FlatBuffers), checksums (XXH3_64), and stream I/O. File structure: `[Stripe Data...][Metadata Groups][Footer][Postscript]`.
3. **Velox Integration** (`velox/`) — Maps between Velox vectors (`VectorPtr`) and Nimble's encoded streams. Provides `VeloxReader`/`VeloxWriter` and per-field type-specific `FieldReader`/`FieldWriter`.
4. **Selective Reader** (`velox/selective/`) —  Supports predicate pushdown and lazy column loading.
5. **Index** (`index/`) — Cluster index for stripe/chunk-level data skipping and point lookups.

## Testing

Tests live in `tests/` subdirectories alongside each component.

```bash
make clean

# Run all tests in a component
make unittest
```

## Coding Guidelines

Nimble follows the **Velox coding style** (see `https://github.com/facebookincubator/velox/blob/main/CODING_STYLE.md`) with these Nimble-specific additions:

### Naming

- **Do not abbreviate** names. Use full, descriptive names (`outputColumn` not `outputCol`, `selectivity` not `sel`).
- Exceptions: domain abbreviations (`id`, `sql`, `expr`), loop indices (`i`, `j`), iterators (`it`), established type aliases (`ExprCP`, `ColumnCP`), `numXxx` pattern.

### Comments

- `///` (triple-slash) only for **public API documentation** in headers.
- `//` (double-slash) for private/internal comments, implementation details, test code.
- Start comments with active verbs, not "This class..." or "This method..."
- Avoid redundant comments that repeat what the code says. Comments should explain **why**, not **what**.

### Naming Conventions (Velox standard)

- **PascalCase** for types and file names.
- **camelCase** for functions, member and local variables.
- **camelCase_** for private/protected member variables.
- **snake_case** for namespaces and build targets.
- **kPascalCase** for static constants and enumerators.

### Variable declarations
- **Declare variables as close as possible to their first use**.

### Usage of lambdas
- Use lambda if there are duplicate lines of code within a function.
- Use functions if duplicate lines of code are used across functions.

# Exasol Gateway Arrow Mapping

## Purpose

Exasol Gateway uses Apache Arrow IPC as the v1 tabular data format for metadata
and cursor result batches. Hot table-write batches use a native DMP-oriented
columnar format so adapters can approach storage-engine write performance
without Arrow IPC serialization/deserialization on every `InsertRows`.

The mapping should be aligned with the broader database Arrow design work. If
that design document defines a more precise type policy, it takes precedence
and this document should be updated to match it.

## Design Principles

| Principle | Description |
|---|---|
| Arrow where useful | Use Arrow for metadata and cursor results where a stable cross-system representation is valuable. |
| Native table-write hot path | Use an Exasol DMP-oriented columnar write format for `InsertRows` so adapters can avoid Arrow IPC overhead. |
| Explicit adapter ownership | Adapters map their local database values either to Arrow fetch consumers or directly to the native write buffers required by table operations. |
| No adapter-specific server conversions | The server should not know about MariaDB field buffers or PostgreSQL tuple/Datum layout. |
| Fail closed for unsupported types | Unsupported Exasol types should return `UNSUPPORTED_TYPE`, not lossy or implicit conversion. |
| Explicit semantics | Decimal, timestamp, string, and null handling must be specified rather than inferred. |

## Batch Structure

An Exasol Gateway cursor returns:

```text
OpenCursorResult
  cursor_id
  Arrow schema

FetchResult
  cursor_id
  Arrow record batch
  row_count
  end_of_cursor
```

A table insert operation sends the table/schema mapping once and then one or
more data batches through the open operation handle:

```text
OpenTableInsert
  target table
  selected columns
  max rows per batch
  accepted table/write metadata

InsertRows
  operation_id
  native DMP-oriented columnar batch
```

Recommended v1 constraints:

- one Arrow schema per cursor;
- one Arrow record batch per `FetchResult`;
- one native columnar DMP batch per `InsertRows` request;
- server-enforced max rows and max bytes per batch;
- no dictionary encoding in v1 unless explicitly approved by the Arrow design;
- no compressed IPC payloads in v1 unless the existing Arrow design already
  standardizes compression.

## Nullability

Exasol `NULL` values map to Arrow validity bitmaps.

Rules:

- nullable Exasol columns produce nullable Arrow fields;
- non-nullable Exasol columns may produce non-nullable Arrow fields;
- a NULL in a non-nullable target during `InsertRows` must fail with a
  structured conversion/constraint error;
- adapters must not infer nullability from observed batches only; they should
  use Arrow field nullability and metadata.

## Initial Type Mapping

The initial helper implementation uses Exasol DTM type identifiers plus column
metadata and maps them to Arrow schema fields. It intentionally stays
independent of MariaDB/PostgreSQL row formats and of ETL/Parquet internals.

| Exasol type family | Arrow type direction | Notes |
|---|---|---|
| BOOLEAN | `bool` | Direct mapping. |
| TINYINT / SMALLINT / INT | `int64` | The current helper deliberately widens these integer DTM IDs to one signed Arrow representation. |
| Unsigned-compatible decimal integer representations | `decimal128(p, 0)` or signed widened integer | Exasol integer semantics should drive this; avoid silent overflow. |
| DECIMAL(p,s) | `decimal128(p, s)` for supported precision | If Arrow runtime or adapter does not support required precision, return `UNSUPPORTED_TYPE`; `decimal256` is deferred. |
| BIGINT | `decimal128(36,0)` | Preserves the full Exasol `DTM_bigint` domain; it must never be narrowed to signed 64-bit. |
| DOUBLE / FLOAT / REAL | `float64` | The current helper maps all supported floating DTM IDs to Arrow `float64`. |
| CHAR / VARCHAR | `utf8` | v1 uses normal UTF-8 strings plus batch/value size limits; `large_utf8` is deferred until needed. |
| DATE | `date32` | Days since Unix epoch. |
| TIMESTAMP | `timestamp(ns)` with no timezone | Preserves Exasol nanosecond precision for timestamp-without-time-zone semantics. |
| TIMESTAMP WITH LOCAL TIME ZONE | `timestamp(ns, "UTC")` | Carries the stored instant in UTC; adapters convert between UTC and their authenticated session timezone at the database boundary. |
| INTERVAL types | defer or map only reviewed subset | Return `UNSUPPORTED_TYPE` until semantics are reviewed. |
| GEOMETRY / spatial | unsupported in v1 | Return `UNSUPPORTED_TYPE`. |
| Complex/nested types | unsupported in v1 | Return `UNSUPPORTED_TYPE`. |
| Binary strings | unsupported or `binary` only if explicitly needed | MariaDB prototype currently avoids binary string types; do not add silently. |

## Decimal Policy

Decimal mapping must preserve precision and scale.

Recommended v1 policy:

```text
Exasol DECIMAL(p, s)
  -> Arrow decimal128(p, s) if p <= decimal128 max precision and supported
  -> Arrow decimal256(p, s) only if runtime and adapter support are confirmed
  -> UNSUPPORTED_TYPE otherwise
```

Current helper policy:

- map `DTM_decimal` to `decimal128(p, s)` only for precision `1..38` and
  scale `0..precision`;
- map `DTM_bigint` losslessly to `decimal128(36,0)`;
- validate decimal metadata before calling Arrow constructors and reject invalid
  metadata with `UNSUPPORTED_TYPE`;
- keep `decimal256` and string compatibility modes out of v1 until a concrete
  adapter/product need is reviewed.

## Timestamp and Timezone Policy

Timestamp mapping must be explicit.

Current helper policy:

| Question | v1 helper direction |
|---|---|
| Timestamp unit | nanoseconds. |
| Timezone metadata | no timezone for timestamp-without-time-zone semantics. |
| Date mapping | `date32`. |
| Time-of-day / interval | unsupported until reviewed. |

`TIMESTAMP WITH LOCAL TIME ZONE` maps to timezone-qualified Arrow
`timestamp(ns, "UTC")`. The Arrow value is the canonical UTC instant; it does
not carry the source session's wall-clock representation. MariaDB maps
`TIMESTAMP(n)` to this type, converts its session-local value to UTC before a
native write, and converts UTC back through the reading MariaDB session's
`time_zone`. MariaDB `DATETIME(n)` remains mapped to timezone-free Exasol
`TIMESTAMP(n)`. Ambiguous/invalid local times and values outside Arrow's
signed-64-bit nanosecond range fail before mutation.

## String Policy

Potential choices:

| Choice | Pros | Cons |
|---|---|---|
| `utf8` | Widely supported by adapters; smaller offsets. | 32-bit offsets limit very large arrays/values. |
| `large_utf8` | Safer for large values/batches. | Some client libraries/adapters may require extra handling. |

Current helper policy:

- use `utf8`;
- enforce max batch bytes in protocol operations before `utf8` offsets can
  overflow;
- include Exasol declared character length metadata when available.

## Arrow Field Metadata

Arrow fields carry optional metadata useful to adapters without making adapters
depend on Exasol internals. The initial helper writes these keys when values
are available.

Potential metadata keys:

| Key | Purpose |
|---|---|
| `exasol.type_name` | Human-readable Exasol type name. |
| `exasol.type_id` | Stable SessionGW DTM identifier used for exact adapter schema validation. |
| `exasol.precision` | Decimal precision or type precision. |
| `exasol.scale` | Decimal scale. |
| `exasol.char_length` | Character length for string types. |
| `exasol.nullable` | Redundant with Arrow nullability, useful for debugging. |
| `exasol.column_name_original` | Original Exasol column name if Arrow field name is normalized. |

Metadata keys should be versioned or documented as stable before adapter code
relies on them.

## Table Write Validation

For native writes, the server validates the target Exasol table when the
operation is opened and validates each native columnar batch against the open
operation state.

Validation rules:

- field count and names/ordinals must match the request's target column list;
- fixed-size payload byte counts must match target Exasol DTM `maxSize()`;
- nulls must respect target nullability;
- decimal precision/scale must not overflow silently;
- string values must respect target length semantics;
- unsupported native/target type combinations fail with `UNSUPPORTED_TYPE`;
- conversion failures return a structured error, not partial silent conversion.

Partial success policy must be explicit. Recommended v1 policy:

```text
Each `InsertRows` request either succeeds for the whole batch or fails the
request. Transaction-level rollback/commit remains controlled by the session
transaction state.
```

## Batch Size and Memory Limits

The protocol should expose and enforce limits.

Recommended v1 limit categories:

| Limit | Purpose |
|---|---|
| max rows per batch | Prevent excessive adapter/server memory use. |
| max bytes per frame/batch | Prevent oversized frames and memory spikes. |
| max open cursors/operations per session | Prevent cursor and write-operation leaks. |
| max total cursor/operation memory per session | Prevent one adapter session from exhausting memory. |
| max string/binary value size | Keep Arrow offset and adapter behavior predictable. |

The server returns `RESOURCE_LIMIT` when a request exceeds limits.

## Unsupported Types

Unsupported types must fail explicitly.

For metadata:

```text
DescribeTable can either:
  - return the table with unsupported columns marked, if the adapter can decide;
  - or return UNSUPPORTED_TYPE for the table.
```

Recommended v1:

- `DescribeTable` should return enough information to explain unsupported
  columns if possible.
- `OpenTableScan` / `OpenPushedQuery` must fail if the result contains an
  unsupported type.
- `OpenTableInsert` must fail if the target column type is unsupported.

## Testing Requirements

Unit tests should cover:

- schema generation for each supported Exasol type;
- nullability mapping;
- decimal precision/scale boundaries;
- string values including empty strings and multi-byte UTF-8;
- date/timestamp representative values;
- unsupported type errors;
- table write open and `InsertRows` validation failures;
- batch size limit behavior.

Module/nano tests should cover:

- `DescribeTable` returns expected Arrow schema for a table with v1 supported
  types;
- `OpenPushedQuery` returns Arrow batches with expected values/nulls;
- `OpenTableScan` returns Arrow batches with expected values/nulls;
- `OpenTableInsert` plus repeated native-columnar `InsertRows` writes supported
  values and native Exasol SQL can read them back;
- unsupported types return stable error categories.

## Implemented Helper Scope

Initial table-write implementation note: the direct insert provider writes
native columnar batches through Exasol DMP write iterators rather than generated
SQL or Arrow IPC conversion. It currently requires all target-table columns in
table order and accepts the core table-write types needed by the first adapter
path. Unsupported native/target type combinations return `UNSUPPORTED_TYPE`.

The first helper implementation provides:

- `Column` metadata with Exasol DTM type identifiers to Arrow field/schema mapping;
- IPC schema serialization/deserialization that copies input into owned storage;
- IPC record-batch serialization/deserialization with caller-provided schema and
  owned input storage, so returned Arrow objects never borrow caller bytes;
- stable `UNSUPPORTED_TYPE` errors for unsupported DTM types or invalid metadata;
- unit tests for supported schema fields, metadata, unsupported types, schema
  IPC round trip, and record-batch IPC round trip.

## Open Questions

| Question | Owner / resolution path |
|---|---|
| Mapping from query/result metadata to `Column` descriptors carrying DTM type identifiers | Add during metadata/cursor implementation. |
| Maximum decimal precision in v1 | Resolved: Decimal128 precision `1..38`; `DTM_bigint` uses `decimal128(36,0)`. |
| Whether dictionary encoding is allowed | Defer unless Arrow design already requires it. |
| Whether IPC stream or standalone record batch is preferred per frame | Decide during protocol framing implementation; v1 should keep one schema per cursor and one batch per fetch. |
| Field metadata stability | Decide before MariaDB/PostgreSQL adapters depend on metadata keys. |

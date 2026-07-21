# Exasol Session Gateway Protocol v1

## Purpose

Session Gateway Protocol v1 defines a small, versioned protocol mode for a
normal authenticated Exasol session. It is intended for database adapters such
as a MariaDB storage engine or PostgreSQL FDW that need structured table,
cursor, batch, and transaction operations without embedding the Exasol runtime
inside the external database process.

The protocol is not a replacement for the normal Exasol SQL client protocol.
It is a negotiated capability for trusted adapters that already connect through
Exasol's existing TLS, authentication, session creation, and `exasql` process
assignment path.

The wire contract is designed for eventual MPP execution, but the currently
validated implementation is single-node and must reject unsupported multi-node
upgrades before acknowledging the mode switch. All-node routing requires a
separately funded MPP follow-on project; existing coordinator code is
preparatory scaffolding only.

## Design Principles

| Principle | Consequence |
|---|---|
| Reuse existing Exasol session lifecycle | SessionGW starts after normal authentication and session creation. |
| Preserve MPP execution | Operations run inside a normal Exasol session executed by the standard `exasql` worker group. |
| Keep v1 single-connection | The client uses one protocol connection; optional node-local data channels are deferred. |
| Use Arrow IPC where it fits | Metadata and cursor result batches use Arrow schemas/record batches. Hot table-write batches use a native DMP-oriented columnar format to avoid avoidable serialization/deserialization. |
| Keep adapters responsible for local conversion | MariaDB/PostgreSQL value conversion happens in adapter clients, not in Exasol server code. |
| Fail closed | Unsupported protocol versions, message types, table types, and operations return structured errors. |

## Protocol Lifecycle

```text
normal Exasol connection
        │
        │ existing TLS/authentication/session creation
        ▼
normal Exasol session
        │
        │ SessionGW capability request
        ▼
SessionGW protocol mode
        │
        ├─ metadata operations
        ├─ cursor operations
        ├─ Arrow fetch operations and native table-write batches
        ├─ transaction operations
        └─ close / cleanup
```

The initial integration point is described in `integration_point.md`: after
normal Exasol login, the client sends an after-login upgrade request that
switches the connection to SessionGW framing. This keeps the upgrade
protocol-level rather than SQL-level while preserving the existing
authentication and session creation path.

The preferred public SDK carrier is Exasol WebSocket API v5:

```json
{
  "command": "enterSessionGateway",
  "protocolVersion": 1
}
```

The server validates upgrade eligibility before the JSON acknowledgement. A
rejected upgrade returns a structured command-level error and then closes the
connection; it never acknowledges the upgrade and subsequently rejects through
`SGW1`.

After the JSON acknowledgement, each WebSocket binary message payload is exactly
one complete `SGW1` frame. Fragmented, truncated, concatenated, text, and
oversized carrier messages are protocol errors and close the connection.

For native binary protocol clients, the equivalent initial command byte is
`ENTER_SESSION_GATEWAY = 127`, followed by raw `SGW1` frames on the socket.

Neither carrier changes or bumps the existing Exasol SQL protocol version.
The existing protocol/WebSocket API is used only for normal connection,
authentication, session creation, and the upgrade acknowledgement. SessionGW
versioning starts after that point in the `SGW1` frame header and `Hello`
negotiation.

## Framing Model

The exact binary layout should be finalized during implementation, but v1
should follow this logical frame structure:

```text
FrameHeader
  magic/version marker
  message_type
  request_id
  flags
  payload_length

payload
```

The physical carrier defines frame boundaries:

- WebSocket carrier: one WebSocket binary message contains exactly one complete
  `SGW1` frame; WebSocket fragmentation and multiple SGW1 frames per message are
  not supported.
- Native carrier: the raw socket byte stream contains consecutive `SGW1`
  frames; `payload_length` defines each frame boundary.

### Request IDs

Every client request carries a `request_id`. Every response carries the same
`request_id`.

v1 processes requests sequentially. The request ID makes error reporting and
unknown-outcome diagnostics deterministic, but it is not an idempotency key:
clients must not replay a mutation completion or `Commit` after losing its
response.

### Message Size and Backpressure

The server should enforce configurable limits:

- maximum frame size;
- maximum Arrow batch bytes;
- maximum rows per batch;
- maximum open cursors and table operations per session;
- maximum outstanding request bytes.

Cursor reads are pull-based. `Fetch` requests only the next sequential batch;
`FetchPositionedRows` requests an explicit vector of logical row handles without
observing or advancing sequential cursor progress. Both operations accept byte limits.

## Capability Negotiation

Minimum v1 capabilities:

| Capability | Meaning |
|---|---|
| `SESSION_GW_V1` | Basic SessionGW framing, request/response, errors, and lifecycle. |
| `ARROW_IPC_BATCH_V1` | Arrow IPC schema and record-batch payloads for metadata and cursor fetch results. |
| `PUSHED_QUERY_V1` | `OpenPushedQuery`, `Fetch`, `CloseCursor`. |
| `METADATA_V1` | `DescribeTable`, `GetTableVersion`; optional list operations. |
| `TABLE_SCAN_V1` | `OpenTableScan`, sequential `Fetch`, out-of-band `FetchPositionedRows`, `CloseCursor`. |
| `TABLE_WRITE_V1` | Stateful table write handles: `OpenTableInsert`, repeated native columnar `InsertRows`, and `CloseOperation`. |
| `TRANSACTION_CONTROL_V1` | `SetAutocommit`, `Commit`, `Rollback`. |
| `NATIVE_TABLE_WRITE_BATCH_V2` | ABI-described, bounds-checked native DMP table-write batches; advertised only with a v2-capable provider. Numeric value 12. |
| `TABLE_UPDATE_V1` | `OpenTableUpdate`/`UpdateRows` using provenance-validated logical row handles and direct `UpdateDMP`. |
| `TABLE_DELETE_V1` | `OpenTableDelete`/`DeleteRows` using provenance-validated logical row handles and direct `DeleteDMP`. |

Reserved but not advertised by current v1 servers:

| Capability | Meaning |
|---|---|
| `OUTCOME_RECOVERY_V1` | Durable authenticated lookup/deduplication for completion and commit outcomes across reconnect. Numeric value 11 is reserved; current servers do not implement or advertise it. |

Future capabilities:

| Capability | Meaning |
|---|---|
| `PARALLEL_NODE_CHANNELS_V2` | Optional node-local data channels for high-throughput scans. |
| `DELEGATED_IDENTITY_V2` | Trusted adapter end-user delegation. |

## Stable Numeric Values

Every protocol-facing enum or ABI/API enum used by the SDK or wire protocol
must have:

- an explicit fixed-width underlying type;
- explicit numeric values for every enumerator;
- tests that assert the stable values for externally visible entries.

Names are used in this document for readability, but implementations must not
rely on compiler-assigned enum ordinals.

## Message Categories

### Session and Control

| Message | Direction | Purpose |
|---|---|---|
| `Hello` | client → server | Start SessionGW mode and present requested protocol version/capabilities. |
| `HelloOk` | server → client | Return accepted version, server capabilities, and limits. |
| `Ping` | client → server | Keepalive / connectivity check. |
| `Pong` | server → client | Keepalive response. |
| `Close` | client → server | Gracefully leave SessionGW mode and close cursors/session resources. |
| `Ok` | server → client | Generic successful completion for operations without specific response. |
| `Error` | server → client | Structured error response. |

### Metadata

| Message | Direction | Purpose |
|---|---|---|
| `DescribeTable` | client → server | Request schema and metadata for a table visible to the authenticated Exasol user. |
| `DescribeTableResult` | server → client | Return table metadata and Arrow schema. |
| `GetTableVersion` | client → server | Request stable table metadata version/signature. |
| `GetTableVersionResult` | server → client | Return table version/signature. |
| `ListSchemas` | client → server | Optional v1 operation for discovery. |
| `ListTables` | client → server | Optional v1 operation for discovery. |

### Cursor Operations

| Message | Direction | Purpose |
|---|---|---|
| `OpenPushedQuery` | client → server | Open a cursor for adapter-generated Exasol SQL. Stable message id: 12. |
| `OpenTableScan` | client → server | Open a cursor for direct table scan. Stable message id: 17. |
| `OpenCursorResult` | server → client | Return `cursor_id`, Arrow schema, and cursor capabilities. Stable message id: 13. |
| `Fetch` | client → server | Fetch only the next sequential Arrow record batch and advance cursor progress. Stable message id: 14. |
| `FetchResult` | server → client | Return Arrow record batch, aligned row handles when requested, and end-of-cursor flag. Stable message id: 15. |
| `CloseCursor` | client → server | Close cursor and release server resources. Stable message id: 16. |
| `FetchPositionedRows` | client → server | Read a non-empty vector of logical row handles from an open table cursor without changing its sequential progress. Stable message id: 30. |

Read cursors are already operation handles: `OpenPushedQuery` and
`OpenTableScan` establish server-side state, sequential `Fetch` and table-only
`FetchPositionedRows` repeat against that state, and `CloseCursor` releases it.
A positioned read shares the open table/projection context but has independent
request-owned row positions; it must not save, restore, or otherwise touch the
sequential next-row state. Write and positioned-DML messages use explicit table
operation terminology (`operation_id`, `CloseOperation`).

### Table Operation Handles and Writes

MariaDB handlers and PostgreSQL FDWs use an open / repeated operation / close
lifecycle. SessionGW table operations should mirror that model instead of
making each batch a standalone table operation. Opening a table operation owns
state such as resolved table metadata, selected columns, preallocated native
batch scratch, authorization checks, limits, coordinator state, and diagnostics.
Repeated data messages then carry only an operation id and batch payload.

| Message | Direction | Purpose |
|---|---|---|
| `OpenTableInsert` | client → server | Open a table-insert operation for a target table and column/schema mapping. Stable message id: 18. |
| `OpenTableOperationResult` | server → client | Return `operation_id`, accepted schema, and operation limits. Stable message id: 19. |
| `InsertRows` | client → server | Append one native DMP-oriented columnar batch to an open insert operation. Stable message id: 20. |
| `AffectedRowsResult` | server → client | Return number of inserted/updated/deleted rows for one data message or final operation stats. Stable message id: 21. |
| `CloseOperation` | client → server | Close a table operation and release server resources. Stable message id: 22. |
| `OpenTableUpdate` | client → server | Open an update handle scoped to logical row handles and selected update columns. Stable message id: 26. |
| `UpdateRows` | client → server | Update rows identified by logical row handles within an open update operation. Stable message id: 27. |
| `OpenTableDelete` | client → server | Open a delete handle scoped to logical row handles. Stable message id: 28. |
| `DeleteRows` | client → server | Delete rows identified by logical row handles within an open delete operation. Stable message id: 29. |

A one-shot `InsertBatch(schema, table, batch)` protocol operation is
intentionally not part of the product-shaped v1 plan. Adapters may still buffer
local rows into native columnar batches matching the target Exasol DTM layout,
but those batches must be sent through an open insert operation handle.

Update/delete require opaque row-handle semantics. They are deferred from the
initial read-only slice, but are now considered core gateway functionality that
must be designed and implemented before MariaDB/PostgreSQL adapter integration.

### Transactions

| Message | Direction | Purpose |
|---|---|---|
| `SetAutocommit` | client → server | Set autocommit behavior for the Exasol session. Stable message id: 23. |
| `Commit` | client → server | Commit current transaction. Stable message id: 24. |
| `Rollback` | client → server | Roll back current transaction. Stable message id: 25. |

## Operation Details

### DescribeTable

Request fields:

```text
schema_name
table_name
options
```

Response fields (in wire order):

```text
schema_name
table_name
table_version_or_signature
arrow_schema
```

Column names, types, nullability, and Exasol-specific precision, scale, and
character-length attributes are carried by the Arrow schema and its field
metadata. When Exasol has a current global row-count statistic, the schema-level
metadata key `sessiongw.table.row_count` contains its unsigned decimal value.
The key is optional, is a planning estimate rather than a transaction snapshot,
and does not contribute to `table_version_or_signature`. Embedding it in Arrow
schema metadata preserves the v1 response framing for existing consumers.

Authorization:

- Must obey normal Exasol metadata visibility and table privileges.
- Unauthorized objects must not leak more metadata than the normal Exasol
  session would expose.

### GetTableVersion

Returns a stable version/signature containing the schema-table object identity
and metadata version. It changes when the table is replaced or its externally
visible shape changes, but not when rows are inserted, updated, or deleted.

Adapters use this to invalidate local table definitions.

### OpenPushedQuery

Request fields:

```text
sql_text
optional_expected_arrow_schema
options
```

The client is a trusted adapter that has already validated whether the source
query shape is safe for pushdown. The server still executes the SQL as the
normal authenticated Exasol user, so Exasol permissions and SQL semantics
apply.

Implementation note: real SQL execution must be coordinated with the normal
Exasol session worker group. The initial SessionGW handler owns only the root
client socket, so it must not directly call normal SQL execution from node 0
while non-root nodes wait at a barrier. A pushed-query provider may advertise
`PUSHED_QUERY_V1` only after it enters a cluster-coordinated execution path
that cannot deadlock the session communicator.

Response fields:

```text
cursor_id
arrow_schema
cursor_limits
```

### OpenTableScan

Request fields:

```text
schema_name
table_name
selected_columns[]
options
```

v1 options should remain small:

- selected columns;
- optional batch-size hints;
- no general predicate language unless it can reuse safe existing expression
  handling.

Response fields:

```text
cursor_id
arrow_schema
cursor_limits
```

### Fetch

Request fields:

```text
cursor_id: u64
max_rows: u32       # 0 selects the server default
max_bytes: u32      # 0 selects the server default
```

`Fetch` is a pure sequential-next-batch operation. It contains no position vector
or mode flag and advances the cursor only by rows actually returned. For table scans,
a nonzero `max_bytes` is a strict raw table-read ceiling. If even the first row
exceeds it, the server returns `RESOURCE_LIMIT` and leaves all sequential progress
unchanged. If a bounded read returns a valid prefix, segment and row progress commit
only that prefix; a following `Fetch` resumes at the first row not returned.

Response fields:

```text
cursor_id
arrow_record_batch
row_count
end_of_cursor
row_handles_optional
```

`end_of_cursor=true` means the cursor is exhausted. The client should still
send `CloseCursor` unless the protocol explicitly defines auto-close on EOF.
The v1 recommendation is explicit close.

### FetchPositionedRows

Request fields:

```text
cursor_id: u64
max_bytes: u32      # 0 selects the server default
row_handle_count: u32
row_handles[row_handle_count]: u64
```

`row_handle_count` must be nonzero and within the negotiated row limit. Handles
are logical row numbers previously issued by this SessionGW session for the same
table version and transaction. They must be unique and ordered. The server
validates provenance collectively before any positioned-read collective begins.

The operation is an out-of-band read from the already-open table cursor. It uses
the cursor's table, selected-column, and Arrow-schema context but does not read,
advance, save, or restore sequential progress. Repeated size-one requests are
valid. A nonzero `max_bytes` is passed to the table reader as its raw result-size
ceiling. The response uses `FetchResult` with `end_of_cursor=false` because this
operation has no independent cursor position.

Positioned results are all-or-error: every requested handle must map to one row
in request order. The server rejects short reads instead of returning an
ambiguous compacted batch whose rows cannot be mapped safely back to handles.
Clients retain the same cursor lifecycle:

```text
OpenTableScan
  -> Fetch* / FetchPositionedRows*
  -> CloseCursor
```

Initial server provider constraints:

- `OpenPushedQuery` accepts only conservative read-only `SELECT` or `WITH` SQL
  text.
- The query must produce a result set.
- Result batches are encoded as Arrow IPC record batches.
- Unsupported Exasol column types or currently unsupported physical result
  conversions return structured SessionGW errors.

Direct table operations use the authenticated session's current user and active
roles. They require normal permission to use the schema (for example, schema
`USAGE` or `USE ANY SCHEMA`) plus the operation-specific object privilege (or
its corresponding system privilege):

| Request | Required object privilege | Accepted system privilege |
|---|---|---|
| `OpenTableScan` | `SELECT` | `SELECT ANY TABLE` |
| `OpenTableInsert` | `INSERT` | `INSERT ANY TABLE` |
| `OpenTableUpdate` | `UPDATE` | `UPDATE ANY TABLE` |
| `OpenTableDelete` | `DELETE` | `DELETE ANY TABLE` |

Table ownership satisfies the object-privilege check. A privilege for one
operation does not authorize any other operation. Missing privileges return
`NOT_AUTHORIZED` before a cursor or DMP operation is opened.

Authorization of mutation and discovery are intentionally separate. Opening an
update or delete operation checks only `UPDATE` or `DELETE`, respectively. The
normal high-level workflow must first discover transaction-scoped row locations
with `OpenTableScan`, which independently requires `SELECT`. Consequently an
end-to-end scan-then-update caller needs `SELECT` plus `UPDATE`, and an
end-to-end scan-then-delete caller needs `SELECT` plus `DELETE`. A caller that
already holds row locations issued earlier by the same still-active session and
unchanged transaction does not repeat discovery, but locations never survive
commit, rollback, reconnect, or transaction restart.

### OpenTableInsert / InsertRows / CloseOperation

Opening an insert operation is the write-side equivalent of opening a scan
cursor. It should be used by MariaDB `start_bulk_insert`/`write_row`/
`end_bulk_insert` and PostgreSQL `BeginForeignModify`/`ExecForeignInsert`/
`EndForeignModify` workflows.

`OpenTableInsert` request fields:

```text
schema_name
table_name
column_names[]
max_rows_per_batch
schema_optional_for_metadata
```

`OpenTableInsert` response fields:

```text
operation_id
accepted_schema_optional
limits
```

`InsertRows` request fields:

```text
operation_id
native_columnar_batch
```

`InsertRows` response fields:

```text
affected_rows
```

Native columnar batch v2 layout, inside the `native_columnar_batch` byte field:

```text
u32 magic = 'SGWB'                         // big-endian header integer
u32 version = 2
u32 row_count
u32 column_count
u32 alignment = 16
u32 variable_size_width = 8
u32 scalar_endianness                     // 1 little, 2 big
u32 scalar_abi = 1
for each opened column in table order:
  align to 16 with zero padding
  u8 null_vector[row_count]               // 0x00 not null, 0xff null
  align to 16 with zero padding
  if target column is fixed-size:
    byte fixed_data[row_count * target_type.maxSize()]  // Exasol DTM scalar ABI
  else if target column is variable-size:
    u64be sizes[row_count]
    u32 variable_data_bytes
    align to 16 with zero padding
    byte variable_data[variable_data_bytes]
final align to 16 with zero padding
```

Capability `NATIVE_TABLE_WRITE_BATCH_V2` is required; current servers do not
advertise the obsolete architecture-implicit v1 capability. V2 fixes alignment
and variable-length words on the wire, while explicitly describing the remaining
native fixed-scalar DTM ABI. A server rejects an alignment, width, endianness, or
scalar-ABI mismatch before opening a DMP iterator. It also validates every
multiplication, offset, padding byte, null byte, per-row size, and final extent
before DMP access. Adapters must still use table metadata to choose the correct
fixed-size encoder (`INTEGER` versus `BIGINT`, decimal storage size, boolean
storage, string semantics, etc.).

`CloseOperation` request fields:

```text
operation_id
```

`CloseOperation` releases operation resources and may return final operation
statistics. It must not implicitly commit the Exasol transaction; commit and
rollback remain explicit session-level transaction operations.

The native columnar batch must match the resolved target table columns and the
`max_rows_per_batch` accepted when the operation was opened. Constraint and
column-generation behavior in the initial direct provider is deliberately
fail-closed:

- `NOT NULL` is enforced from table metadata while parsing every insert or
  update batch, before that batch reaches a DMP iterator. A rejected batch
  CLEAN-aborts its operation, including any prefix accepted by earlier frames.
- Tables with `PRIMARY KEY`, `UNIQUE`, or `FOREIGN KEY` constraints, including
  tables referenced by a foreign key, are rejected at operation open with
  `UNSUPPORTED_OPERATION`; direct writes do not assume SQL-layer enforcement.
- Insert requires every table column in table order, so omitted-column,
  `DEFAULT`, and generated-value semantics are not exposed by protocol v1.
  Values are always explicit.
- The provider does not emulate SQL triggers or generated expressions. Only
  the lifecycle intrinsic to the table-level DMP pipeline is run; table shapes
  requiring additional SQL-layer semantics are not supported by direct v1
  operations.

Fixed-size columns
carry bytes in Exasol DTM in-memory layout and are passed directly to
`DMP::WriteIterator::write`. Variable-size columns carry fixed-width big-endian
`u64` lengths plus one contiguous data area; the server materializes the native
`size_t` length and pointer vectors required by DMP in preallocated operation
scratch. The initial
direct provider requires all target-table columns in table order so it can feed
the DMP insert iterator without inventing default-value semantics. The server
returns a structured error for format mismatch, unsupported target types,
constraint failures, authorization failures, stale operation ids, or transaction
failures.

### UpdateRows / DeleteRows

Update/delete expose logical row locations, not durable physical storage
addresses or registry tokens. A preceding scan/fetch can request row handles:

```text
Fetch(..., include_row_handles=true)
```

The response may then contain a row-handle vector aligned with the Arrow batch.
Each handle carries one 64-bit logical `row_number`; clients must treat it as a
transaction-scoped locator and must not derive node/local storage components.
Those handles can be used by later DML requests:

```text
UpdateRows(update_operation_id, row_handles, updated_values)
DeleteRows(delete_operation_id, row_handles)
```

The v1 row handle is a logical protocol locator carrying the table row number
needed by Exasol DBO/DMP APIs. Handles are scoped to the current
SessionGW session, transaction/snapshot, schema-table object version, and the
scan that issued the row number. The server records issued row numbers as a
fixed-capacity set of disjoint ranges, avoiding per-row heap allocation in the
scan and DML hot paths. Update, delete, and explicit positioned-read requests
must supply unique handles in ascending row-number order.

Handles remain usable after their producing cursor closes. A handle is consumed
by a successful update or delete and cannot be used by another data message.
All remaining handles are invalidated by commit, rollback, successful operation
close, session cleanup, or a table object-version mismatch. Unknown,
cross-session, cross-table, stale, and out-of-order handles are rejected instead
of being forwarded to DMP. The
current prototype supports handles returned by SessionGW table scans. Remaining
hardening includes:

- multi-node row-number and partition binding;
- conflict categorization for concurrent table changes;
- explicit compatibility guarantees for the fixed range-capacity limit.

## Payload Encoding

See `arrow_mapping.md` for type mapping details.

Recommended v1 layout:

- Cursor open response returns the Arrow schema once.
- Each fetch response contains one Arrow record batch matching that schema.
- Table write open requests carry table/column metadata and `max_rows_per_batch`.
- `InsertRows` carries one native DMP-oriented columnar batch, aligned for the
  target architecture and validated against the open operation.
- `UpdateRows` carries a vector of provenance-validated logical row handles plus
  one native value batch; `DeleteRows` carries a vector of the same logical row
  handles.
- Later versions can allow streaming multiple batches per frame if needed, but
  v1 should keep one batch per message.

## Error Response

Every operation can return `Error` instead of its normal response.

Logical fields:

```text
request_id
category
sql_state_optional
native_error_code_optional
message
details_optional
```

SessionGW v1 does not direct adapters to replay requests. An error terminates
the affected operation. Transaction conflicts are propagated to the host
database, which owns any complete-statement or transaction retry.

Stable categories:

| Category | Meaning |
|---|---|
| `PROTOCOL_ERROR` | Malformed frame, invalid state, unknown message, version mismatch. |
| `AUTHENTICATION_FAILED` | Existing session authentication failed before SessionGW mode. |
| `NOT_AUTHORIZED` | User lacks capability or table/operation privileges. |
| `OBJECT_NOT_FOUND` | Requested schema/table/cursor does not exist or is not visible. |
| `UNSUPPORTED_TYPE` | Table/result contains a type outside v1 Arrow mapping. |
| `UNSUPPORTED_OPERATION` | Operation is outside v1 protocol or disabled. |
| `TRANSACTION_CONFLICT` | Transaction conflict/deadlock/serialization-style failure. |
| `CONSTRAINT_VIOLATION` | Insert/write violates a constraint or conversion rule. |
| `RESOURCE_LIMIT` | Batch/cursor/session memory or count limit exceeded. |
| `CURSOR_NOT_FOUND` | Unknown or closed cursor id. |
| `INTERNAL_ERROR` | Unexpected server-side failure. |
| `TRANSPORT_ERROR` | Connection-level failure. |

Adapters should branch on `category`, not free-form text. The text remains
important for diagnostics.

### Completion outcomes after transport loss

`CloseOperation` may FINISH a DMP operation and autocommit before its `Ok`
response reaches the client. `Commit` has the same acknowledgement window. If
the client attempted either request but did not receive and validate its
correlated response, the outcome is `OUTCOME_UNKNOWN` at the client API boundary.
This is a client-local category, numeric value 13, and is never encoded as a
server `Error` frame.

Current v1 has no durable outcome registry and drops SessionGateway session
state after handler disconnect. The client must close the failed connection,
must not replay the completion, commit, or mutation payload, and must report the
unknown result to its caller. Reconnect starts a new session and cannot resolve
the old outcome. Clients requiring recoverable outcomes must require
`OUTCOME_RECOVERY_V1` and reject current servers because that capability is not
advertised.

## Native table read batch v1

`NATIVE_TABLE_READ_BATCH_V1` (capability 13) is an optional data encoding for
clients that already consume the stable C ABI. Arrow schema IPC is still sent
when the cursor opens, and ordinary `Fetch`/`FetchPositionedRows` continue to
return Arrow IPC. A client selects native data for that cursor by sending
`FetchNative` (31) or `FetchPositionedRowsNative` (33); both return
`FetchNativeResult` (32). Sequential and positioned requests retain their
existing progress, all-or-error, row-location, byte-limit, and cursor-lifetime
semantics. The first sequential or positioned fetch fixes that cursor's result
encoding; later requests using the other encoding fail with `PROTOCOL_ERROR`.
This keeps sequential and positioned batches symmetric without changing cursor
progress.

The result envelope is the normal cursor id, end flag, length-prefixed batch,
and row-location vector. An empty batch denotes no rows. A non-empty batch is
pointer-free and has this big-endian framing:

```text
u32 magic = 0x53475242 ("SGRB")
u32 version = 1
u32 row_count
u32 column_count
u32 scalar_encoding = 1 (little-endian)
u32 scalar_abi = 1
repeat column_count:
  u8 kind; u8 reserved[3] = 0
  i32 scale (encoded as u32; zero except Decimal128)
  u32 buffer_count (2 fixed-width, 3 UTF-8)
  u32 null_size = row_count
  u8 nulls[null_size]       # 0x00=value, 0xff=NULL
  fixed-width:
    u32 data_size = row_count * physical_width
    u8 data[data_size]
  UTF-8:
    u32 sizes_size = row_count * 8
    u64be sizes[row_count]  # NULL size must be zero
    u32 data_size = sum(sizes)
    u8 data[data_size]
```

Kinds and physical widths are: Boolean `1`/1 byte (`0` or `1`), Int64 `2`/8,
Double `3`/8 IEEE-754, Decimal128 `4`/16, Date32 `5`/4 Unix-epoch days,
TimestampNs `6`/8 signed Unix-epoch nanoseconds, UTF-8 `7`/variable,
Decimal32 `8`/4, and Decimal64 `9`/8. Decimal values are signed two's-complement
unscaled integers; scale remains schema-checked. Fixed-width scalars are little-endian. Sizes and all
framing integers are big-endian. Timestamp timezone metadata remains in the
cursor's Arrow schema: no timezone means wall-clock `DATETIME`; `UTC` means an
instant converted through the MariaDB session timezone.

Receivers must reject unknown kinds/ABI values, nonzero reserved bytes, schema
kind or scale mismatches, invalid NULL/Boolean bytes, arithmetic overflow,
truncation, trailing data, and any extent not exactly implied by row count.
Borrowed C views remain valid only until their native-fetch owner is destroyed.
Raw EXASOL DTM objects, pointers, and build-dependent layouts never cross the
wire.

## Session State and Cleanup

Server responsibilities:

- close all SessionGW cursors and invalidate every readable reference and
  issued row location before `Commit` or `Rollback` completes;
- reject `Commit` when a table operation is still open, CLEAN-abort the
  operation, and roll back the transaction because CLEAN alone cannot retract
  every batch already applied to that transaction; `Rollback` CLEAN-aborts
  every still-open operation and succeeds;
- reject the resulting cursor ids, operation ids, and row locations in the next
  transaction;
- close all SessionGW cursors and CLEAN-abort table operation handles on
  `Close`, disconnect, session reset, or transport cleanup;
- roll back or clean up according to normal Exasol session semantics on
  disconnect with an open transaction;
- release Arrow buffers, cursor memory, and write-operation state promptly;
- enforce per-session cursor/operation and memory limits;
- keep normal SQL clients unaffected.

Client responsibilities:

- close cursors and table operation handles explicitly;
- map local database transaction lifecycle to `Commit`/`Rollback` or
  autocommit behavior, and discard all cursor, operation, and row-location
  state after every transaction boundary; unsupported local features such as
  savepoints must fail closed, and an adapter that cannot map its advertised
  statement, transaction, and disconnect semantics must remain
  non-transactional rather than expose partial support;
- convert Arrow batches to local database representation;
- treat protocol errors as connection/session errors unless documented
  otherwise.

## Version 1 Testing Expectations

Every implementation step should include:

- unit tests for message/frame helpers and state machines;
- unit tests for Arrow schema/batch mapping;
- module tests using Exasol nano and a protocol simulation client;
- negative tests for malformed messages, unsupported versions, unsupported
  types, missing objects, and cursor cleanup.

The simulation client should become the reusable test fixture for all
SessionGW module tests.

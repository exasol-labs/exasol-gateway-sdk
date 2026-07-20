# Exasol Gateway SDK

## Purpose

`sessiongw-sdk` is the client-side SDK for integrating external database
adapters with Exasol Gateway. The first target language is
C++, because the immediate adapter targets, MariaDB storage engines and
PostgreSQL FDWs, are native C/C++ extensions.

The SDK is designed for open-source packaging, subject to normal product and
legal review. It allows adapter authors to use Exasol Gateway without
reimplementing Exasol login, SessionGW framing, capability negotiation, Arrow
IPC handling, cursor lifecycle, and error mapping in every adapter.

The SDK is not part of the Exasol server runtime. It is a client-side component
used by database-specific adapters and by the protocol simulation client used
in module tests.

## Design Goals

| Goal | Description |
|---|---|
| Reusable adapter foundation | MariaDB, PostgreSQL, and later adapters share the same SessionGW client implementation. |
| Stable public boundary | Expose a small C++ API independent of Exasol server internals. |
| Open-source ready | Avoid depending on private Exasol build/runtime details where possible; keep packaging boundaries explicit. |
| Arrow/native payloads | Expose Arrow schemas and record batches for reads, and native DMP-oriented buffers for table-write hot paths. |
| Fail-closed pushdown helpers | Provide reusable Exasol SQL construction helpers and unsupported-reason handling, but keep source-database AST traversal adapter-specific. |
| Test reuse | Use the SDK as the basis for the protocol simulation client and nano/module tests. |

## Non-Goals

| Non-goal | Reason |
|---|---|
| Server-side SessionGW implementation | The SDK is client-side only. |
| MariaDB/PostgreSQL value materialization | Adapters must convert Arrow values into their own row/tuple formats. |
| Parsing every source database AST | MariaDB/PostgreSQL ASTs are different and should be traversed by adapter-specific code. |
| Replacing Exasol official clients | The SDK is specialized for SessionGW table/cursor/batch operations. |
| Arrow Flight SQL implementation | The SDK may align with future Arrow Flight SQL work, but SessionGW v1 uses its own protocol mode and Arrow IPC payloads. |

## High-Level Shape

```text
MariaDB storage engine / PostgreSQL FDW / other adapter
        │
        ▼
sessiongw-sdk
  - Exasol WebSocket API v5 connection and authentication
  - SessionGW protocol upgrade after login
  - SessionGW frame encode/decode
  - capability negotiation
  - metadata/cursor/transaction API
  - Arrow IPC schema and record-batch access for metadata/cursor results
  - native columnar table-write batch builders for storage-engine-level writes
  - structured error mapping
  - pushdown SQL builder helpers
        │
        ▼
Exasol SessionGateway v1 protocol mode
        │
        ▼
normal Exasol session / exasql worker group
```

## Public Package Layout

`SessionGatewaySdk` is independently buildable and installable. Normal consumers
include only the umbrella header; low-level framing and WebSocket headers remain
available for protocol tools but are not needed by adapters.

```text
SessionGatewaySdk/
  CMakeLists.txt
  cmake/SessionGatewaySdkConfig.cmake.in
  include/sessiongw/
    sessiongw.hpp                 # public umbrella
    session.hpp                   # typed high-level API
    native_write_batch.hpp
    capabilities.hpp
    errors.hpp
    frame.hpp                     # low-level protocol tools
    websocket.hpp                 # low-level transport tools
  src/
  examples/public_consumer.cpp
```

The preferred installed CMake target is `ExasolGateway::Sdk`. The compatible
`SessionGateway::Sdk` target remains available and resolves to the same library;
see [Naming and Compatibility](branding.md). The typed C++ facade has a
public Apache Arrow dependency because metadata and cursor results expose
`arrow::Schema` and `arrow::RecordBatch`. OpenSSL and SGW1/WebSocket remain
implementation details.

Native adapters that cannot adopt the SDK's C++ language or Arrow ABI use
`sessiongw/c_api.h`. This stable C boundary exposes opaque session, metadata,
cursor, fetch, and operation handles; structured thread-local errors; borrowed
Arrow IPC views; transaction and mutation calls; and transport statistics. No
C++ or server-private type crosses that boundary. Borrowed views remain valid
until their owning opaque handle is destroyed.

## Public High-Level API

Include `<sessiongw/sessiongw.hpp>`. `Session::connect()` performs WebSocket API
v5 authentication, the SessionGateway upgrade, `Hello`, capability negotiation,
and response correlation.

```cpp
sessiongw::WebSocketOptions options;
options.host = "db.example";
options.port = 8563;
options.user = "adapter_user";
options.password = "...";

sessiongw::Session session = sessiongw::Session::connect(options);
if (!session.capabilities().supports(
        sessiongw::Capability::native_table_write_batch_v2)) {
    throw std::runtime_error("native writes unavailable");
}
```

Verified TLS is the default. Certificate skipping and plaintext modes are
explicit test-only choices. Carrier framing, request IDs, peer close, and
`SIGPIPE` suppression are enforced below `Session`.

### Typed metadata and reads

```cpp
sessiongw::TableName table{"SALES", "ORDERS"};
sessiongw::TableMetadata metadata = session.describeTable(table);
std::shared_ptr<arrow::Schema> schema = metadata.schema;

sessiongw::Cursor cursor = session.openTableScan(
    table, {"ID", "CUSTOMER"}, true);
for (;;) {
    sessiongw::FetchBatch batch = session.fetch(cursor, 4096);
    // batch.rows is an owned Arrow RecordBatch; row_locations align with rows.
    if (batch.end_of_cursor) break;
}
session.closeCursor(cursor);
```

`openPushedQuery()` uses the same `Cursor`/`FetchBatch` lifecycle.
`fetchPositioned()` takes a vector of transaction-scoped `RowLocation` values
and does not advance sequential cursor progress.

### Native mutations

```cpp
session.setAutocommit(false);
sessiongw::TableOperation operation = session.openTableInsert(
    table, {"ID", "CUSTOMER"}, 1000, schema);

std::vector<std::uint8_t> bytes;
sessiongw::NativeWriteBatchBuilder batch(bytes);
batch.begin(row_count, 2);
batch.appendDecimal64Column(ids);           // spans of std::optional<int64_t>
batch.appendStringColumn(customers);        // spans of optional<string_view>
batch.finish();

session.insertRows(operation, bytes);       // repeat for each vector batch
session.closeOperation(operation);
session.commit();                           // explicit because autocommit is off
```

`openTableUpdate()` plus `updateRows()` and `openTableDelete()` plus
`deleteRows()` use row locations issued by a scan in the same session and
transaction. A lost correlated `CloseOperation` or `Commit` response is surfaced
as `OUTCOME_UNKNOWN`; callers must not replay it.

### Standalone build/install

```bash
cmake -S SessionGatewaySdk -B build/sessiongw-sdk \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_INSTALL_PREFIX="$PWD/install/sessiongw-sdk"
cmake --build build/sessiongw-sdk -j
cmake --install build/sessiongw-sdk
```

A separate consumer then uses:

```cmake
find_package(SessionGatewaySdk CONFIG REQUIRED)
target_link_libraries(my_adapter PRIVATE ExasolGateway::Sdk)
```

`examples/public_consumer.cpp` is a compile/link example that uses only installed
SDK and Arrow headers. Pure-C consumers include `sessiongw/c_api.h`; the header is
validated as C11 and links to the same Exasol Gateway SDK library. No
SessionGateway server-private header or library is part of either boundary.

## Error Model

The SDK should preserve the stable SessionGW error category and detailed
server diagnostics.

```cpp
try {
    auto desc = client.describeTable({"S", "T"});
} catch (const sessiongw::Error& e) {
    switch (e.category()) {
        case sessiongw::ErrorCategory::ObjectNotFound:
            // adapter maps to local table-not-found behavior
            break;
        case sessiongw::ErrorCategory::NotAuthorized:
            // adapter maps to permission denied
            break;
        default:
            // adapter-specific fallback
            break;
    }
}
```

The SDK should not hide server errors behind generic exceptions only. Adapter
code needs structured categories for MariaDB/PostgreSQL error mapping.

## Payload Handling

The SDK should expose Arrow data directly for metadata and cursor reads:

- Arrow schema for table descriptions and cursor results;
- Arrow record batches for fetch results;
- helper methods for IPC serialization/deserialization when useful for tests.

For table-write hot paths, the SDK exposes native DMP-oriented builders or
views instead of forcing adapters through Arrow IPC. `NativeWriteBatchBuilder`
writes the native batch into a caller-owned/reusable byte buffer, and can append
into an existing `InsertRows` payload buffer so the SDK does not need an extra
batch vector followed by a payload copy. V2 batches require capability
`NATIVE_TABLE_WRITE_BATCH_V2` and contain an explicit alignment/endianness/scalar
ABI descriptor, Exasol-layout fixed buffers, `0x00`/`0xff` null vectors,
big-endian `u64` length words for variable columns, zero padding, and contiguous
variable data areas. Public typed helpers cover BOOLEAN, 32/64/128-bit unscaled
DECIMAL values, DOUBLE, validated DATE/TIMESTAMP components, and strings while
constructing the required NULL vectors. ABI mismatch and malformed extents fail
before DMP access. WebSocket transport
still has to mask/copy into its reusable frame buffer; fully zero-copy network
send remains a future transport/writev-level optimization.

```text
Read path:
Exasol server -> Arrow IPC batch -> sessiongw-sdk Arrow object/wrapper -> adapter-specific conversion

Write path:
adapter row/column buffers -> sessiongw-sdk native table-write batch -> Exasol DMP::WriteIterator
```

## High-Level SDK Operation Mapping

The SDK should expose high-level table/cursor operations while keeping a direct
mapping to protocol commands:

| SDK operation | Protocol command sequence | Payload family | Status |
|---|---|---|---|
| `describeTable` | `DescribeTable` | Arrow schema metadata | implemented |
| `getTableVersion` | `GetTableVersion` | metadata version string | implemented |
| `openPushedQuery` / `fetch` / `closeCursor` | `OpenPushedQuery` → `Fetch`* → `CloseCursor` | Arrow IPC result batches | implemented |
| `openTableScan` / `fetch` / `closeCursor` | `OpenTableScan` → `Fetch`* → `CloseCursor` | direct table read, Arrow IPC result batches | implemented |
| `openTableInsert` / `insertRows` / `closeOperation` | `OpenTableInsert` → `InsertRows`* → `CloseOperation` | native DMP-oriented columnar write batches | implemented |
| `openTableUpdate` / `updateRows` / `closeOperation` | `OpenTableUpdate` → `UpdateRows`* → `CloseOperation` | native row-handle/value batch | prototype implemented |
| `openTableDelete` / `deleteRows` / `closeOperation` | `OpenTableDelete` → `DeleteRows`* → `CloseOperation` | native row-handle batch | prototype implemented |
| `setAutocommit` / `commit` / `rollback` | `SetAutocommit`, `Commit`, `Rollback` | transaction control | implemented |

`WebSocketOptions::client_name` sets the escaped Exasol login `clientName`
attribute. The additive C ABI entry point
`sessiongw_c_connect_with_client_name()` provides the same behavior without
changing the layout of `sessiongw_c_options`; the original connect function
uses the SDK default client name.

`TableMetadata::row_count` exposes the optional validated
`sessiongw.table.row_count` schema metadata. The stable C ABI provides
`sessiongw_c_metadata_has_row_count()` and `sessiongw_c_metadata_row_count()`;
absence is distinct from an empty table.

Update/delete message ids are part of the protocol enum for API stability. The
current prototype advertises update/delete capabilities when the table-operation
provider is available. Row handles are logical, provenance-validated locations
that are usable only in the authenticated session and unchanged transaction that
issued them; callers must not derive or guess them.

`Commit` and `Rollback` are hard SDK lifecycle boundaries. They invalidate every
open cursor and every issued row location. `Rollback` CLEAN-aborts open table
operations and returns `Ok`. `Commit` with an open table operation is rejected,
CLEAN-aborts the operation, and rolls the transaction back because CLEAN alone
cannot retract every batch already applied to the transaction. SDK callers must
close successful operations before commit and discard all corresponding handles
after either boundary.

If transport fails after sending `CloseOperation` or `Commit` but before the SDK
receives and validates the correlated response, the SDK reports the client-local
`OUTCOME_UNKNOWN` category. It closes the connection and never retries the
completion or any mutation payload. Current servers intentionally do not
advertise reserved capability `OUTCOME_RECOVERY_V1`; reconnect cannot query the
old session outcome. Applications that require recoverable rather than explicit
unknown outcomes must fail capability negotiation.

## Protocol Simulation Client

The simulation client should be built on top of the SDK API. It is not a
throwaway tool; it is the reusable black-box test client for SessionGW module
and nano tests.

Initial commands:

```text
sessiongw-sim ping
sessiongw-sim capabilities
sessiongw-sim describe --schema S --table T
sessiongw-sim pushed-query --sql 'SELECT ...'
sessiongw-sim scan --schema S --table T --column C1 --column C2 --include-row-handles
sessiongw-sim insert --schema S --table T --int32-column ID --string-column NAME --row 1,Alice --row 2,Bob
sessiongw-sim update --schema S --table T --row-handle ROW_NUMBER --string-column NAME --row Updated
sessiongw-sim delete --schema S --table T --row-handle ROW_NUMBER
```

Initial implementation can start with only:

```text
sessiongw-sim connect
sessiongw-sim ping
sessiongw-sim capabilities
```

and grow as server operations are implemented.

## Nano Test Harness Role

The nano/module tests should use the simulation client to validate the protocol
through the same external boundary that adapters will use.

Expected early test shape:

```text
start Exasol nano
        │
        ▼
sessiongw-sim connects and authenticates through WebSocket API v5
        │
        ▼
sessiongw-sim sends enterSessionGateway JSON command
        │
        ▼
sessiongw-sim sends SGW1 Ping as WebSocket binary message
        │
        ▼
sessiongw-sim receives SGW1 Pong and sends Close
```

Later tests reuse the same client for:

- `DescribeTable`;
- `OpenPushedQuery` / `Fetch` / `CloseCursor`;
- `OpenTableScan` / `Fetch` / `CloseCursor`;
- `OpenTableInsert` / repeated `InsertRows` / `CloseOperation`;
- `OpenTableUpdate` / `UpdateRows` / `CloseOperation`;
- `OpenTableDelete` / `DeleteRows` / `CloseOperation`;
- transaction control;
- negative protocol/error cases.

The current live nano scaffold runs a pushed-query type/edge matrix by default
when `SESSIONGW_HOST`, `SESSIONGW_PORT`, `SESSIONGW_USER`, and
`SESSIONGW_PASSWORD` are set. When `SESSIONGW_SCHEMA`, `SESSIONGW_TABLE`, and
`SESSIONGW_SCAN_COLUMNS` are set, it also opens a table-scan cursor and fetches
one batch using repeated `--column` scan arguments. When
`SESSIONGW_INSERT_SCHEMA` and `SESSIONGW_INSERT_TABLE` are set, it also opens
SessionGW table-insert operation handles, sends native columnar `InsertRows`
batches, and exercises autocommit, explicit commit, and rollback paths. Insert,
update, and delete are separate SDK operation families; delete is exposed as
`OpenTableDelete`/`DeleteRows`/`CloseOperation`, not folded into insert or a
generic mixed DML object. The pushed-query matrix
covers representative BOOLEAN,
integer/decimal/double, character/Unicode, DATE, TIMESTAMP, NULL,
empty-result, partial-fetch, max-rows-zero/max-bytes, and read-only-guard
cases. It also checks structured negative-path errors for fetch row/byte
resource limits and unsupported non-read-only SQL. Set `SESSIONGW_RUN_TYPE_MATRIX=0`
for a smoke-only live run.

A broader SDK integration workload is available as:

```bash
SessionGatewaySdk/test/nano/run_integration_workload.sh
```

The script extracts and starts `.build/exasol-nano-db-2026.2.0-nano.3-x86_64.run`,
compiles `SessionGatewaySdk/test/integration/sessiongw_sdk_integration_test.cpp`
with the host C++ compiler against the public SDK headers/sources, prepares a
nano table through `c4 sqlclient`, runs the SDK workload over WSS, verifies the
final database state through SQL, and writes a report to
`.build/sessiongw-sdk-integration/report.txt`.

The current workload covers:

- WebSocket API v5 login, `enterSessionGateway`, SGW1 `Hello`, `Ping`, and
  `Close`;
- `DescribeTable` and `GetTableVersion`;
- pushed-query cursor open/fetch/close;
- direct table scan with row handles;
- native table insert operation handles;
- explicit commit and rollback through SessionGW transaction frames;
- row-location update and delete operation handles;
- scan rows → update/delete by row location → scan visible rows again; the
  discovery scan requires `SELECT`, so the normal end-to-end update/delete
  workflows require `SELECT + UPDATE` or `SELECT + DELETE`, while the mutation
  operation itself checks only its corresponding mutation privilege;
- deleted-row filtering by verifying that deleted handles/keys disappear from
  subsequent scans and SQL checks;
- small seed table, typed matrix table, and configurable large table workload;
- a full native type matrix for all currently SessionGW-supported Arrow/native
  families: BOOLEAN, DECIMAL backed by small/int/big fixed layouts,
  scaled DECIMAL, DOUBLE, DATE, TIMESTAMP, CHAR, VARCHAR, and NULL values;
- multi-frame table operations on the large table: one `OpenTableInsert` handle
  receives multiple `InsertRows` batches, the table scan is consumed through
  repeated `Fetch` calls, one `OpenTableUpdate` handle receives multiple
  `UpdateRows` batches, and one `OpenTableDelete` handle receives multiple
  `DeleteRows` batches;
- deleted-row stress: by default the large table updates 10% of rows, deletes
  5% of rows, verifies that a stale handle cannot be reused by another session,
  rescans all visible row handles in multiple fetch batches, and SQL-verifies
  count, deleted-key absence, updated-row count, and checksum;
- negative DML lifecycle checks for structured errors and cleanup: unknown
  operation handle, `InsertRows` above `maxRowsPerBatch`, `InsertRows` after
  `CloseOperation`, and `UpdateRows` handle/value cardinality mismatch;
- transport-abort cleanup with an open cursor and with an unclosed insert
  operation after one `InsertRows` batch; the follow-up SQL check verifies the
  unclosed DMP operation was cleaned and did not commit the row;
- mixed-frontend writer conflicts: ordinary WebSocket SQL inserts and direct
  SessionGW inserts run concurrently against one table, each operation is sent
  exactly once, and every attempt must either commit one row or return a
  transaction conflict; final SQL verification requires the row count to equal
  the number of successful attempts;
- session interruption handling: a SessionGW query timeout must surface as an
  SDK exception while leaving the SessionGW connection usable for a follow-up
  `Ping`, and a separate admin WebSocket session can `KILL SESSION` for a
  SessionGW connection running `"$SLEEP"(...)`; the SDK must surface an
  exception and a fresh reconnect must succeed;
- SQL consistency checks for counts, committed/rolled-back rows, typed updated
  values, typed NULL row, rejected negative-DML writes, transport-aborted
  unclosed writes, deleted rows, large-table checksum, and large-table batched
  update/delete effects;
- session-close cleanup of an intentionally leaked cursor;
- multi-connection query stress (`SESSIONGW_ITEST_CONCURRENCY`, default `8`,
  times `SESSIONGW_ITEST_CONCURRENCY_ITERATIONS`, default `4`).

Performance instrumentation is disabled by default. Setting
`EXASOL_SESSIONGW_INSTRUMENTATION=1` enables bounded scalar counters and
monotonic-clock timers without per-row logging or retained request data. The
public SDK exposes `WebSocketConnection::statistics()` for connection,
transport read/write, WebSocket header/payload ingestion, SGW frame decoding,
SGW frame/payload, and WebSocket masking-copy totals. The stable C aggregate
remains available through `sessiongw_c_statistics_get()`; the additive
`sessiongw_c_transport_profile_get()` exposes the detailed transport phases. The
server emits one provider summary and one transport summary per SessionGW
session, covering protocol payload-decode count/parse time, pushed-query executor time,
direct-fetch/provider time, DMP time, rows, batches, native bytes, projection,
protocol-handler lifetime, and transport bytes/time. The MariaDB adapter emits one summary when a THD closes,
covering connection attempts/retries, metadata cache behavior, cursor/operation
lifecycle, projection, Arrow/native conversion, request round trips, rows,
batches, bytes, and structured transaction conflicts. The live SDK and MariaDB
workloads enable instrumentation and append these records to their reports.
Production TCP connections enable `TCP_NODELAY` so small cursor, operation, and
completion frames are not delayed behind Nagle buffering; TLS remains mandatory
unless the explicit test-only plain transport is selected. Transport read time
includes time waiting for the peer; server executor/DMP and
adapter conversion timers are reported separately so the values are not
misrepresented as isolated CPU measurements. Unset or set the variable to `0`
to keep instrumentation disabled.

The large-table workload defaults to `SESSIONGW_ITEST_LARGE_ROWS=100000`,
`SESSIONGW_ITEST_LARGE_INSERT_BATCH_ROWS=1000`,
`SESSIONGW_ITEST_LARGE_FETCH_BATCH_ROWS=777`,
`SESSIONGW_ITEST_LARGE_UPDATE_BATCH_ROWS=333`, and
`SESSIONGW_ITEST_LARGE_DELETE_BATCH_ROWS=257`. The timeout/kill coverage is a
real live server interruption test and can add roughly one minute to the run on
nano because Exasol timeout/kill handling for `"$SLEEP"(...)` is not
instantaneous. The workload can be raised to the
`rr.examariadb` large-scale default with, for example,
`SESSIONGW_ITEST_LARGE_ROWS=1000000`. Quick mode caps the effective large table
at 1000 rows while preserving multi-batch insert/fetch/update/delete behavior.

The workload takes its shape from the MariaDB prototype validation scripts,
especially the smoke, supported type matrix, large-scale, and multiuser stress
cases under `rr.examariadb:Server/test/mariadb/`. `rr.examariadb`'s
`test_mariadb_large_scale` defaults to 1,000,000 rows and 10,000-row SQL insert
batches; SessionGW now defaults to a smaller but still large direct-protocol
100,000-row nano workload and can be configured to the same million-row scale.
It exercises the same core adapter concerns directly through the public SDK
boundary instead of through MariaDB: metadata, typed DML, scans, row-handle
update/delete, rollback, deleted-row visibility, large-row batches, and
concurrent clients. The runner appends an explicit SessionGW-vs-rr.examariadb
coverage-gap report to `.build/sessiongw-sdk-integration/report.txt`.

The host-buildable integration test deliberately avoids Exasol internal runtime
dependencies. It uses SDK-owned native value encoders for the fixed-size DTM
layouts currently exposed by SessionGW and treats Arrow IPC schema/result bytes
as opaque transport payloads. DTM families that are not currently supported by
SessionGW Arrow mapping (`TIME`, intervals, geometry, timestamp UTC, hash, array)
are outside this suite until the server protocol and SDK expose them as supported
adapter-facing types.

Current pushed-query safety limits are intentionally conservative and bounded
for product use: SQL text is limited to 256 KiB, fetch requests are limited to
1,000,000 rows or 64 MiB, and each SessionGW session may have at most 128 open pushed-query cursors. Server observability
logs cursor open/fetch/close/cleanup events with cursor ids, counts, and row
counts, but not raw SQL text or secrets.

Live mode also runs a bounded concurrent session/cursor cleanup stress by
default. The stress launches `SESSIONGW_STRESS_SESSIONS` WSS sessions in
parallel for `SESSIONGW_STRESS_ITERATIONS` iterations. Each stress session
opens and fetches a pushed-query cursor, leaves it open, then sends SessionGW
`Close` so server-side close-all-cursors cleanup is exercised under concurrent
session shutdown. Defaults are 12 sessions and 3 iterations. Set
`SESSIONGW_RUN_STRESS=0` to skip this stress.

## Pushdown Translation Toolkit

The SDK can provide reusable helpers for adapter-side pushdown generation, but
it should not attempt to own source-database AST traversal.

### Boundary

```text
MariaDB AST / PostgreSQL planner tree / other source tree
        │
        │ adapter-specific visitor
        ▼
SDK Exasol SQL builder / small IR
        │
        │ reusable Exasol dialect emission
        ▼
Exasol SQL text for SessionGW OpenPushedQuery
```

### Suggested Helper Areas

| Area | SDK role |
|---|---|
| Identifier quoting | Quote Exasol identifiers safely and consistently. |
| Literal quoting | Quote strings, dates, timestamps, numbers, booleans, and NULL. |
| Type helpers | Exasol type names and cast syntax. |
| Expression builder | Small IR or fluent builder for supported expressions. |
| Function/operator allowlists | Reusable tables for safe Exasol-supported operators/functions. |
| Aggregate/window helpers | Reusable emission helpers where semantics are stable. |
| Unsupported result | Carry an explicit unsupported reason instead of generating approximate SQL. |
| SQL formatting tests | Shared tests for emitted Exasol SQL fragments. |

### Adapter Responsibility

Each adapter remains responsible for:

- walking its own AST/planner structures;
- deciding which source-database constructs are semantically safe;
- mapping source types/functions/operators to SDK builder calls;
- preserving source-database correctness by failing closed when unsure.

This avoids putting MariaDB- or PostgreSQL-specific parser dependencies into
the SDK.

## Native read API

Generic consumers continue to use `Session::fetch()` and
`Session::fetchPositioned()` for Arrow record batches. Servers advertising
`NATIVE_TABLE_READ_BATCH_V1` additionally support `Session::fetchNative()` and
`Session::fetchPositionedNative()`. The SDK validates magic, ABI, schema kinds,
decimal scale, exact buffer extents, NULL/Boolean values, string-size sums,
overflow, and trailing bytes before returning an owning `NativeFetchBatch`.

The stable C ABI exposes `sessiongw_c_native_fetch` plus indexed column views:
`column_nulls`, `column_fixed_data`, `column_sizes`, and
`column_variable_data`. These pointers are borrowed and remain valid until
`sessiongw_c_native_fetch_destroy()`. No C++ or Arrow C++ ABI object crosses
that boundary. MariaDB retains the opaque owner while directly materializing
numeric, decimal, temporal, Boolean, and UTF-8 fields.

## Packaging and Open-Source Considerations

Open-source readiness should be a design constraint from the beginning, even if
initial development happens inside this repository.

Questions to resolve before external publishing:

| Question | Direction |
|---|---|
| License | Decide with product/legal; do not assume the database server license automatically applies. |
| Dependencies | Keep dependency footprint small; Arrow dependency is expected. |
| Build system | CMake-friendly standalone package is preferred for MariaDB/PostgreSQL integration. |
| ABI/API stability | Prefer source-level API stability first; binary ABI stability can come later if needed. |
| Authentication helpers | Avoid embedding secrets; expose configuration hooks. |
| Versioning | Version SDK API independently but keep it aligned with SessionGW protocol capabilities. |

## First Implementation Micro-Plan

For the current task, the clean implementation path should be:

1. Review this SDK design with the owner.
2. Decide temporary in-repo SDK location versus separate package skeleton.
3. Add only the minimal client scaffold needed for future tests:
   - connection options type;
   - error/category type;
   - capability type;
   - frame encode/decode helpers;
   - placeholder client class;
   - simulation client command-line skeleton.
4. Add unit tests only for concrete helper code introduced in this task.
5. Add nano test scaffold that validates the simulation client scaffold now and
   can be extended to a live Exasol login/upgrade test once the server-side
   protocol skeleton exists.
6. Stop for independent-agent and human review before expanding into protocol
   upgrade implementation.

## Open Questions

| Question | Current direction |
|---|---|
| SDK repository location | Keep the initial in-repo layout/package boundaries compatible with future extraction. |
| Dependency on existing Exasol drivers | Prefer public WebSocket API v5 bootstrap for the SDK; avoid copying native driver/login internals into every adapter. |
| C API wrapper | Implemented as the stable `sessiongw_c_*` boundary for native adapters. |
| Arrow ownership model | The C++ facade returns Arrow C++ objects; the C ABI returns borrowed Arrow IPC views owned by opaque SDK handles. |
| Pushdown IR size | Keep it small; add constructs only when a concrete adapter needs them. |

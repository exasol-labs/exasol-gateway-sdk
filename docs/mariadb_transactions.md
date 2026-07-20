# MariaDB Transaction Semantics

## Transaction Model

MariaDB `ENGINE=EXASOL` supports full transactions without user savepoints.
One MariaDB THD maps to one authenticated Exasol Gateway connection, one Exasol
session, and one Exasol transaction stream.

The adapter must synchronize the remote autocommit attribute with MariaDB
transaction state whenever EXASOL first participates in a statement:

| MariaDB state | SessionGateway state |
|---|---|
| normal autocommit statement | `SetAutocommit(true)` |
| `SET autocommit=0` | `SetAutocommit(false)` |
| `START TRANSACTION` while session autocommit remains enabled | `SetAutocommit(false)` for the explicit transaction |
| full `COMMIT` | `Commit` |
| full `ROLLBACK` | `Rollback` |
| return to normal autocommit work | `SetAutocommit(true)` before participation |

The state must be derived from MariaDB's effective transaction options, not
only from `@@autocommit`, because `START TRANSACTION` creates an explicit
transaction without permanently changing the session variable. The adapter
should avoid redundant `SetAutocommit` requests but must resynchronize after a
reconnect or session reset.

Always forcing the SessionGateway default autocommit state would be simpler,
but it would make MariaDB `COMMIT` and `ROLLBACK` misleading. Explicit state
synchronization is therefore the selected model.

## Commit and Rollback

The adapter registers EXASOL participation with `trans_register_ha()` at the
MariaDB statement scope and, when an explicit transaction is active, at full
transaction scope.

In remote autocommit mode, closing a successful table operation commits it
according to Exasol autocommit semantics. In an explicit transaction,
`CloseOperation` finishes the DMP operation but its changes remain in the
Exasol transaction until MariaDB invokes the full commit or rollback callback.
Reads, inserts, updates, and deletes on the THD share that same transaction and
snapshot.

MariaDB statement-level commit callbacks must not commit an explicit remote
transaction. Full transaction callbacks map as follows:

- full commit calls SessionGateway `Commit` exactly once;
- full rollback calls SessionGateway `Rollback` exactly once;
- disconnect with an active transaction performs best-effort rollback before
  closing the SessionGateway session;
- a lost commit acknowledgement is `OUTCOME_UNKNOWN` and must never be replayed.

After a successful explicit commit or rollback, the SessionGateway autocommit
attribute remains synchronized with the effective MariaDB mode. A later
implicit transaction can begin through the next read or DML operation.

## No Savepoint Support

User savepoints are not supported in this product slice. `SAVEPOINT`,
`ROLLBACK TO SAVEPOINT`, and `RELEASE SAVEPOINT` fail closed for a transaction
in which EXASOL participates; they never appear to succeed while leaving remote
changes unaffected. EXASOL access is also rejected if a savepoint was created
before the engine's first participation, because MariaDB cannot invoke an
engine callback for that pre-existing savepoint.

Without a statement savepoint, an EXASOL statement failure inside an explicit
transaction rolls back the entire remote transaction. The adapter must mark the
MariaDB transaction rollback-only so that subsequent `COMMIT` cannot silently
commit a partial local/mixed-engine transaction. This behavior must be explicit
in the returned diagnostic and documentation.

Statements involving multiple EXASOL handler instances share the same remote
transaction. If one operation fails, the full rollback rule prevents an earlier
operation from the failed transaction being committed later. Distributed
atomicity across EXASOL and another storage engine is not provided: XA/two-phase
commit remains unsupported and must not be advertised.

## Implementation State

The generic SessionGateway protocol and SDK implement and test
`SetAutocommit`, `Commit`, and `Rollback`, including explicit commit/rollback,
open-operation rejection, cleanup, stale row-location invalidation, uncertain
completion, and transaction conflicts.

The MariaDB adapter installs handlerton commit, rollback, and savepoint
callbacks, registers statement/full participation, synchronizes effective
autocommit state, and reports `TRANSACTIONS=YES`. Successful autocommit DML
continues to commit when its operation closes. Full explicit transaction
boundaries are owned by the handlerton callbacks.

Pushed SQL is disabled while an explicit transaction is active. Exasol's query
cache retains table read references until transaction end and cannot safely
upgrade the same object for a later DMP write. Explicit transactions therefore
use the direct vector scan path, which releases each cursor at statement end
while preserving the transaction and supports read-before-write semantics.

## Implementation Gate

The transaction advertisement requires all of the following:

1. Effective MariaDB autocommit/explicit-transaction state synchronization.
2. Statement and full-transaction `trans_register_ha()` participation.
3. Handlerton commit and rollback callbacks with correct `all` handling.
4. Full-transaction rollback and MariaDB rollback-only marking on statement
   failure when remote autocommit is disabled.
5. Explicit rejection of unsupported user savepoint operations.
6. Cursor, metadata, and row-location invalidation after full transaction
   boundaries, reconnect, and rollback-on-error.
7. Disconnect rollback with no commit from handler destruction or cleanup.
8. Tests for implicit read-started transactions, DML read-your-writes,
   autocommit transitions, commit, rollback, statement failure, savepoint
   rejection, conflicts, kill/timeout, transport loss, and unknown completion.
9. Explicit documentation that XA/distributed two-phase commit is unsupported.

The Release plugin build and live workloads validate transaction capability
advertisement, `SET autocommit=0`, `START TRANSACTION`, read-started
transactions, read-your-writes, full commit/rollback, transition back to
autocommit, positioned and multi-batch DML, statement failure with full rollback,
both savepoint orderings, and transaction-scoped row-location invalidation.
Generic SessionGateway tests cover conflict, cleanup, transport loss, open-handle
rejection, and unknown completion behavior. Debug fault injection additionally
checks that uncertain completion is reported and never replayed.

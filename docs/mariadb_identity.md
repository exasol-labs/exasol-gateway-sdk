# MariaDB Identity, Authorization, and Audit Mapping

## Supported Model

MariaDB `ENGINE=EXASOL` supports an explicitly configured
**service-account model**. Every admitted MariaDB THD opens its own Exasol Gateway
session using the configured `EXASOL_SESSIONGW_USER` and
`EXASOL_SESSIONGW_PASSWORD` credentials.

The adapter does not receive a reusable clear-text MariaDB password and does not
attempt to authenticate to Exasol as the MariaDB login user. Direct user
impersonation, MariaDB-role-to-Exasol-role mapping, credential delegation, and
shared Exasol Gateway sessions are not supported.

The service-account mode is disabled by default. A deployment must set:

```text
EXASOL_SESSIONGW_IDENTITY_MODE=service_account
EXASOL_SESSIONGW_ALLOWED_MARIADB_USERS=<allowlist>
```

The allowlist is a comma-separated list of exact authenticated MariaDB user
names or `user@host_or_ip` principals. Whitespace around entries is ignored.
`*` admits every authenticated MariaDB user, but is accepted only as an explicit
operator choice. An absent mode, absent allowlist, empty allowlist, or unmatched
principal fails before the Exasol Gateway connection is opened.

The identity check uses `THD::main_security_ctx`, which represents the original
authenticated MariaDB connection. Stored-program definer contexts and temporary
changes to the effective privilege context cannot bypass admission.

## Authorization Consequences

There are two independent authorization boundaries:

1. MariaDB grants determine which authenticated users may access each local
   `ENGINE=EXASOL` table.
2. The Exasol service account determines the maximum remote schemas, tables,
   and operations available through Exasol Gateway.

The allowlist is an admission boundary, not a substitute for MariaDB table
grants. Operators must grant the Exasol service account only the remote
privileges needed by the mapped MariaDB tables and must grant MariaDB users only
intended local table privileges. The adapter continues to rely on normal Exasol
metadata visibility and object privilege checks for every direct operation.

Because the remote session uses a service identity, Exasol roles are those of
the service account. MariaDB roles are not propagated. Mixed assumptions such
as “a MariaDB user with the same name automatically has that Exasol user's
rights” are explicitly invalid.

## Audit Mapping

The Exasol login user and SQL audit user remain the configured service account.
For correlation, the SDK sets the Exasol WebSocket `clientName` login attribute
to:

```text
ExasolGateway MariaDB <authenticated-user>@<host-or-ip>
```

Identity bytes outside the unambiguous ASCII set `[A-Za-z0-9_.:-]` are
percent-encoded and the value is bounded to 255 bytes. This attribute appears in Exasol session/login monitoring and identifies the
originating MariaDB principal, but it is informational and must not be used by
Exasol as an authorization assertion.

A complete audit trail therefore correlates:

- MariaDB authentication, grants, statement/audit records, and connection ID;
- Exasol service-account sessions and their `clientName` origin attribute;
- Exasol Gateway structured errors and transaction outcomes.

The current mapping is connection-level. It records the authenticated login,
not a stored-program definer or per-statement effective role. If the
THD's authenticated principal changes while an engine context remains active,
the adapter fails closed and requires a new connection rather than reusing a
session carrying the old audit identity.

## Credential and Deployment Requirements

- Service-account credentials must be provisioned as deployment secrets, not
  embedded in table definitions or SQL text.
- TLS remains mandatory for production connections.
- One MariaDB THD maps to one Exasol session; credentials and authenticated
  state are never shared between THDs.
- Credential rotation requires new Exasol Gateway connections; existing THDs
  retain their already authenticated Exasol sessions until disconnect/reset.
- Failed admission must not attempt a remote login and must not fall back to a
  broader identity.

A future per-user model requires an explicit credential-delegation or trusted
impersonation protocol with authenticated, integrity-protected origin claims.
It must be introduced as a separate negotiated capability rather than inferred
from matching user names.

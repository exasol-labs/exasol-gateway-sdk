# Security Policy

## Supported versions

The project is currently a `0.x` preview. Security fixes are applied to the
latest release line.

## Reporting

Do not open public issues for suspected vulnerabilities. Report them through
Exasol's security contact at <security@exasol.com> with the affected version,
reproduction details, and impact.

## Deployment requirements

Production clients must use TLS certificate verification. Plaintext and
certificate-skipping modes exist only for isolated integration tests. Configure
credentials through a deployment secret mechanism; never commit credentials or
place them in SQL text. Completion requests with uncertain outcomes must not be
replayed automatically.

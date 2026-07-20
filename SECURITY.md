# Security Policy

Capstan executes your code against your database; the engine itself holds no
credentials beyond the storage URL you give it, and the dashboard/MCP
surfaces are operator tools intended for trusted networks (bind localhost or
front with your proxy; use `token:` and `authorizer:`).

## Reporting a vulnerability

Please do **not** open a public issue for security reports. Email the
maintainer (address on the GitHub profile) with details and a reproduction
if possible. You'll get an acknowledgment within a few days; fixes ship as
patch releases with credit unless you prefer otherwise.

## Scope notes for researchers

- SQL construction: all values are bind parameters; the only interpolations
  are compile-time field/index names. Reports proving otherwise are very
  welcome.
- Encrypted inputs are AES-256-GCM envelopes (`Capstan.Job`); keys come from
  your MFA config and are never persisted or logged by Capstan.
- The dashboard's HTTP server is intentionally minimal (no TLS); deployment
  guidance lives in the operations guide.

# Contributing to Capstan

Thanks for looking under the hood. Ground rules that keep this codebase easy
to work on:

## Development

```bash
mix deps.get
mix test                # full suite against the in-memory adapter
CAPSTAN_PG=1 mix test   # same suite against Postgres
```

The Postgres run expects a server at `localhost:55433` (password `capstan`):

```bash
docker run -d --name capstan-postgres -e POSTGRES_PASSWORD=capstan \
  -e POSTGRES_DB=capstan_v2_test -p 55433:5432 postgres:16
```

## The rules that matter

1. **Storage semantics live once.** Anything both adapters must agree on
   belongs in the pure shared-logic module (lib/capstan/storage.ex) or in
   the shared test suite.
   If you add a storage operation, implement it in Memory *and* Postgres and
   cover it with a test that runs against both — the suite is the contract.
2. **Never read the wall clock in engine code.** Take `now` as an argument
   (SQL included: `$now` parameters, not `now()`). Tests advance a
   `Capstan.Clock.Sim`; a `Process.sleep` in a test is a review flag.
3. **No leaders.** If your feature needs cluster-wide once-ness, express it
   as an idempotent operation deduped by the database (unique index,
   row-level atomicity), not by election.
4. **First-class columns over meta blobs.** If the engine branches on it, it
   gets a column and an index, not a JSON path.
5. **Failure honesty.** Document at-least-once edges and race windows in the
   moduledoc where they live. A known caveat in writing beats an implicit one
   in production.

## Pull requests

- One logical change per PR; tests required, on both adapters where storage
  is touched.
- `mix compile --warnings-as-errors` must pass.
- Public functions get `@doc` and specs; internal modules get `@moduledoc
  false` and a one-paragraph comment saying what they own.

## Reporting issues

Include: Capstan version, storage adapter, Postgres version, and — for
engine bugs — the smallest failing scenario you can express with
`Capstan.Testing.drain/2` and a SimClock. Security reports: email the
maintainer rather than opening a public issue.

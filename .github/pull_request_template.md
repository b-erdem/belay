## What & why

## Checklist
- [ ] `mix compile --warnings-as-errors` clean
- [ ] `mix test` and `CAPSTAN_PG=1 mix test` both green
- [ ] Storage semantics touched? Implemented in **both** adapters, shared
      logic in `Storage.Logic`, covered by a test that runs against both
- [ ] No wall-clock reads in engine code (`now` is always a parameter)
- [ ] Docs/guides updated where behavior changed
- [ ] Wire contract touched? `SCHEMA.md` updated in the same PR
- [ ] Durable semantics touched? TLA+ model/trace mapping updated; canonical
      smoke passes and the corresponding mutant still fails

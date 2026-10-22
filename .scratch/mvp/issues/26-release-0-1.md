# 26: 0.1 release

**What to build:** The library is ready to publish as 0.1: user docs including the caveats (async Processors stretch transactions, disposable disks mean full Restore), CI running against Kafka 3.9 and 4.x, and the package no longer private.

**Blocked by:** 20, 23, 24, 25

**Status:** ready-for-agent

- [x] README/user guide with examples for DSL, Processor API, Test Driver
- [x] Documented caveats and the minimum broker version
- [x] CI workflow for unit, integration (3.9 + 4.x) and nightly crash suite
- [x] Remove `private`, version 0.1.0 (do not publish without the user)

Spec: [../spec.md](../spec.md). Glossary: [CONTEXT.md](../../../CONTEXT.md). ADRs: [docs/adr/](../../../docs/adr/)

## Notes from earlier tickets (for the user docs)

- **Lifecycle (ticket 25):** created → rebalancing ⇄ restoring ⇄ running → stopping → stopped | error, aggregated across Workers (any rebalancing ⇒ rebalancing; else any restoring ⇒ restoring; else running). Readiness = `running`, liveness = not `error`. `start()` resolves once every Worker has joined, possibly before `running`.
- **Events and metrics (ticket 25):** document `app.on`/`app.off` events and a metric table (incl. consumer-lag = committed offset → last stable offset, sampled every 2 s; rates come from the `records-processed` counter). Exceptions from listeners and the metrics sink are ignored.
- **Restore (tickets 19, 25):** a Task restores when its first record arrives; no limit on concurrent Restores at startup; starting Tasks hold at most `commitMaxRecords` records, beyond that they re-read from committed offsets.
- **Error handlers (tickets 23, 25):** records skipped by a handler don't advance Stream Time; outputs forwarded before a failure are kept.
- **Windows (ticket 30):** retention = size + grace, not configurable yet; windows expire only when new records arrive.
- **Exactly-once caveats:** async Processors stretch transactions; disposable disks mean full Restore; transactional IDs include the App Process ID (ADR-0008); `@platformatic/kafka` read_committed is worked around (`src/read-committed.ts`, platformatic/kafka#467/#470).
- **Workers (ticket 18):** `workers: { count, module: import.meta.url }`, App Module must have no side effects (ADR-0013).

## Comments

Done on branch `ticket/26`. Nothing published, pushed or tagged.

- **Docs:** README rewritten; `docs/` user guide (streams-app, dsl, windows, processor-api, state-stores, test-driver, delivery-guarantees, workers, error-handlers, internal-topics, observability, operations, limitations). Every ```ts block in README/docs is a verbatim excerpt of a file in `examples/` (typechecked by `npm run typecheck` via `paths` in tsconfig); `test/docs.test.ts` enforces that and runs the Test Driver example. `vitest.config.ts` aliases `kstreams-ts` to `src/index.ts` for that.
- **CI:** `.github/workflows/ci.yml` (not run here): check job (typecheck, lint, unit, build, `npm pack --dry-run`), integration matrix Kafka 3.9.1 and 4.4.0 via `scripts/kafka-local.sh` (`KAFKA_VERSION`), nightly + manual crash suite on both (8 iterations). Unverified: the 3.9.1 download and broker start; I could not run Java here. I made the script fall back to a static single-voter quorum if `kafka-storage.sh format --standalone` is unsupported. Check the first CI run on 3.9.
- **Package:** version 0.1.0, `private` removed, keywords, `prepack` builds, `files` = dist (no maps), docs, examples, CONTEXT.md, CHANGELOG.md (checked with `npm pack --dry-run`: no src/test/.scratch). `repository`/`bugs`/`homepage` are not set; a TODO comment sits in the README.
- **Left for the user:** create the GitHub repo and fill in `repository`; confirm CI is green (esp. 3.9); `npm publish` and the git tag. Relative README links only work on npmjs once `repository` is set (the docs ship in the tarball anyway).
- **API observations (not fixed):** (1) `map` then `groupByKey` does not repartition, so grouping by a new key silently aggregates per Task; documented in limitations, but worth a guard or an API comment. (2) The "zombie timer writes" item from STATUS.md is documented as a known limitation. (3) Claims written from source comments but not tested against a broker: the transaction-timeout wording under "async Processors stretch transactions"; timers not persisted across restarts.
- `@platformatic/kafka` is `^2.14.0` (not pinned) while `src/read-committed.ts` relies on its internals; consider pinning exactly for 0.1.

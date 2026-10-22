# 26: 0.1 release

**What to build:** The library is ready to publish as 0.1: user docs including the caveats (async Processors stretch transactions, disposable disks mean full Restore), CI running against Kafka 3.9 and 4.x, and the package no longer private.

**Blocked by:** 20, 23, 24, 25

**Status:** ready-for-agent

- [ ] README/user guide with examples for DSL, Processor API, Test Driver
- [ ] Documented caveats and the minimum broker version
- [ ] CI workflow for unit, integration (3.9 + 4.x) and nightly crash suite
- [ ] Remove `private`, version 0.1.0 (do not publish without the user)

Spec: [../spec.md](../spec.md). Glossary: [CONTEXT.md](../../../CONTEXT.md). ADRs: [docs/adr/](../../../docs/adr/)

## Notes from earlier tickets (for the user docs)

- **Lifecycle (ticket 25):** created → rebalancing ⇄ restoring ⇄ running → stopping → stopped | error, aggregated across Workers (any rebalancing ⇒ rebalancing; else any restoring ⇒ restoring; else running). Readiness = `running`, liveness = not `error`. `start()` resolves once every Worker has joined, possibly before `running`.
- **Events and metrics (ticket 25):** document `app.on`/`app.off` events and a metric table (incl. consumer-lag = committed offset → last stable offset, sampled every 2 s; rates come from the `records-processed` counter). Exceptions from listeners and the metrics sink are ignored.
- **Restore (tickets 19, 25):** a Task restores when its first record arrives; no limit on concurrent Restores at startup; starting Tasks hold at most `commitMaxRecords` records, beyond that they re-read from committed offsets.
- **Error handlers (tickets 23, 25):** records skipped by a handler don't advance Stream Time; outputs forwarded before a failure are kept.
- **Windows (ticket 30):** retention = size + grace, not configurable yet; windows expire only when new records arrive.
- **Exactly-once caveats:** async Processors stretch transactions; disposable disks mean full Restore; transactional IDs include the App Process ID (ADR-0008); `@platformatic/kafka` read_committed is worked around (`src/read-committed.ts`, platformatic/kafka#467/#470).
- **Workers (ticket 18):** `workers: { count, module: import.meta.url }`, App Module must have no side effects (ADR-0013).

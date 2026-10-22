# 25: Observability

**What to build:** `app.state` reports the full Application State lifecycle (including rebalancing and restoring), typed events fire for state changes and errors, and a metrics interface reports lag, restore progress, commit duration, aborts, dropped late records and throughput.

**Blocked by:** 19, 30

**Status:** ready-for-agent

- [ ] Full Application State lifecycle
- [ ] Typed event emitter
- [ ] Metrics interface with the listed metrics

Spec: [../spec.md](../spec.md). Glossary: [CONTEXT.md](../../../CONTEXT.md). ADRs: [docs/adr/](../../../docs/adr/)

## Notes from ticket 18

- With worker threads, only running/stopped/failed cross the thread boundary today. Rebalancing/restoring Application States and metrics need Workers to report state and metrics to the main thread.

## Notes from ticket 23

- Count and report records skipped by error handlers (a dropped-records metric, as in Kafka Streams), and emit typed error events carrying the rebuilt error class from worker threads.
- Records skipped by a handler do not advance Stream Time; outputs a record forwarded before it failed are kept (as in Kafka Streams). Document both in the user docs (ticket 26).

## Notes from ticket 29

- Rebalance rounds are visible in Ownership (src/ownership.ts). Waits on Task locks, unstable offsets and open Changelog transactions are silent today; expose them as metrics/events.

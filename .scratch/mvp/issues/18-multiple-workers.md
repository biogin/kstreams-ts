# 18: Multiple Workers (worker threads)

**What to build:** `workers > 0` runs each worker thread as its own consumer-group member with its own consumer, producer and State Stores (ADR-0007). `workers: 0` still runs in the main thread.

**Blocked by:** 16, 28

**Status:** ready-for-agent

- [ ] Worker threads, each a group member, with results exact under exactly-once
- [ ] Workers rebuild the app definition according to the worker-module decision (see note)
- [ ] Integration test: 1 process × 2 Workers share Tasks

**Pending design decision:** worker threads can't receive the app definition (it contains functions). The recommended option is that the app is exported from its own module and config points at it (`workers: { count, module: import.meta.url }`); this needs the user's confirmation and an ADR before work starts. Split from the original ticket 18; scaling out across processes is ticket 29.

Spec: [../spec.md](../spec.md). Glossary: [CONTEXT.md](../../../CONTEXT.md). ADRs: [docs/adr/](../../../docs/adr/)

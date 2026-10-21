# 18: Multiple Workers (worker threads)

**What to build:** `workers > 0` runs each worker thread as its own consumer-group member with its own consumer, producer and State Stores (ADR-0007). `workers: 0` still runs in the main thread.

**Blocked by:** 16, 28

**Status:** ready-for-agent

- [ ] Worker threads, each a group member, with results exact under exactly-once
- [ ] Workers rebuild the app by importing the App Module named in `workers: { count, module }` (ADR-0013); a clear error if `workers > 0` without a module
- [ ] Integration test: 1 process × 2 Workers share Tasks

Decided: App Module, see ADR-0013. Split from the original ticket 18; scaling out across processes is ticket 29.

Spec: [../spec.md](../spec.md). Glossary: [CONTEXT.md](../../../CONTEXT.md). ADRs: [docs/adr/](../../../docs/adr/)

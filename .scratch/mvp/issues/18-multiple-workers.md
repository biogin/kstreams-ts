# 18: Multiple Workers

**What to build:** `workers > 0` runs each worker thread as its own consumer-group member with its own consumer, producer and stores (ADR-0007), using cooperative-sticky assignment. Running more processes with the same Application ID scales out.

**Blocked by:** 16

**Status:** ready-for-agent

- [ ] Worker threads, each a group member. `workers: 0` still runs in the main thread
- [ ] Cooperative-sticky assignor
- [ ] Integration test: 2 processes × 2 Workers share Tasks, results are exact under exactly-once

Spec: [../spec.md](../spec.md). Glossary: [CONTEXT.md](../../../CONTEXT.md). ADRs: [docs/adr/](../../../docs/adr/)

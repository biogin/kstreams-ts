# 10: `count()` with an in-memory State Store

**What to build:** A user writes `groupByKey().count({ store })` or uses `ctx.store(name)` in a Processor, and inspects the store's contents in the Test Driver. The State Store interface and its contract suite (seam 3) exist, with the in-memory store passing it.

**Blocked by:** 03, 06

**Status:** ready-for-agent

- [ ] Pluggable key-value State Store interface (ADR-0005)
- [ ] In-memory store passes the contract suite (get/put/delete, transactional rollback)
- [ ] `groupByKey`, `count`. `ctx.store(name)` in Processors
- [ ] Test Driver: read store contents

Spec: [../spec.md](../spec.md). Glossary: [CONTEXT.md](../../../CONTEXT.md). ADRs: [docs/adr/](../../../docs/adr/)

# 15: Exactly-once tracer (stateless)

**What to build:** With `guarantee: 'exactly-once'`, a stateless app commits outputs and consumed offsets atomically using one transactional producer, adding only the last offset per partition per batch. A `read_committed` reader never sees aborted output.

**Blocked by:** 07

**Status:** ready-for-agent

- [ ] Transactional producer per Worker (ADR-0008). Transactional ID derived from the Application ID
- [ ] `addOffset` only for the last record per partition per batch
- [ ] Integration test: forced abort leaves nothing visible to `read_committed`

Spec: [../spec.md](../spec.md). Glossary: [CONTEXT.md](../../../CONTEXT.md). ADRs: [docs/adr/](../../../docs/adr/)

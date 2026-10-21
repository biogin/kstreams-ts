# 23: Error handlers

**What to build:** Deserialization and processing errors go to pluggable handlers that fail fast by default. A ready-made `deadLetter(topic)` handler writes inside the transaction under exactly-once. Produce errors and `AsyncNotAllowedError` always stop the app (ADR-0010, ADR-0012).

**Blocked by:** 04, 16

**Status:** ready-for-agent

- [ ] Handler config on the Streams App
- [ ] `deadLetter(topic)` (transactional under exactly-once)
- [ ] Produce errors and `AsyncNotAllowedError` skip handlers

Spec: [../spec.md](../spec.md). Glossary: [CONTEXT.md](../../../CONTEXT.md). ADRs: [docs/adr/](../../../docs/adr/)

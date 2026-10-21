# 04: Serdes

**What to build:** A user can use built-in JSON, number and raw-bytes Serdes or plug in their own through the typed Serde interface. Null values are tombstones throughout, and a malformed payload raises a typed deserialization error.

**Blocked by:** 02

**Status:** ready-for-agent

- [ ] `serdes.json<T>()`, `serdes.number`, `serdes.bytes` alongside `serdes.string`
- [ ] Custom Serde interface is public and documented in the type definitions
- [ ] Null value ↔ tombstone round-trips through the Test Driver
- [ ] Malformed JSON raises a typed deserialization error. With no handlers yet (ticket 23), the app stops

Spec: [../spec.md](../spec.md). Glossary: [CONTEXT.md](../../../CONTEXT.md). ADRs: [docs/adr/](../../../docs/adr/)

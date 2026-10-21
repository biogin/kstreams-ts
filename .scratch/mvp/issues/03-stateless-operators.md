# 03: Stateless operators with typed chains

**What to build:** A user chains `filter`, `map` and `peek` on a Stream, with key and value types inferred through the whole chain, including `map` changing both, and sees the results in the Test Driver.

**Blocked by:** 02

**Status:** ready-for-agent

- [ ] `filter`, `map` (may change key and value types), `peek`
- [ ] Type inference tests (type-level assertions) show types flow through the chain with no annotations
- [ ] Behavior tested through the Test Driver

Spec: [../spec.md](../spec.md). Glossary: [CONTEXT.md](../../../CONTEXT.md). ADRs: [docs/adr/](../../../docs/adr/)

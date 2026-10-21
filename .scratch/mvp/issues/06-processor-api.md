# 06: Processor API

**What to build:** An advanced user registers a custom Processor with `app.processor(name, ctx => ({ process(record) {...} }))`, forwards records downstream with `ctx.forward`, and can write async Processors that are awaited one after another per Task.

**Blocked by:** 02

**Status:** ready-for-agent

- [ ] `app.processor` usable alongside DSL nodes (DSL → processor → DSL)
- [ ] `ctx.forward(record)` sends to children
- [ ] Async `process` is awaited, and records within one Task never overlap (test with deliberate delays)
- [ ] `ctx.store` / `ctx.schedule` exist in types but throw 'not implemented' until tickets 10/21

Spec: [../spec.md](../spec.md). Glossary: [CONTEXT.md](../../../CONTEXT.md). ADRs: [docs/adr/](../../../docs/adr/)

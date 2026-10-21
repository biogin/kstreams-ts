# 06: Processor API

**What to build:** An advanced user registers a custom Processor with `app.processor(name, ctx => ({ process(record) {...} }))`, forwards records downstream with `ctx.forward`, and can write async Processors that are awaited one after another per Task.

**Blocked by:** 02

**Status:** done

- [x] `app.processor` usable alongside DSL nodes (DSL → processor → DSL)
- [x] `ctx.forward(record)` sends to children
- [x] Async `process` is awaited, and records within one Task never overlap (test with deliberate delays)
- [x] `ctx.store` / `ctx.schedule` exist in types but throw 'not implemented' until tickets 10/21

Spec: [../spec.md](../spec.md). Glossary: [CONTEXT.md](../../../CONTEXT.md). ADRs: [docs/adr/](../../../docs/adr/)

## Comments

- 2026-10-21: Done on branch `ticket/06`. Public: `app.processor(name, supplier)` returns a `NamedProcessor<KIn, VIn, KOut, VOut>`, and `stream.process(namedProcessor)` connects it and returns the Stream of records it forwards (DSL → Processor → DSL). Exported types: `Processor` (`process` may return a Promise), `ProcessorContext` (`forward`, plus `store`/`schedule` typed as `never` that throw "not implemented" until tickets 10/21), `ProcessorSupplier`, `TimerType` (`'stream-time' | 'wall-clock-time'`), `NamedProcessor`. Users annotate the supplier (`(ctx: ProcessorContext<KOut, VOut>) => ({ process(record: StreamRecord<KIn, VIn>) {...} })`) or pass the generics. Stream/Processor record types are checked at compile time; names must be non-empty and unique; connecting several Streams to one Processor merges them. The Stream after a Processor carries no Serdes, so `.to()` needs both.
- Test Driver decision: `pipe` now returns `Promise<void>` (non-breaking: when every Processor is synchronous the record is fully processed before `pipe` returns). Await it with async Processors. Records piped without awaiting are queued per Task and never overlap; each Processor also handles the records forwarded to it one at a time, in order, so `ctx.forward` needs no await. **Errors now surface as a rejected `pipe` Promise, never a synchronous throw**, and a failed Task rejects every later record with the same error (fail-fast, ADR-0010). Tickets 05 and 23 should assert with `await expect(input.pipe(...)).rejects`.
- `createTask(...).process` returns `Promise<void>` with the same semantics; the Kafka runtime (ticket 07) should await it per Task.

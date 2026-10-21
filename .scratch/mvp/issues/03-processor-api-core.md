# 03: Processor API core

Status: ready-for-agent
Milestone: M1
Blocked by: 01

The foundation from ADR-0001: a graph of Processors, each receiving records and passing them on to downstream Processors.

## Acceptance

- `app.processor(name, ctx => ({ process(record) { ... } }))` registers a Processor. `ctx.forward(record)` sends to its children.
- Source nodes (topic + Serdes) and sink nodes (topic + Serdes) bound the graph.
- Async `process` is awaited one after another per Task, so records for the same Task never overlap (spec, API sketch).
- `ctx.store()` and `ctx.schedule()` exist in the types but throw "not implemented until M2/M5".
- Unit tests run the graph in memory, with no Kafka.

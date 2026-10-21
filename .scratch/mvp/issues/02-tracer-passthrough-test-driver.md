# 02: Tracer bullet: pass-through Streams App in the Test Driver

**What to build:** A user creates a Streams App with `createStreamsApp`, declares `stream(topic)` with a string Serde and `.to(topic)`, and a record piped into the public Test Driver comes out of the output topic unchanged. This is the thinnest possible path through the public API, the DSL-to-Processor graph, the Serde boundary and the Test Driver.

**Blocked by:** 01

**Status:** done

- [x] `createStreamsApp(config)` with an Application ID returns a Streams App
- [x] `app.stream(topic, { key, value })` → `.to(topic, serdes?)` builds a runnable graph
- [x] `createTestDriver(app)`: `input(topic).pipe(key, value, { timestamp? })` and `output(topic).read()`/`readAll()`
- [x] Records pass through real Serde serialize/deserialize in the Test Driver (only `serdes.string` needed here)
- [x] Exported from the package entry point. Tests are at seam 1 (Test Driver) only

Spec: [../spec.md](../spec.md). Glossary: [CONTEXT.md](../../../CONTEXT.md). ADRs: [docs/adr/](../../../docs/adr/)

## Comments

- 2026-10-21: Done on branch `ticket/02`. Public: `createStreamsApp({ applicationId })` (validated: ASCII letters, digits, `.`, `_`, `-`), `app.stream(topic, { key, value })`, `stream.to(topic, serdes?)` (missing Serdes default to the input topic's), `createTestDriver(app)` with `input<K, V>(topic).pipe(key, value, { timestamp? })` and `output<K, V>(topic).read()`/`readAll()`, `serdes.string`, types `Serde`, `Serdes`, `StreamRecord`. Internal foundations: `ProcessorGraph` (source, Processor and sink nodes; `src/processor-graph.ts`) that the DSL compiles to, and `createTask(graph, collector)` (`src/task.ts`) that runs it on serialized records, meant to be reused by the Kafka runtime. The Test Driver's wall-clock time starts at 0 and is the default timestamp. `pipe` is synchronous for now; async Processors (ticket 06) may need it to return a Promise.

# 02: Tracer bullet: pass-through Streams App in the Test Driver

**What to build:** A user creates a Streams App with `createStreamsApp`, declares `stream(topic)` with a string Serde and `.to(topic)`, and a record piped into the public Test Driver comes out of the output topic unchanged. This is the thinnest possible path through the public API, the DSL-to-Processor graph, the Serde boundary and the Test Driver.

**Blocked by:** 01

**Status:** ready-for-agent

- [ ] `createStreamsApp(config)` with an Application ID returns a Streams App
- [ ] `app.stream(topic, { key, value })` → `.to(topic, serdes?)` builds a runnable graph
- [ ] `createTestDriver(app)`: `input(topic).pipe(key, value, { timestamp? })` and `output(topic).read()`/`readAll()`
- [ ] Records pass through real Serde serialize/deserialize in the Test Driver (only `serdes.string` needed here)
- [ ] Exported from the package entry point. Tests are at seam 1 (Test Driver) only

Spec: [../spec.md](../spec.md). Glossary: [CONTEXT.md](../../../CONTEXT.md). ADRs: [docs/adr/](../../../docs/adr/)

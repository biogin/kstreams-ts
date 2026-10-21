# 04: Test Driver skeleton

Status: ready-for-agent
Milestone: M1
Blocked by: 02, 03

The public **Test Driver** (CONTEXT.md, ADR-0011): runs a Streams App's Processor graph in memory, with no Kafka.

## Acceptance

- `const driver = createTestDriver(app)`. `driver.input(topic).pipe(key, value, { timestamp? })`. `driver.output(topic).read()` and `readAll()` return typed, deserialized records.
- Goes through the real Serdes, so serialization bugs show up in tests.
- Exported from the package as public API, with a usage example in the README.
- Hooks for store access and advancing time are planned but not built until M2/M5.

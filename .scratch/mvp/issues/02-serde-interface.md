# 02: Serde interface and built-in Serdes

Status: ready-for-agent
Milestone: M1
Blocked by: 01

A minimal typed **Serde** interface (`serialize(value): Buffer | null`, `deserialize(bytes): T | null`, where null means a tombstone) with built-in `serdes.string`, `serdes.json<T>()`, `serdes.bytes` and `serdes.number`.

## Acceptance

- Round-trip tests for each built-in Serde, including null and tombstone handling.
- A bad JSON payload throws a typed deserialization error. The error handlers that will catch it come in M6 (ADR-0010).
- No Schema Registry code (out of scope, spec).

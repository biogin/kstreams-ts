# 07: Tracer bullet on real Kafka

**What to build:** The pass-through app from ticket 02 runs against a real broker with `workers: 0` and at-least-once: it consumes, runs the graph, produces, waits for acks, then commits offsets. `start`/`stop` and basic `app.state` (created → running → stopping → stopped | error) work, and graceful stop finishes the batch and leaves the group.

**Blocked by:** 02

**Status:** ready-for-agent

- [ ] Adds `@platformatic/kafka` (ADR-0004). Consumer group = Application ID
- [ ] Integration test harness: tests connect to a broker given by an environment variable. A script downloads and starts a local Kafka 4.x KRaft broker without Docker (see spec Testing Decisions). Each test uses unique topic names
- [ ] Batch closed by time or count (commit interval, default 100 ms)
- [ ] `exactly-once` and `workers > 0` are rejected as not implemented yet
- [ ] Integration test: N records in → all N out

Spec: [../spec.md](../spec.md). Glossary: [CONTEXT.md](../../../CONTEXT.md). ADRs: [docs/adr/](../../../docs/adr/)

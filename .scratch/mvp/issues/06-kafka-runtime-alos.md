# 06: Kafka runtime with `workers: 0` and at-least-once

Status: ready-for-agent
Milestone: M1
Blocked by: 05

Run a Streams App against real Kafka, in the main thread only, with the at-least-once Delivery Guarantee.

## Acceptance

- Adds `@platformatic/kafka` (ADR-0004). The consumer group ID comes from the Application ID.
- Loop: consume batch → run the Processor graph per Task → produce outputs → wait for acks → commit offsets. A batch is closed by time or count, defaulting to 100 ms.
- `app.start()` and `app.stop()`. `app.state` goes through `CREATED → RUNNING → STOPPING → STOPPED | ERROR`. Rebalancing and restoring come later.
- Graceful stop finishes the current batch, commits and leaves the group, with a timeout.
- `guarantee: 'exactly-once'` and `workers > 0` are rejected with "not implemented until M3/M4".

# MVP spec: kstreams-ts

Status: ready-for-agent

M1 issues: [issues/](./issues/)

Partition-aware stateful stream processing on Kafka for Node.js, with fault-tolerant State Stores and exactly-once processing. Terms are defined in [CONTEXT.md](../../CONTEXT.md). Decisions are in [docs/adr/](../../docs/adr/).

## Goal and ambition

Start as a learning project but design for adoption: a public API from day one and real correctness testing, but no chaos testing across several brokers until the core works. Test demand after M6 (pitch to Platformatic, publish a write-up) and decide by month two whether to go further.

## Platform

- Node ≥24 LTS only. ESM-only, strict TypeScript, one package. Apache-2.0.
- Kafka client: `@platformatic/kafka`, used directly ([ADR-0004](../../docs/adr/0004-platformatic-kafka-as-client.md)).
- State Stores: pluggable. In-memory version for tests, SQLite through `better-sqlite3` as the default ([ADR-0005](../../docs/adr/0005-pluggable-stores-sqlite-default.md)).
- Serdes: a minimal typed interface with built-in string, JSON and raw bytes. Schema Registry comes later as a separate package.

## API sketch

```ts
const app = createStreamsApp({
  applicationId: 'order-stats',
  brokers: ['localhost:9092'],
  guarantee: 'exactly-once', // or 'at-least-once'
  workers: 4,                // 0 = run in the main thread
  stateDir: './state',
})

app.stream('orders', { key: serdes.string, value: serdes.json<Order>() })
  .filter((k, o) => o.status === 'paid')
  .groupByKey()
  .window(tumbling('1m', { grace: '10s' }))
  .count({ store: 'paid-per-minute' })
  .toStream()
  .to('paid-order-counts', { value: serdes.number })

await app.start()
```

- **Processor API** ([ADR-0001](../../docs/adr/0001-processor-api-core-idiomatic-dsl.md)): `app.processor(name, ctx => ({ process(record) { ... } }))`, with `ctx.store(name)`, `ctx.forward()` and `ctx.schedule()` available. Processors may be async and are awaited one after another per Task.
- **DSL functions are synchronous**, enforced through types, at build time and at runtime ([ADR-0012](../../docs/adr/0012-dsl-functions-must-be-synchronous.md)).
- **Application ID** derives the consumer group, Internal Topic names (`<applicationId>-<store>-changelog`), transactional IDs (`<applicationId>-<worker>`) and the state directory (`<stateDir>/<applicationId>/<task>/`). Renaming it resets state.
- **Application State**: `app.state` moves through `CREATED → REBALANCING ⇄ RESTORING ⇄ RUNNING → STOPPING → STOPPED | ERROR`, for readiness and liveness probes.

## Engine design

- **Task** = one partition number across all Co-partitioned inputs. Partition counts are checked at startup.
- **Worker** = one consumer-group member with its own consumer, transactional producer and stores ([ADR-0007](../../docs/adr/0007-worker-is-group-member.md), [ADR-0008](../../docs/adr/0008-one-transactional-producer-per-worker.md)).
- **Commit**: a batch per Worker, closed by time or count, defaulting to 100 ms. Under exactly-once, each SQLite transaction stays open for the length of the Kafka transaction and commits only after Kafka commits ([ADR-0002](../../docs/adr/0002-exactly-once-designed-in-from-day-one.md)). Call `addOffset` only for the last record of each partition in a batch.
- **Revoke** under exactly-once aborts the open transaction and rolls back SQLite ([ADR-0006](../../docs/adr/0006-abort-on-revoke.md)). **Graceful shutdown** commits the current batch, writes Checkpoints, closes stores and leaves the group. If the timeout runs out, it aborts.
- **Restore**: SQLite files persist with Checkpoints, and a missing or unclean Checkpoint means wipe and rebuild. A separate per-Worker restore consumer reads with `read_committed`, and only the Task being restored is paused ([ADR-0009](../../docs/adr/0009-local-state-checkpoints-and-restore.md)). Assignment uses cooperative-sticky.
- **Internal Topics** are created automatically by default (compacted, with source partition counts), and the app fails clearly if the partition count is wrong. Setting `autoCreateInternalTopics: false` makes the app check the topics and list any that are missing.
- **Errors**: pluggable handlers, fail-fast by default, and `deadLetter(topic)` is provided (in the same transaction under exactly-once). Produce errors and `AsyncNotAllowedError` always fail fast ([ADR-0010](../../docs/adr/0010-fail-fast-error-handling.md)).
- **Time**: Event Time from record timestamps, Stream Time per Task, Grace Period for late records. Timers fire on Stream Time or on wall-clock time.
- **Observability**: a typed event emitter, a metrics interface (lag, restore progress, commit time, aborts, records per second) and an optional OpenTelemetry adapter package later.

## Scope

**MVP:** Processor API with timers. Key-value State Store (in-memory and SQLite). Changelog, Checkpoint and Restore. At-least-once and then exactly-once. DSL: `stream`, `filter`, `map`, `peek`, `to`, `groupByKey`, `aggregate`/`count`/`reduce`. Tumbling windows. Test Driver. Auto-created Internal Topics. Fail-fast and dead-letter handlers.

**v1:** Repartition Topics (`groupBy(newKey)`), joins (stream-table, stream-stream), hopping and session windows.

**v1.x and later:** a state-aware assignor using per-member join metadata, standby replicas, reading stores from outside the app, a Schema Registry package, an OpenTelemetry adapter, switching to `node:sqlite`.

**Out:** compatibility with Java Kafka Streams formats ([ADR-0003](../../docs/adr/0003-no-java-kafka-streams-compatibility.md)), Bun and Deno.

## Milestones

Each one ends with something you can demo.

1. **M1: stateless pipeline.** `filter`/`map`/`to` end to end, `workers: 0`, at-least-once, Test Driver skeleton.
2. **M2: State.** Key-value State Store (in-memory and SQLite), Changelog, Checkpoint, Restore. `count()` survives a crash.
3. **M3: exactly-once.** Transactions lined up with SQLite, abort on revoke, graceful shutdown commit, first crash tests ([ADR-0011](../../docs/adr/0011-crash-test-suite-backs-exactly-once.md)).
4. **M4: multiple Workers.** Worker threads, cooperative-sticky assignment, per-Task pausing during Restore. Crash tests extended to rebalances.
5. **M5: time.** Event Time, Stream Time, timers, tumbling windows, Grace Period, window retention.
6. **M6: hardening.** Error handlers, auto-created Internal Topics, observability, docs, 0.1 release, Platformatic pitch.

Exactly-once (M3) comes before multiple Workers (M4) on purpose. Prove exactly-once on a single thread first, then prove it survives rebalances.

## Testing

- **Test Driver**: public, in-memory, no Kafka. It's a product feature from M1 onward.
- **Integration**: real Kafka through testcontainers.
- **Crash test suite**: random Worker kills mid-batch, rebalances and restarts, with outputs and stores compared exactly against a reference run. Runs nightly in CI and is required before any release claims exactly-once ([ADR-0011](../../docs/adr/0011-crash-test-suite-backs-exactly-once.md)).

## Defaults (confirmed 2026-10-21)

- **Minimum broker version:** Kafka ≥3.6 using the classic group protocol. KIP-848 is opt-in on Kafka 4.x. CI tests against the latest 3.9 and 4.x.
- **Output record timestamps:** inherit the Event Time of the input record that produced them. Window results get the window end.
- **Store deletes:** `delete(key)` writes a tombstone to the Changelog.
- **Window retention:** window size plus Grace Period, after which entries are dropped and tombstoned. Final values are set in M5.
- **Changelog replication factor:** defaults to 3 and is configurable. Dev setups use 1.

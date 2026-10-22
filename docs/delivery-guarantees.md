# Delivery guarantees and exactly-once

A **Delivery Guarantee** is the processing contract an application runs under. Set it with `guarantee` in the config.

## At-least-once (the default)

Each Worker collects input into batches (every `commitIntervalMs`, or `commitMaxRecords` records), processes them, produces the outputs, and then commits the consumed offsets. After a crash, records since the last commit are processed again, so **outputs can be duplicated**, and an aggregation can count such a record twice. It has lower latency and overhead than exactly-once, and uses the same engine.

## Exactly-once

With `guarantee: 'exactly-once'`, a batch's **outputs, Changelog writes and consumed offsets commit atomically in one Kafka transaction** ([ADR-0002](./adr/0002-exactly-once-designed-in-from-day-one.md)). State Store writes stay in a store transaction (SQLite) until the Kafka transaction has committed, and are rolled back if it aborts. After any crash, rebalance or restart, each input record's effect appears exactly once in the outputs and State Stores. Each Worker has one transactional producer covering all its Tasks ([ADR-0008](./adr/0008-one-transactional-producer-per-worker.md)); fencing comes from the consumer group generation and member ID sent with the offset commit (KIP-447).

A rebalance that takes a Task away mid-batch aborts the open transaction ([ADR-0006](./adr/0006-abort-on-revoke.md)); the new owner resumes from the last committed offsets.

### What you must do

- **Read downstream with `isolation.level=read_committed`.** Otherwise consumers see outputs of aborted transactions. This applies to every consumer of your output topics, including other Streams Apps.
- **Keep Processor side effects idempotent.** Only Kafka writes and State Store writes are transactional. An HTTP call or database write inside a Processor can happen again if the batch is retried.
- Use a Kafka cluster that supports transactions (see [broker support](./operations.md#broker-version-support)); with fewer than three brokers set `transaction.state.log.replication.factor` and `transaction.state.log.min.isr` accordingly, as for any transactional client.

### Caveats

#### Async Processors stretch transactions

A Task awaits an async Processor before handling the next record, and the whole batch is one open Kafka transaction. Slow I/O inside a Processor (an HTTP call, a database query) lengthens the transaction. Consequences:

- `read_committed` readers don't see the batch's outputs until it commits, so end-to-end latency grows by the batch's processing time.
- A long-open transaction holds back the last stable offset of its partitions, and with it the readers behind it, including consumer lag in [metrics](./observability.md#metrics).
- A transaction that stays open longer than the producer's transaction timeout is aborted by the broker, and the batch cannot commit.

Keep async work fast, bound its latency with timeouts, lower `commitMaxRecords` so batches stay short, and use the DSL (synchronous) where you can.

#### Disposable disks mean a full Restore

A persistent State Store restores only the Changelog after its Checkpoint. When local disks are disposable (containers without persistent volumes, for example Kubernetes without a PersistentVolume), there is no Checkpoint, and **every start is a full Restore** of every store of every Task the Worker owns. That is correct but slow for large stores. See [operations](./operations.md#disposable-disks).

#### Transactional IDs include the App Process ID

Transactional IDs are `<applicationId>-<processId>-worker-<n>` ([ADR-0008](./adr/0008-one-transactional-producer-per-worker.md)). The App Process ID is kept in the state directory and taken again when the process restarts, so the new producers fence the old ones and the broker aborts their open transactions at once. **Without a state directory each start gets a new ID**: after a crash, the dead process's open transaction is only aborted when it times out (the commit interval plus 10 s), and the new owners of its Tasks wait for that (see the `stable-offsets` wait in [metrics](./observability.md#metrics)). Keep the state directory across restarts for fast recovery.

#### A workaround for the client's read_committed

The Kafka client (`@platformatic/kafka`) mis-handled aborted transactions in `read_committed` (platformatic/kafka#467, fix proposed in #470). The library works around it in `src/read-committed.ts`, filtering aborted records per fetch response like the Java client, for both Restore and the input path. It relies on client internals, so it is re-checked on every `@platformatic/kafka` upgrade, and goes away once the client fixes the problem.

#### Produce errors stop the app

Errors writing to Kafka are never given to [error handlers](./error-handlers.md): the app stops, so the guarantee can never be broken silently.

## The crash test suite

The exactly-once claim is backed by a crash test suite against real Kafka ([ADR-0011](./adr/0011-crash-test-suite-backs-exactly-once.md)). It runs a stateful app, SIGKILLs Workers at random times (and forces rebalances) while input keeps arriving, and compares outputs and State Stores exactly against a reference: nothing missing, no duplicates, and the SQLite stores and Changelogs hold the exact final values. It runs nightly in CI (`npm run test:crash`); see the README for its settings.

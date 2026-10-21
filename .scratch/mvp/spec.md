# MVP spec: kstreams-ts

Status: ready-for-agent

M1 issues: [issues/](./issues/). Terms are defined in [CONTEXT.md](../../CONTEXT.md). Decisions are in [docs/adr/](../../docs/adr/).

## Problem Statement

I'm a Node.js/TypeScript developer and my team runs Kafka. Many of our services need **stateful** stream processing: counts per key, aggregates over time windows, enriching one topic with the latest value from another. The Node ecosystem has nothing reliable for this. Kafka clients (KafkaJS, Confluent's client, `@platformatic/kafka`) give us consume and produce, and the only stream-processing library (`kafka-streams`) is abandoned, keeps state in memory only, and has no fault tolerance.

So today we either:
- write processing by hand, with state in Redis or Postgres, and accept duplicates and lost updates on crashes and rebalances,
- or bring in a JVM stack (Kafka Streams, Flink) or a streaming database just for the stateful part, which means new languages, deployments and expertise.

We want partition-aware, fault-tolerant state and exactly-once processing in the language and runtime we already use.

## Solution

A TypeScript library for Node.js (≥24) where I define a **Streams App** with a typed DSL or a lower-level **Processor API** and run it on as many processes and **Workers** as I like. The library:

- splits input partitions into **Tasks**, each owning its own **State Stores** (SQLite on local disk),
- records every store write to a compacted **Changelog** topic, which is the source of truth, so state survives crashes, restarts and rebalances through **Restore**,
- offers **exactly-once** processing by lining up each SQLite transaction with a Kafka transaction, plus a cheaper **at-least-once** mode,
- handles event time, **Stream Time**, **Grace Period** and tumbling windows,
- ships a public in-memory **Test Driver** so I can unit-test my processing without Kafka,
- backs its exactly-once claim with a crash test suite against real Kafka that runs in CI.

## User Stories

### Defining a Streams App

1. As a Node developer, I want to create a Streams App with a single Application ID, brokers and a Delivery Guarantee, so that I can start processing with minimal configuration.
2. As a Node developer, I want the consumer group, Internal Topic names, transactional IDs and state directory to all derive from the Application ID, so that I can't misconfigure them independently.
3. As a Node developer, I want to declare a Stream from a topic with typed key and value Serdes, so that every operator downstream has correct types.
4. As a Node developer, I want `filter`, `map` and `peek` on a Stream, so that I can express stateless transformations concisely.
5. As a Node developer, I want `map` to be able to change both the key type and the value type with types inferred through the chain, so that I never write type annotations by hand.
6. As a Node developer, I want to write a Stream to an output topic with its own Serdes, so that results reach other services.
7. As a Node developer, I want built-in string, JSON, number and raw-bytes Serdes, so that common formats need no extra code.
8. As a Node developer, I want to plug in my own Serde through a small typed interface, so that I can use Avro, Protobuf or custom formats.
9. As a Node developer, I want null values treated as tombstones throughout, so that deletes work as Kafka users expect.

### Synchronous DSL functions

10. As a Node developer, I want my code to fail to compile if I pass an async function to a DSL operator, with a message telling me to use a Processor, so that I catch the mistake in my editor.
11. As a JavaScript developer without TypeScript, I want the app to refuse to build if I pass an `async` function to the DSL, so that I find out before it connects to Kafka.
12. As a Node developer, I want a DSL function that returns a Promise to stop the app on the first record with `AsyncNotAllowedError`, so that a Promise is never silently treated as a value or sent downstream.
13. As an operator, I want `AsyncNotAllowedError` to skip my dead-letter handler, so that a code bug never quietly routes every record to the dead-letter topic.

### Processor API

14. As an advanced user, I want to register a Processor with access to a context, so that I can write custom per-record logic the DSL doesn't cover.
15. As an advanced user, I want to pass records to downstream Processors from my Processor, so that I can build custom processing graphs.
16. As an advanced user, I want my Processor to be able to be async (e.g. for HTTP enrichment), with records awaited one after another per Task, so that I can do I/O without breaking per-key ordering.
17. As an advanced user, I want to read and write named State Stores from a Processor, so that I can implement custom stateful logic.
18. As an advanced user, I want to schedule timers on Stream Time or wall-clock time, so that I can emit periodic or timeout-based results.

### Stateful processing

19. As a Node developer, I want `groupByKey` followed by `aggregate`, `count` or `reduce`, so that I can keep running totals per key.
20. As a Node developer, I want to name the State Store behind an aggregation, so that its Changelog topic has a stable, readable name.
21. As a Node developer, I want the result of an aggregation to be a Table that I can turn back into a Stream and write out, so that downstream services get the updates.
22. As a Node developer, I want deleting a key from a store to write a tombstone to the Changelog, so that deletes survive Restore.

### Windows and time

23. As a Node developer, I want tumbling windows with a size and a Grace Period, so that I can compute per-minute (or per-hour) aggregates.
24. As a Node developer, I want windows driven by Event Time taken from record timestamps, so that results reflect when events happened, not when they were processed.
25. As a Node developer, I want records later than the Grace Period to be dropped (and counted in metrics), so that windows eventually close and I can see how much late data I'm losing.
26. As a Node developer, I want output records to take the Event Time of the record that produced them, and window results to take the window end, so that timestamps stay meaningful downstream.
27. As an operator, I want window state kept only for window size plus Grace Period, so that stores don't grow without limit.

### Fault tolerance and exactly-once

28. As an operator, I want every State Store write recorded to a compacted Changelog, so that state can always be rebuilt after a disk loss.
29. As an operator, I want local SQLite stores to survive restarts and only replay the Changelog after their Checkpoint, so that restarts are fast.
30. As an operator, I want a store with a missing or unclean Checkpoint to be wiped and fully rebuilt automatically, so that corrupted local state never leaks into results.
31. As an operator, I want a Task being restored paused while the Worker's other Tasks keep processing, so that one large store doesn't stall everything.
32. As an operator, I want `guarantee: 'exactly-once'` to make outputs, store updates and consumed offsets commit atomically, so that crashes and rebalances never produce duplicates or lose updates.
33. As an operator, I want an at-least-once mode with the same engine, so that I can trade duplicates for lower latency and overhead when I don't need exactly-once.
34. As a downstream consumer, I want outputs produced under exactly-once to be readable with `read_committed`, so that I never see results of aborted transactions.
35. As an operator, I want a rebalance that revokes a Task mid-batch to abort the open transaction safely, so that the new owner resumes from a consistent point.
36. As an operator, I want a graceful shutdown (SIGTERM) to finish and commit the current batch, write Checkpoints and leave the group within a timeout, so that deploys don't cause rework or rebalance delays.
37. As an operator, I want to control the commit interval (default 100 ms), so that I can trade latency against throughput.

### Scaling

38. As an operator, I want to set a number of Workers per process, each a consumer-group member, so that I can use all CPU cores without running more processes.
39. As an operator, I want `workers: 0` to run everything in the main thread, so that small apps and debugging stay simple.
40. As an operator, I want to scale out by starting more processes with the same Application ID, so that Kafka spreads Tasks across all of them.
41. As an operator, I want cooperative-sticky assignment, so that rebalances move as few Tasks as possible and keep their local state.
42. As a Node developer, I want the app to check at startup that joined or Co-partitioned input topics have the same partition count, so that I get a clear error instead of silently wrong results.

### Topics and operations

43. As an operator, I want Internal Topics (Changelogs) created automatically, compacted and with matching partition counts, so that I don't create them by hand.
44. As an operator on a locked-down cluster, I want to turn off auto-creation and get a clear error listing exactly which topics to create, so that I can ask my platform team for them.
45. As an operator, I want the app to fail clearly if an existing Internal Topic has the wrong partition count, so that misconfiguration never corrupts state.
46. As an operator, I want a configurable Changelog replication factor (default 3), so that I can use 1 locally and 3 in production.

### Error handling

47. As an operator, I want deserialization and processing errors to stop the app by default, so that bad data or bugs are never silently ignored.
48. As an operator, I want to choose a ready-made dead-letter handler that sends failed records to a topic, in the same transaction under exactly-once, so that one bad record doesn't stop the pipeline.
49. As an operator, I want produce errors to always stop the app, so that the Delivery Guarantee can never be broken silently.

### Observability

50. As an operator, I want `app.state` to report the Application State (created, rebalancing, restoring, running, stopping, stopped, error), so that I can wire it to readiness and liveness probes.
51. As an operator, I want typed events for state changes and errors, so that I can log and alert on them.
52. As an operator, I want metrics for lag, restore progress, commit duration, aborts, dropped late records and throughput through a metrics interface, so that I can feed my monitoring system.

### Testing my app

53. As a Node developer, I want a public Test Driver that runs my Streams App in memory, so that I can unit-test my processing without Kafka.
54. As a Node developer, I want to feed records with optional timestamps into the Test Driver and read typed outputs, so that tests read like examples.
55. As a Node developer, I want the Test Driver to go through my real Serdes, so that serialization bugs show up in unit tests.
56. As a Node developer, I want to inspect State Stores and advance time in the Test Driver, so that I can test aggregations, windows and timers.

### Trust

57. As a team evaluating the library, I want a published crash test suite that kills Workers mid-processing against real Kafka and checks outputs exactly, so that I can trust the exactly-once claim.
58. As a team evaluating the library, I want a documented minimum broker version and tested broker versions, so that I know whether it works with our cluster.

## Implementation Decisions

### Platform

- Node ≥24 LTS only. ESM-only, strict TypeScript, a single package. Apache-2.0.
- Kafka client: `@platformatic/kafka`, used directly with no client abstraction layer (ADR-0004). Its transactional offset commit sends the consumer's generation ID and member ID, which gives KIP-447 fencing.
- State Stores are a pluggable interface, with an in-memory version and SQLite through `better-sqlite3` as the default durable store. Switch to `node:sqlite` once it's stable (ADR-0005).
- Serdes: a minimal typed interface. Schema Registry support comes later as a separate package.

### API

- The entry point is `createStreamsApp(config)`, which returns a Streams App. Config: Application ID, brokers, Delivery Guarantee (`exactly-once` or `at-least-once`), worker count (0 = main thread), state directory, commit interval, auto-create flag for Internal Topics, error handlers, and metrics sink.
- The DSL is idiomatic TypeScript and borrows Kafka Streams concepts, not its names (ADR-0001): stream, filter, map, peek, to, groupByKey, aggregate, count, reduce, window (tumbling, with grace), toStream. It compiles down to the Processor API.
- Processor API: a named Processor is created from a factory that receives a context. The context offers forwarding to children, State Store access by name, and timer scheduling (Stream Time or wall-clock time). Async Processors are awaited one after another per Task.
- DSL functions must be synchronous, enforced in three places (ADR-0012): the compile-time return type check, rejecting `async` functions when the app is built, and a runtime check for a returned thenable that throws `AsyncNotAllowedError`. That error skips the error handlers.
- Application ID derives the consumer group, Internal Topic names (`<applicationId>-<store>-changelog`), transactional IDs (per Worker), and the state directory (per Application ID and Task). Renaming it resets state.
- The Application State lifecycle is: created → rebalancing ⇄ restoring ⇄ running → stopping → stopped, or error from any state.
- The Test Driver is public API: input topics accept key, value and optional timestamp; output topics return typed records; stores can be inspected; time can be advanced.

### Engine

- **Task** = one partition number across all Co-partitioned input topics. Partition counts are checked at startup.
- **Worker** = one consumer-group member with its own consumer, one transactional producer covering all its Tasks, and its State Stores (ADR-0007, ADR-0008). Kafka does the load balancing, and no records are sent between threads.
- **Commit cycle**: a batch per Worker, closed by time or count (default 100 ms). Under exactly-once, the SQLite transaction stays open for the length of the Kafka transaction: process the batch → produce outputs and Changelog entries → add consumed offsets (only the last record per partition) → commit Kafka → commit SQLite with an updated Checkpoint (ADR-0002).
- **Revoke** under exactly-once aborts the Kafka transaction and rolls back SQLite, because the client has no revoke hook the app can make wait (ADR-0006). Under at-least-once, commit on revoke when possible.
- **Graceful shutdown** commits the current batch, writes Checkpoints, closes stores and leaves the group, falling back to abort if the timeout runs out.
- **Restore**: SQLite files persist in the state directory with Checkpoints, and a missing or unclean Checkpoint means wipe and rebuild. A separate per-Worker restore consumer, outside the group, reads the Changelog with `read_committed`. Only the Task being restored is paused (ADR-0009).
- **Assignment**: cooperative-sticky in the MVP. A state-aware assignor using per-member join metadata comes later.
- **Store key encoding**: deterministic bytes that preserve order regardless of the key Serde, so range scans (windows) work.
- **Internal Topics** are created automatically by default: compacted, with source partition counts and a configurable replication factor. A wrong partition count on an existing topic is a fatal error. With auto-creation off, the app checks the topics and lists any that are missing.
- **Errors** (ADR-0010): pluggable deserialization and processing handlers, fail-fast by default. A `deadLetter(topic)` handler is provided and writes in the same transaction under exactly-once. Produce errors always fail fast.
- **Time**: Event Time from record timestamps, Stream Time per Task (only moves forward), Grace Period per window. Late records are dropped and counted.
- **Observability**: a typed event emitter and a metrics interface in the core. An OpenTelemetry adapter comes later as a separate package.
- **Internal formats** follow our own documented scheme. No compatibility with Java Kafka Streams formats (ADR-0003).

### Defaults (confirmed 2026-10-21)

- Minimum broker: Kafka ≥3.6 with the classic group protocol. KIP-848 is opt-in on Kafka 4.x. CI tests the latest 3.9 and 4.x.
- Output record timestamps inherit the input's Event Time. Window results get the window end.
- `delete(key)` writes a Changelog tombstone.
- Window retention = window size plus Grace Period, then entries are dropped and tombstoned.
- Changelog replication factor defaults to 3 and is configurable (1 for dev setups).

### Milestones

Each one ends with something you can demo.

1. **M1: stateless pipeline.** filter, map, to end to end, `workers: 0`, at-least-once, Test Driver skeleton.
2. **M2: State.** Key-value State Stores (in-memory and SQLite), Changelog, Checkpoint, Restore. `count()` survives a crash.
3. **M3: exactly-once.** Transactions lined up with SQLite, abort on revoke, graceful shutdown commit, first crash tests.
4. **M4: multiple Workers.** Worker threads, cooperative-sticky assignment, per-Task pausing during Restore. Crash tests extended to rebalances.
5. **M5: time.** Event Time, Stream Time, timers, tumbling windows, Grace Period, retention.
6. **M6: hardening.** Error handlers, auto-created Internal Topics, observability, docs, 0.1 release, Platformatic pitch.

Exactly-once (M3) comes before multiple Workers (M4) on purpose: prove exactly-once on a single thread first, then prove it survives rebalances.

## Testing Decisions

- **What makes a good test**: it drives the system only through public surfaces and checks only externally visible results (output records, State Store contents read through public APIs, Application State, events and metrics). Tests never check internal call order, private fields or how the engine is structured inside, so the engine can be refactored freely.
- **Seam 1: the public Streams App API through the Test Driver** (in memory, no Kafka). This is the main seam, and most tests live here: DSL operators and type inference, Processor API, Serdes, the three-layer synchronous check (including type-level tests), aggregations, windows, timers, Stream Time and Grace Period behavior, and error handler routing.
- **Seam 2: the same public API against real Kafka** (single broker, the latest 3.9 and 4.x). Tests connect to a broker given by an environment variable. Locally, a script downloads Kafka and runs a KRaft broker on Java 17+, because Docker is not available on the development machine. CI can use a Kafka service container. It covers integration tests (topic auto-creation, Restore, rebalances, graceful shutdown) and the **crash test suite** (ADR-0011). Workers run as child processes and are killed from outside with SIGKILL at random times, with no fault-injection hooks inside the engine. Each run compares outputs and State Stores exactly against a reference run: nothing missing, and no duplicates under exactly-once. Precise timings (e.g. between the Kafka commit and the SQLite commit) are covered by running many randomized iterations, not targeted hooks. It runs nightly in CI, and no release claims exactly-once unless it passes.
- **Seam 3: a State Store contract suite.** One suite runs against every store implementation (in-memory and SQLite), because State Stores are a public, pluggable interface. It covers get, put and delete, range scans in key order across encoded keys, transactional rollback, and Checkpoint persistence.
- **No mocking of the Kafka client** anywhere. ADR-0004 rules out a client abstraction, and behavior that depends on Kafka is tested against a real broker.
- **Prior art**: none in this repo yet, since it's new. External references: Kafka Streams' `TopologyTestDriver` (for the shape of seam 1) and Jepsen-style crash testing (for seam 2).

## Out of Scope

- **Comes in v1, not the MVP:** Repartition Topics (`groupBy` with a new key), joins (stream-table and stream-stream), hopping and session windows.
- **v1.x and later:** a state-aware partition assignor, standby replicas, reading stores from outside the app, a Schema Registry package, an OpenTelemetry adapter, switching to `node:sqlite`.
- **Never:** compatibility with Java Kafka Streams formats (ADR-0003), Bun and Deno runtimes, mocking Kafka for engine tests.

## Further Notes

- **Goal and ambition**: start as a learning project but design for adoption, with a public API from day one and real correctness testing. Test demand after M6 (pitch to Platformatic, publish a write-up) and decide by month two whether to go further.
- **Known client constraint**: `@platformatic/kafka`'s `addOffset` makes one call per message, so the engine adds only the last offset per partition per batch.
- **Async Processors stretch transactions**: slow I/O inside a Processor lengthens the open transaction and delays when `read_committed` readers see results. This belongs in the user docs.
- **Disposable disks** (e.g. Kubernetes without persistent volumes) mean a full Restore on every start. That's correct, but slow for large stores, and needs documenting.

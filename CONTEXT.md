# kstreams-ts

A TypeScript library for partition-aware, stateful stream processing on Kafka for Node.js, with fault-tolerant state and exactly-once processing. Inspired by Kafka Streams' concepts, not its API.

## Language

### Application

**Streams App**:
The in-process object, defined by an Application ID, that runs one or more Workers; many processes running the same Streams App form one application.
_Avoid_: App, client, instance, topology

**App Module**:
The module that exports a Streams App definition without side effects, so that each Worker can import it and rebuild the same app.
_Avoid_: Entry file, worker script, bootstrap

**Application ID**:
The single name an application is identified by. Its consumer group, Internal Topic names, transactional IDs and state directory all derive from it, so changing it creates a new application with empty state.
_Avoid_: App name, service name, group ID

**Application State**:
The lifecycle phase a running application reports: created, rebalancing, restoring, running, stopping, stopped or error.
_Avoid_: Status, health, mode

### Processing model

**Processor**:
A user-defined unit of per-record logic that may read and write State Stores and schedule timers.
_Avoid_: Handler, transformer, function

**Processor API**:
The low-level programming surface for wiring Processors and State Stores together; the foundation everything else is built on.
_Avoid_: Core API, low-level DSL

**DSL**:
The typed, chainable high-level API (stream, table, aggregate, window, join) compiled down to the Processor API.
_Avoid_: Fluent API, builder API

**Stream**:
An unbounded sequence of keyed records where each record is an independent event.
_Avoid_: KStream, flow, pipe

**Table**:
A keyed view where each record is an upsert of the latest value for its key.
_Avoid_: KTable, map, materialized view

**Task**:
The unit of processing and state ownership: one partition number across all co-partitioned input topics (e.g. `orders-3` + `customers-3`), owned by exactly one Worker at a time.
_Avoid_: Partition, job, shard

**Co-partitioned**:
Describes input topics that share a partition count and keying, so a key's records from all of them land in the same Task.
_Avoid_: Aligned, matched

**Worker**:
A worker thread (or the main thread when `workers: 0`) that is its own consumer-group member and owns its Tasks, State Stores and producer.
_Avoid_: Instance, node, thread, consumer

### Time

**Event Time**:
The timestamp a record carries, describing when the event happened.
_Avoid_: Record time, created-at

**Stream Time**:
The highest Event Time a Task has seen so far; it only moves forward and drives windows and Stream Time timers.
_Avoid_: Watermark, current time

**Grace Period**:
How far behind Stream Time a record can arrive and still be included in its window; records later than this are dropped.
_Avoid_: Lateness, allowed delay

### State

**State Store**:
Durable keyed state owned by exactly one Task at a time, restorable from its Changelog.
_Avoid_: Cache, DB, state

**Changelog**:
The compacted Kafka topic that records every write to a State Store; it is the source of truth, and the local store is only a derived copy.
_Avoid_: Backup topic, WAL

**Restore**:
Rebuilding a State Store from its Changelog before its Task resumes processing.
_Avoid_: Recovery, rehydration, replay

**Checkpoint**:
The Changelog offset a local State Store has applied up to; Restore replays only what comes after it.
_Avoid_: Snapshot, savepoint, bookmark

**Internal Topic**:
A topic the library creates and owns for an application: a Changelog or a Repartition Topic.
_Avoid_: System topic, private topic

**Repartition Topic**:
An Internal Topic that re-keys records so that records with the same new key reach the same Task.
_Avoid_: Shuffle topic, through topic

### Testing

**Test Driver**:
An in-memory harness, shipped as part of the public API, that runs an application's Processors and State Stores without Kafka. You feed records in, read outputs and stores out, and advance time.
_Avoid_: Mock, simulator, fake broker

### Guarantees

**Delivery Guarantee**:
The processing contract an application runs under: at-least-once or exactly-once.
_Avoid_: Semantics, consistency mode

**Serde**:
A typed pair of serializer and deserializer for record keys or values.
_Avoid_: Codec, converter, marshaller

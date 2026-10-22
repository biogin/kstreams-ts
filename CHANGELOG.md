# Changelog

## 0.1.0

First release, a beta: the public API may still change before 1.0.

### Added

- Streams App with an Application ID, configuration for brokers, state directory, Delivery Guarantee, commit settings, timeouts (incl. `transactionTimeoutMs`), produce request size (`maxProduceBytes`, sends are split), Changelog replication and Internal Topic auto-creation.
- Typed DSL: `stream`, `filter`, `map`, `mapValues`, `peek`, `process`, `to`, `groupByKey`, `count`, `aggregate`, `reduce`, `Table.toStream`; synchronous DSL functions enforced by types, at build time and at run time (`AsyncNotAllowedError`).
- Tumbling windows with a Grace Period, Stream Time and late-record dropping.
- Processor API: Processors (sync or async), `forward`, State Store access, Stream Time and wall-clock timers.
- State Stores: SQLite (default, with a state directory), in memory, and a pluggable `StateStore` interface (custom stores implement `clear()` too); Changelogs, Checkpoints and Restore that doesn't block the Worker's other Tasks. Store keys are null-free and used as their key Serde gives them back.
- Record timestamps must be safe integers (milliseconds) in both the Test Driver and on Kafka.
- Exactly-once processing (one Kafka transaction per batch) and at-least-once processing.
- Worker threads rebuilt from an App Module, scale-out across processes with cooperative-sticky assignment, graceful shutdown.
- Error handlers (`deadLetter`, custom handlers); fail-fast by default.
- Observability: Application State, typed events, a metrics sink with ten metrics, including consumer lag and Restore progress.
- Test Driver: an in-memory harness with Serdes, State Store reads, wall-clock time and metrics.
- User guide in `docs/`, type-checked examples in `examples/`, and a crash test suite for exactly-once.

### Requirements

- Node.js 24 or later; ESM only.
- Apache Kafka 3.6 or later (classic consumer group protocol); tested on 3.9 and 4.x.

### Known limitations

See [docs/limitations.md](./docs/limitations.md): no joins, Repartition Topics (so `groupByKey` after `map` or `process` is refused) or Table sources yet; no standby replicas; the Kafka client's `read_committed` is worked around in `src/read-committed.ts` until platformatic/kafka#470 ships.

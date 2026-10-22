# Known limitations (0.1)

**Not there yet**

- Reading a topic as a Table, joins (stream-table and stream-stream), `groupBy` with a new key and so Repartition Topics, hopping and session windows. Without them nothing is repartitioned, so `groupByKey()` on a re-keyed Stream (after `map` or `process`) throws when the app is built, rather than aggregating the same key separately in several Tasks. Write the re-keyed records to a topic keyed by the new key and read it back (see [the DSL](./dsl.md#aggregations)); use `mapValues` when only the value changes.
- Input topics with different partition counts: in 0.1 **all input topics of an app must be Co-partitioned** (the same partition count, and keyed the same way), because a Task reads one partition number from each of them. The app refuses to start otherwise. This includes a topic you write re-keyed records to and read back.
- Standby replicas and a state-aware partition assignor: a Task that moves to a Worker without its local stores does a full Restore.
- Reading State Stores from outside the app (interactive queries). Use the Test Driver in tests, or write the Table's updates to a topic.
- Configurable window retention: it is size plus Grace Period.
- A Schema Registry package, an OpenTelemetry adapter (the metrics sink is the integration point), `node:sqlite` as a store.

**Behaviours to keep in mind**

- DSL functions must be synchronous; async work goes in a Processor, and async Processors [stretch transactions](./delivery-guarantees.md#async-processors-stretch-transactions).
- Windows expire only when new records arrive for their Task.
- Timers are not persisted: after a restart, or for a Task recreated by a rebalance, a Processor schedules them again. A Task retained across a cooperative rebalance keeps its timers.
- A Task restores when its first record arrives, so a quiet Task starts late; there is no limit on concurrent Restores.
- Disposable disks mean a [full Restore on every start](./operations.md#disposable-disks).
- Without a `stateDir`, State Stores are in memory and each start gets a random App Process ID, so a crashed process's open transactions are only aborted by their timeout (see [delivery guarantees](./delivery-guarantees.md#transactional-ids-include-the-app-process-id)).
- One `stateDir` per App Process ([ADR-0017](./adr/0017-one-state-directory-per-app-process.md)): processes can't share one. A process starting on a `stateDir` that another live process (or another Streams App of the same process) uses fails to start. The lock is an OS file lock, so the `stateDir` must be on a file system with POSIX advisory locks (a local file system; not NFS without locking).
- At-least-once: when a rebalance takes a Task away, the uncommitted work of its lost partitions is redone by the new owner, so their outputs since the last commit can be duplicated.
- A skipped record (error handler) does not advance Stream Time, so it expires no windows and moves no timer.
- Exactly-once relies on a workaround for the Kafka client's `read_committed` (`src/read-committed.ts`) until platformatic/kafka#470 ships.
- Java Kafka Streams formats and topology names are not compatible, and will not be ([ADR-0003](./adr/0003-no-java-kafka-streams-compatibility.md)). Node.js 24 or later only (no Bun or Deno).
- 0.1 is a beta: the API may change before 1.0.

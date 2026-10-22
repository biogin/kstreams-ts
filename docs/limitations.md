# Known limitations (0.1)

**Not there yet**

- Reading a topic as a Table, joins (stream-table and stream-stream), `groupBy` with a new key and so Repartition Topics, hopping and session windows. `map` followed by `groupByKey` does **not** repartition: if the new key isn't the key the input topic is partitioned by, the same key can reach several Tasks and be aggregated separately. Re-key only when the topic is already partitioned that way, or write to an intermediate topic and read it back as a second app.
- Standby replicas and a state-aware partition assignor: a Task that moves to a Worker without its local stores does a full Restore.
- Reading State Stores from outside the app (interactive queries). Use the Test Driver in tests, or write the Table's updates to a topic.
- Configurable window retention: it is size plus Grace Period.
- A Schema Registry package, an OpenTelemetry adapter (the metrics sink is the integration point), `node:sqlite` as a store.

**Behaviours to keep in mind**

- DSL functions must be synchronous; async work goes in a Processor, and async Processors [stretch transactions](./delivery-guarantees.md#async-processors-stretch-transactions).
- Windows expire only when new records arrive for their Task.
- Timers are not persisted across restarts and rebalances; a Processor schedules them again.
- A Task restores when its first record arrives, so a quiet Task starts late; there is no limit on concurrent Restores.
- Disposable disks mean a [full Restore on every start](./operations.md#disposable-disks).
- A skipped record (error handler) does not advance Stream Time.
- A zombie Worker whose open transaction holds only timer-driven Changelog writes could still commit them after the new owner has restored. This is rare; it is a known gap in the fencing and is on the list for a fix.
- Exactly-once relies on a workaround for the Kafka client's `read_committed` (`src/read-committed.ts`) until platformatic/kafka#470 ships.
- Java Kafka Streams formats and topology names are not compatible, and will not be ([ADR-0003](./adr/0003-no-java-kafka-streams-compatibility.md)). Node.js 24 or later only (no Bun or Deno).
- 0.1 is a beta: the API may change before 1.0.

# Streams App and configuration

A **Streams App** is the in-process object, identified by an **Application ID**, that holds your processing definition and runs it. Create it with `createStreamsApp`, define streams and processors on it, then `start()` it. Many processes running the same Streams App (same Application ID, same definition) form one application.

```ts
import { createStreamsApp, serdes } from 'kstreams-ts'

const app = createStreamsApp({
  applicationId: 'page-views',
  brokers: ['localhost:9092'],
  stateDir: './state',
  guarantee: 'exactly-once',
  changelogReplicationFactor: 1, // 3 (the default) needs at least three brokers
})
```

Define the whole app before you start it.

## Application ID

The Application ID is the one name everything derives from: the consumer group, the [Internal Topic](./internal-topics.md) names, the transactional IDs and the state directory. Allowed characters are ASCII letters, digits, `.`, `_` and `-`. **Changing it creates a new application with empty state** that reads its input topics from the beginning. Treat it like a database name.

## Configuration

| Option | Default | Meaning |
| --- | --- | --- |
| `applicationId` | required | See above. |
| `brokers` | none | Bootstrap brokers (`host:port`). Needed to `start()`; the Test Driver doesn't use them. |
| `stateDir` | none | Where local State Stores live, under `<stateDir>/<applicationId>/<task>/`. Without it, stores are in memory and every start is a full Restore. See [operations](./operations.md#state-directory). |
| `guarantee` | `'at-least-once'` | `'at-least-once'` or `'exactly-once'`. See [delivery guarantees](./delivery-guarantees.md). |
| `workers` | `0` | Number of Workers in worker threads, or `{ count, module }`. `0` runs one Worker in the main thread. See [workers](./workers.md). |
| `commitIntervalMs` | `100` | How long a Worker collects records into a batch before it produces the outputs and commits. Lower means lower latency and more overhead. |
| `commitMaxRecords` | `1000` | A batch commits early at this many input records. It also bounds how many records a Worker holds for Tasks that are still restoring. |
| `maxProduceBytes` | `900000` | The most bytes of records one produce request carries; a larger batch is written in several requests, in order. Keep it below the broker's (or topics') `max.message.bytes` (default about 1 MB). |
| `sessionTimeoutMs` | `45000` | How long the consumer group waits for a silent member before moving its Tasks. |
| `shutdownTimeoutMs` | `30000` | How long `stop()` may take to finish and commit the current batch before giving it up. |
| `changelogReplicationFactor` | `3` | Replication factor of the Changelogs the app creates. Use `1` on a single-broker cluster. |
| `autoCreateInternalTopics` | `true` | Create missing Internal Topics on start, or only check them. See [Internal Topics](./internal-topics.md). |
| `errorHandlers` | none (fail-fast) | See [error handlers](./error-handlers.md). |
| `metrics` | none | A metrics sink. See [observability](./observability.md). |

## Lifecycle

```ts
import app from './quick-start.js'

process.on('SIGTERM', () => void app.stop())
process.on('SIGINT', () => void app.stop())

app.on('error', ({ error }) => console.error('Streams App failed', error))
await app.start()
```

- `start()` connects, starts the Workers, and resolves once every Worker has joined the consumer group. The app may still be rebalancing or restoring then; watch `app.state` or the `state` event for `running`. It rejects if there are no brokers, the app reads no topic, it was started before (an app starts once), or a Worker can't start.
- `stop()` finishes and commits each Worker's current batch, closes the State Stores and leaves the group, so its Tasks move to other members at once. If that takes longer than `shutdownTimeoutMs`, the batch is given up (under exactly-once its transaction is aborted and the records are processed again later). If the app already stopped on its own because of an error, `stop()` rejects with that error.
- Without a handler the first deserialization or processing error stops the app (fail-fast, [ADR-0010](./adr/0010-fail-fast-error-handling.md)): the state becomes `error` and the `error` event carries the root cause.

Always handle `SIGTERM` as above. A process that is killed instead of stopped leaves its transaction open until the broker times it out, and its Tasks restore more.

The states and events are described in [observability](./observability.md).

## Serdes

A **Serde** converts keys and values to and from bytes. Built in: `serdes.string`, `serdes.json<T>()` (the type is trusted, not validated), `serdes.number` (an 8-byte double) and `serdes.bytes`. Implement `Serde<T>` (`serialize` and `deserialize`) for Avro, Protobuf or anything else. A Serde never sees `null`; a null value is a **Tombstone** and passes through as is. If `deserialize` throws, the app reports a `DeserializationError` to the [deserialization error handler](./error-handlers.md).

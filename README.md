# kstreams-ts

Partition-aware stateful stream processing on Kafka for Node.js, with fault-tolerant State Stores and exactly-once processing. Inspired by Kafka Streams' concepts, not its API.

Capitalized terms (Streams App, Task, Worker, State Store, Changelog, Restore, ...) are defined in the glossary, [CONTEXT.md](./CONTEXT.md).

- **Typed DSL and a Processor API.** `filter`, `map`, `groupByKey`, `count`, `aggregate`, `reduce` and tumbling windows, on top of Processors you can also write yourself.
- **Fault-tolerant state.** Every State Store write goes to a compacted Changelog topic; local state is SQLite. State survives crashes, restarts and rebalances through Restore.
- **Exactly-once or at-least-once.** Outputs, store updates and consumed offsets commit atomically in one Kafka transaction. The claim is backed by a [crash test suite](./docs/delivery-guarantees.md#the-crash-test-suite) that runs nightly.
- **Scales on one machine and out.** Worker threads per process, more processes with the same Application ID.
- **Test without Kafka.** A public in-memory Test Driver.

> 0.1 is a beta: the public API may still change before 1.0. See [known limitations](./docs/limitations.md).

## Install

Requires Node.js 24 or later and a Kafka cluster, version 3.6 or later; CI tests Kafka 3.9 and 4.x ([broker support](./docs/operations.md#broker-version-support)). The package is ESM only.

```sh
npm install kstreams-ts
```

Source and issues: [github.com/biogin/kstreams-ts](https://github.com/biogin/kstreams-ts).

## Quick start

Count page views per user (the key of the `page-views` topic), exactly once, and write every new count to another topic:

```ts
import { createStreamsApp, serdes } from 'kstreams-ts'

const app = createStreamsApp({
  applicationId: 'page-views',
  brokers: ['localhost:9092'],
  stateDir: './state',
  guarantee: 'exactly-once',
  changelogReplicationFactor: 1, // 3 (the default) needs at least three brokers
})

const view = serdes.json<{ path: string }>()

app
  .stream('page-views', { key: serdes.string, value: view })
  // A record's value can be null (a deletion, or tombstone): skip those.
  .filter((_userId, value): value is { path: string } => value !== null)
  .groupByKey()
  .count({ store: 'views-per-user' })
  .toStream()
  .to('views-per-user', { key: serdes.string, value: serdes.number })

export default app
```

Start it from a second file. The file that builds the app only exports it, with no side effects, so it can later serve as the [App Module](./docs/workers.md#the-app-module) that worker threads import. Stop it gracefully on SIGTERM so the current batch commits and the app leaves the consumer group:

```ts
import app from './quick-start.js'

process.on('SIGTERM', () => void app.stop())
process.on('SIGINT', () => void app.stop())

app.on('error', ({ error }) => console.error('Streams App failed', error))
await app.start()
```

Test it without Kafka, with the [Test Driver](./docs/test-driver.md).

## User guide

1. [Streams App and configuration](./docs/streams-app.md)
2. [The DSL](./docs/dsl.md), [windows](./docs/windows.md), and [joins and re-keying today](./docs/joins.md)
3. [The Processor API](./docs/processor-api.md)
4. [State Stores](./docs/state-stores.md)
5. [The Test Driver](./docs/test-driver.md)
6. [Delivery guarantees and exactly-once](./docs/delivery-guarantees.md)
7. [Worker threads, the App Module and scaling out](./docs/workers.md)
8. [Error handlers](./docs/error-handlers.md)
9. [Internal Topics](./docs/internal-topics.md)
10. [Observability: states, events and metrics](./docs/observability.md)
11. [Operations: state directory, disposable disks, broker versions](./docs/operations.md)
12. [Known limitations](./docs/limitations.md)

More: design decisions are in [docs/adr/](./docs/adr/); examples are in [examples/](./examples/); see the [CHANGELOG](./CHANGELOG.md).

## Development

```sh
npm install
npm run typecheck
npm run lint
npm test
```

Every code example in the docs is a verbatim excerpt of a file under `examples/`, which `npm run typecheck` checks, and `npm test` verifies that the docs still match.

Integration tests run against a real broker given by `KSTREAMS_KAFKA_BROKERS` and are skipped without it. `npm run kafka -- start|stop|status|reset` downloads and runs a local single-node Kafka KRaft broker without Docker (needs Java 17+); `KAFKA_VERSION=3.9.1 npm run kafka -- start` picks another version:

```sh
npm run kafka -- start
KSTREAMS_KAFKA_BROKERS=localhost:9092 npm run test:integration
npm run kafka -- stop
```

### Crash test suite

The crash test suite ([ADR-0011](./docs/adr/0011-crash-test-suite-backs-exactly-once.md)) is the evidence behind the exactly-once claim. It runs separately from the integration tests (it is slow and randomized, meant for nightly runs). Each iteration runs a stateful exactly-once app (`count()` and `aggregate()` on SQLite State Stores, several partitions) as one Worker in a child process, SIGKILLs and restarts it at randomized times while input keeps arriving, then checks against a reference computed from the input: every output exactly once and in order (nothing missing, no duplicates), all input consumed, and the SQLite stores on disk and the Changelogs holding the exact final values.

```sh
KSTREAMS_KAFKA_BROKERS=localhost:9092 npm run test:crash
```

The defaults (4 iterations of 5 SIGKILLs) take about 3 minutes. Settings, as environment variables:

- `KSTREAMS_CRASH_ITERATIONS`: number of iterations, each with fresh topics and Application ID (default 4).
- `KSTREAMS_CRASH_KILLS`: SIGKILLs per iteration (default 5).
- `KSTREAMS_CRASH_SEED`: base seed; iteration `i` (from 0) uses seed + `i` (default: random). The seed fixes the plan (partitions, keys, commit settings, input values, kill strategies and delays), not the broker's timing.

The suite also checks Restore's Changelog reader (`createChangelogReader`) against the log.

Each iteration prints its seed, plan and kills. A failure names its seed and the command that reruns that iteration alone, e.g. `KSTREAMS_CRASH_SEED=1234 KSTREAMS_CRASH_ITERATIONS=1 KSTREAMS_CRASH_KILLS=5 npm run test:crash`.

## License

Apache-2.0

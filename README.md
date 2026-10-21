# kstreams-ts

Partition-aware stateful stream processing on Kafka for Node.js, with fault-tolerant State Stores and exactly-once processing. Inspired by Kafka Streams' concepts, not its API.

> Early development. Nothing is published yet.

- Glossary: [CONTEXT.md](./CONTEXT.md)
- Decisions: [docs/adr/](./docs/adr/)
- MVP spec and issues: [.scratch/mvp/spec.md](./.scratch/mvp/spec.md)

## Development

Requires Node ≥24.

```sh
npm install
npm run typecheck
npm run lint
npm test
```

Integration tests run against a real broker given by `KSTREAMS_KAFKA_BROKERS` and are skipped without it. `npm run kafka -- start|stop|status|reset` downloads and runs a local single-node Kafka 4.x KRaft broker without Docker (needs Java 17+):

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

The suite also checks Restore's Changelog reader (`createChangelogReader`) against the log. It once dropped committed records now and then, through the client's read_committed filter (platformatic/kafka#467); reads now go through `src/read-committed.ts`, which filters aborted transactions per fetch response the way the Java client does.

Each iteration prints its seed, plan and kills. A failure names its seed and the command that reruns that iteration alone, e.g. `KSTREAMS_CRASH_SEED=1234 KSTREAMS_CRASH_ITERATIONS=1 KSTREAMS_CRASH_KILLS=5 npm run test:crash`.

## License

Apache-2.0

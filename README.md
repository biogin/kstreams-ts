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

## License

Apache-2.0

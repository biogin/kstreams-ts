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

## License

Apache-2.0

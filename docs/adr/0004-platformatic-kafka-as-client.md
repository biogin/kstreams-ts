# `@platformatic/kafka` is the Kafka client, used directly

We build on `@platformatic/kafka`, with no client abstraction layer in between. It's pure TypeScript with no native addon, so it's safe inside worker threads. It supports transactions, `read_committed`, cooperative-sticky assignment and KIP-848. Committing offsets inside a transaction (`transaction.addOffset`) sends the consumer's `generationId` and `memberId` (`src/clients/producer/producer.ts`, TxnOffsetCommit v3/v4), which gives us KIP-447 fencing. Custom partition assignors can read per-member join metadata, which makes state-aware assignment possible later.

## Considered Options

- `@confluentinc/kafka-javascript`: commercially backed, built on librdkafka. Rejected because the native addon causes friction in worker threads and builds.
- A client abstraction layer: rejected. We'd have one implementation and a leaky abstraction over transaction and rebalance internals.

## Consequences

- The client has no public revoke callback the app can make the rebalance wait on, so we abort on revoke (ADR-0006).
- `addOffset` makes one TxnOffsetCommit call per message, so we call it only for the last record of each partition in a batch.

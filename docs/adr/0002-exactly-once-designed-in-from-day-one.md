# Exactly-once is designed in from day one; at-least-once ships first as a mode

Exactly-once is the reason this library exists, and it dictates the core shape of the engine: State Store commits line up with Kafka transactions, and the Changelog is the source of truth. Bolting it on later would mean rewriting the commit path, so the engine is built around it from the start. At-least-once ships first as a cheaper mode of the same engine (same commit path, without the transaction), for users who don't need exactly-once.

## Consequences

- The Kafka client must support transactional producers, adding consumer offsets to a transaction with group metadata, and `read_committed`. This rules out clients without those features.

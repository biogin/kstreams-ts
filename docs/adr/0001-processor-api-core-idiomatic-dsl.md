# Processor API is the core; the DSL is idiomatic TypeScript, not a Java port

The Processor API (per-record Processors with State Store access and timers) is the foundation we build and test first. The DSL is a thin, fully typed layer compiled down to it. We borrow Kafka Streams' concepts (Stream, Table, State Store, window) but not its class hierarchy or names (`KStream`, `KTable`, `Topology`, `Serdes`). A Java-shaped API would feel familiar to JVM users but drags Java-isms into TypeScript, and the people we're targeting are Node-only teams, not JVM teams.

## Considered Options

- Close Java parity (`KStream`/`KTable`/`Topology`): rejected, poor TypeScript ergonomics, wrong audience.
- DSL only, no public Processor API: rejected, leaves no escape hatch and makes correctness harder to test in isolation.

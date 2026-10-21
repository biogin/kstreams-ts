# Tombstones are visible in the types and reach operators

Records with a null value (Tombstones) are passed to stateless operators and Processors, as in Kafka Streams, and the types say where they can appear: Streams read from topics and Streams produced by `Table.toStream()` are typed `Stream<K, V | null>`. A `filter` type guard removes them and narrows the type, `groupByKey()` produces non-null values because aggregations skip Tombstones (and null keys), and Serdes only ever see non-null values. We chose this over silently skipping Tombstones in operators, which would drop deletes in `map` and hide them from Processors.

## Consequences

- A Processor attached to a topic Stream must accept `V | null`.
- Null keys still reach operators at runtime but are typed `K`. That gap is known and not yet closed.
- Windows and joins must follow the same rule: Tombstones appear in the types wherever they can occur at runtime.

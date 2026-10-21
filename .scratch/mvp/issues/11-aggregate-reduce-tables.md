# 11: `aggregate`/`reduce`, Tables, deletes

**What to build:** A user aggregates or reduces per key into a Table, turns it back into a Stream and writes updates out. Deleting a key produces a tombstone.

**Blocked by:** 10

**Status:** done

- [x] `aggregate`, `reduce`. Table → `toStream()`
- [x] Store `delete` emits a tombstone downstream (and to the Changelog once ticket 13 lands)
- [x] Tested through the Test Driver

Spec: [../spec.md](../spec.md). Glossary: [CONTEXT.md](../../../CONTEXT.md). ADRs: [docs/adr/](../../../docs/adr/)

## Comments

- 2026-10-21: Done on branch `ticket/11`. Public API:
  - **`GroupedStream.aggregate(initializer, aggregator, { serde, store?, storeSupplier? })`** → `Table<K, VA>`. `aggregator(key, value, aggregate)` (Kafka Streams' argument order); `initializer` runs when the key has no aggregate. `serde` (required) is the aggregate Serde for the store and the Table's Stream.
  - **`GroupedStream.reduce(reducer, { serde?, store?, storeSupplier? })`** → `Table<K, V>`. `reducer(aggregate, value, key)` (Array.reduce order); the first value of a key is stored as is. `serde` defaults to the grouped value Serde, which `groupByKey(serdes)` now keeps (`{ key, value }` from the call or the Stream); reduce throws when the app is built if there is none (e.g. after `map`).
  - New exported option types `AggregateOptions<VA>`, `ReduceOptions<V>` (both extend `CountOptions`). Default store names: `aggregate-store-NNNN`, `reduce-store-NNNN`.
  - **`Table.toStream()`** → `Stream<K, V | null>` over the aggregation Processor's updates, carrying the Table's Serdes (so `.to(topic)` needs none). Every update is forwarded with the Event Time of the record that caused it. `count`, `aggregate` and `reduce` share one internal aggregation Processor.
  - **Deletes:** returning `null` from an aggregator or reducer deletes the key from the store and forwards a tombstone `{ key, value: null }`; the key's next record starts from `initializer` again (Kafka Streams behaves the same). The Changelog tombstone is ticket 13: the store `delete` call is where it hooks in.
  - All aggregations skip records with a null key or a tombstone value, like Kafka Streams. Callbacks are wrapped with `synchronous('aggregate' | 'reduce', ...)` and typed with `Sync<R>` (ADR-0012).
- **Tombstone typing decision: honest types where tombstones can appear; operators see tombstones.** Like Kafka Streams' KStream, `filter`/`map`/`peek` and Processors are called with every record, tombstones included, and they are not skipped or forwarded behind the user's back (skipping would silently drop deletes in `map`, and Processors must see deletes). The types say so instead:
  - `app.stream<K, V>(topic, serdes)` returns `Stream<K, V | null>`; `Table<K, V>.toStream()` returns `Stream<K, V | null>` (also for `count`, which never deletes: safe over-approximation, avoids a second Table type parameter).
  - `filter` with a type guard drops tombstones and narrows: `.filter((k, v): v is Order => v !== null)` gives `Stream<K, Order>`. `map` output types are whatever the mapper returns (so `null` stays only if the mapper returns it).
  - `groupByKey()` returns `GroupedStream<K, NonNullable<V>>` because aggregations skip tombstones, so aggregator/reducer values are never null.
  - Serde parameters (`to`, `groupByKey`) take `Serde<NonNullable<V>>`, since a Serde never sees null.
  - Consequence: a Processor connected to a topic Stream must accept `StreamRecord<K, V | null>` (or the Stream is filtered first); existing tests were updated that way. Covered by type and runtime tests in `test/tables.test.ts` ("tombstones in Streams").
  - Added **Tombstone** to the CONTEXT.md glossary.
- **Not done / for later tickets:**
  - Null **keys** are still typed `K` though they reach operators at runtime (Test Driver `pipe(null, ...)`); aggregations skip them. Same treatment (`K | null` on topic Streams) would be a bigger ripple; consider it with ticket 13 or a follow-up.
  - `KeyValueStore.put(key, null)` deletes at runtime but `put` is typed `V`; left alone to keep away from state-store internals (ticket 12).
  - No `Table` operators beyond `toStream()` (no table-from-topic, `filter`/`mapValues` on Tables, or old/new value pairs); a tombstone is forwarded even if the key had no value, as in Kafka Streams.
  - This decision could become an ADR if more operators (windows, joins) need to follow it.

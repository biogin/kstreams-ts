# The DSL

The **DSL** is the typed, chainable API on top of the [Processor API](./processor-api.md): `app.stream(...)`, then operators. Types are inferred through the chain; you never write them by hand.

```ts
import { createStreamsApp, serdes, tumbling } from 'kstreams-ts'

interface Order {
  customer: string
  amountCents: number
}

const app = createStreamsApp({ applicationId: 'orders' })
const orderSerdes = { key: serdes.string, value: serdes.json<Order>() }
```

## Streams

A **Stream** is an unbounded sequence of keyed records, each an independent event. `app.stream(topic, serdes)` declares one read from a topic. Because a record without a value is a Tombstone, the value type is `V | null`; drop tombstones with a type guard:

```ts
// Reading a topic: values are `Order | null`, because a record without a value is a Tombstone.
const orders = app
  .stream('orders', orderSerdes)
  .filter((_id, order): order is Order => order !== null)
  .peek((id, order) => console.log('order', id, order.amountCents))
```

Operators:

- `filter(predicate)` keeps records for which it returns true (a type guard narrows the type).
- `map((key, value) => ({ key, value }))` makes new keys and values, possibly of other types. The record keeps its Event Time. Its result has no Serdes of its own, so `.to()` needs both.
- `peek(action)` calls `action` for each record (logging, metrics) and passes the record on unchanged.
- `process(processor)` sends records through a [Processor](./processor-api.md).
- `to(topic, serdes?)` writes every record to a topic. Serdes you leave out default to the ones the Stream was read with.

```ts
// Stateless operators: `map` may change the key and value types.
orders
  .map((_id, order) => ({ key: order.customer, value: order.amountCents }))
  .to('amounts', { key: serdes.string, value: serdes.number })
```

Output records get the Event Time of the record that produced them. Output topics are not created for you.

## Functions must be synchronous

Functions you give to DSL operators must be synchronous ([ADR-0012](./adr/0012-dsl-functions-must-be-synchronous.md)), so that a Promise is never silently treated as a value. Three layers catch the mistake:

1. An `async` function **fails to compile**, with a message telling you to use a Processor.
2. In plain JavaScript, an `async` function is **rejected when the app is built**, before it connects to Kafka.
3. A function that returns a Promise anyway throws `AsyncNotAllowedError` on the first record and **stops the app**. This error never reaches your [error handlers](./error-handlers.md), so a bug can't quietly send every record to a dead-letter topic.

Do async work (HTTP calls, database lookups) in a [Processor](./processor-api.md).

## Aggregations

`groupByKey()` groups a Stream by its current key; to group by another key `map` first. The aggregation's State Store needs a key Serde, so pass one if the Stream has none (for example after `map`). Then:

- `count()` counts records per key.
- `reduce(reducer)` combines values per key; the first value is stored as is.
- `aggregate(initializer, aggregator, { serde })` folds values per key into a result of another type.

Each returns a **Table**: a keyed view where each record is an upsert of the latest value for its key. `table.toStream()` turns its updates back into a Stream, one record per change, carrying the Event Time of the record that caused it. Like Kafka Streams, aggregations skip records with a null key or null value. Returning `null` from `aggregate` or `reduce` deletes the key and forwards a Tombstone.

```ts
// Aggregations: group by the current key, then count, reduce or aggregate into a Table.
const perCustomer = orders
  .map((_id, order) => ({ key: order.customer, value: order }))
  .groupByKey({ key: serdes.string, value: serdes.json<Order>() })

perCustomer.count({ store: 'orders-per-customer' }).toStream().to('order-counts', {
  key: serdes.string,
  value: serdes.number,
})

perCustomer
  .aggregate(
    () => 0,
    (_customer, order, total) => total + order.amountCents,
    { store: 'revenue-per-customer', serde: serdes.number },
  )
  .toStream()
  .to('revenue', { key: serdes.string, value: serdes.number })
```

**Name your stores** with `{ store: 'name' }`. The name is part of the Changelog topic (`<applicationId>-<store>-changelog`). Without a name one is generated from the position in the app, so adding an operator before it could point the aggregation at the wrong, or an empty, Changelog.

## Windows

`groupByKey().window(tumbling(...))` aggregates per key and time window; see [windows](./windows.md).

```ts
// Windows: tumbling windows by Event Time, with a Grace Period for late records.
perCustomer
  .window(tumbling('1m', { grace: '10s' }))
  .count({ store: 'orders-per-minute' })
  .toStream()
  .to('orders-per-minute', {
    key: serdes.windowed(serdes.string, '1m'),
    value: serdes.number,
  })
```

## Not in 0.1

Reading a topic as a Table, joins, `groupBy` with a new key (Repartition Topics), and hopping and session windows are not there yet. See [known limitations](./limitations.md).

A Task is one partition number across all of an app's input topics, so the input topics must be Co-partitioned (same partition count). The app checks this at start and fails with a clear error otherwise.

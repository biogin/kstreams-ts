# Joins and re-keying today

kstreams-ts 0.1 has no join operators, no reading of a topic as a Table and no Repartition Topics ([known limitations](./limitations.md)). The two most common cases work with what's there: enriching a Stream from a Table with the [Processor API](./processor-api.md), and grouping by another key through a topic you write yourself.

## Enriching a Stream from a Table (stream-table join)

Example: each order should carry its customer's current data. Keep the latest record per customer in a State Store, and have the orders look it up. Two Processors share the store by name, and since both topics are keyed by customer ID and [Co-partitioned](./limitations.md), a customer's record and its orders land in the same Task, which holds that store.

```ts
// Both topics are keyed by customer ID and Co-partitioned, so a customer's records and orders
// are in the same Task.
const customerSerdes = { key: serdes.string, value: serdes.json<Customer>() }
const orderSerdes = { key: serdes.string, value: serdes.json<Order>() }

// The table side: the latest record per customer, in a State Store that both Processors use.
const customers = app.store('customers', customerSerdes)

const rememberCustomer = app.processor<string, Customer | null, never, never>(
  'remember-customer',
  (ctx) => {
    const store = ctx.store(customers)
    return {
      process(record) {
        // A Tombstone deletes the customer.
        if (record.value === null) store.delete(record.key)
        else store.put(record.key, record.value)
      },
    }
  },
)

// The stream side: each order looks up its customer. An order whose customer the Task hasn't
// seen yet gets `customer: null` (a left join); return early instead for an inner join.
const enrichOrder = app.processor<string, Order | null, string, EnrichedOrder>(
  'enrich-order',
  (ctx) => {
    const store = ctx.store(customers)
    return {
      process(record) {
        if (record.value === null) return
        const customer = store.get(record.key) ?? null
        ctx.forward({
          key: record.key,
          value: { ...record.value, customer },
          timestamp: record.timestamp,
        })
      },
    }
  },
)

app.stream('customers', customerSerdes).process(rememberCustomer)
app
  .stream('orders', orderSerdes)
  .process(enrichOrder)
  .to('enriched-orders', { key: serdes.string, value: serdes.json<EnrichedOrder>() })
```

The full app is [`examples/joins.ts`](../examples/joins.ts), and a unit test runs it through the [Test Driver](./test-driver.md).

What to know about this join:

- An order joins the customer data its Task has processed so far. A Task takes records from its topics in the order they are fetched, not by timestamp, so an order produced shortly after a customer update can still see the previous version, or no customer at all. Kafka Streams instead takes the buffered record with the lowest timestamp across a Task's inputs, and can wait a little for a lagging input (`max.task.idle.ms`); kstreams-ts does neither yet.
- The store is fault-tolerant like any other: it has its own Changelog and is restored after a crash or a rebalance. Kafka Streams can use the source topic as the changelog of such a table; kstreams-ts writes a separate one, so the customer data is stored twice in Kafka.
- Under exactly-once, the store update and the enriched outputs commit in the same transaction as the consumed offsets, like everything else in the app.
- The customer topic must have the same partition count as the order topic, and be keyed the same way. The app checks the partition counts at start.

For an enrichment table small enough to keep whole on every instance (a GlobalKTable in Kafka Streams), there is no recipe yet: a Task only sees its own partition of the table topic.

## Grouping by another key

Aggregations group by the record key. To aggregate by something else, write the re-keyed records to a topic keyed by the new key and read it back; [the DSL page](./dsl.md#aggregations) shows how. The topic must be Co-partitioned with the app's other inputs.

## What still needs real support

Stream-stream (windowed) joins, joins between two Tables, aggregations over Tables and automatic repartitioning are hard to get right by hand. If you need one of them, please open an issue on GitHub: that's how the next version gets prioritized.

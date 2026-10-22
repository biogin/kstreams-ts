import { createStreamsApp, serdes, tumbling } from 'kstreams-ts'

interface Order {
  customer: string
  amountCents: number
}

const app = createStreamsApp({ applicationId: 'orders' })
const orderSerdes = { key: serdes.string, value: serdes.json<Order>() }

// Reading a topic: values are `Order | null`, because a record without a value is a Tombstone.
const orders = app
  .stream('orders', orderSerdes)
  .filter((_id, order): order is Order => order !== null)
  .peek((id, order) => console.log('order', id, order.amountCents))

// Stateless operators: `map` may change the key and value types.
orders
  .map((_id, order) => ({ key: order.customer, value: order.amountCents }))
  .to('amounts', { key: serdes.string, value: serdes.number })

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

// Windows: tumbling windows by Event Time, with a Grace Period for late records.
perCustomer
  .window(tumbling('1m', { grace: '10s' }))
  .count({ store: 'orders-per-minute' })
  .toStream()
  .to('orders-per-minute', {
    key: serdes.windowed(serdes.string, '1m'),
    value: serdes.number,
  })

export default app

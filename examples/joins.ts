import { createStreamsApp, serdes } from 'kstreams-ts'

interface Customer {
  name: string
  tier: 'standard' | 'gold'
}
interface Order {
  amountCents: number
}
interface EnrichedOrder extends Order {
  customer: Customer | null
}

const app = createStreamsApp({ applicationId: 'enrich-orders' })

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

export default app

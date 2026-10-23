// The stream-table join recipe in docs/joins.md (examples/joins.ts) does what the docs say.

import { describe, expect, it } from 'vitest'
import app from '../examples/joins.js'
import { createTestDriver } from '../src/index.js'

describe('the stream-table join example', () => {
  it('enriches each order with the latest customer the Task has seen', async () => {
    const driver = createTestDriver(app)
    const customers = driver.input<string, unknown>('customers')
    const orders = driver.input<string, unknown>('orders')
    const enriched = driver.output('enriched-orders')

    // An order before its customer: a left join forwards it without one.
    await orders.pipe('c1', { amountCents: 100 }, { timestamp: 1 })
    await customers.pipe('c1', { name: 'Ada', tier: 'standard' }, { timestamp: 2 })
    await orders.pipe('c1', { amountCents: 200 }, { timestamp: 3 })
    // An update replaces the customer; a Tombstone deletes it.
    await customers.pipe('c1', { name: 'Ada', tier: 'gold' }, { timestamp: 4 })
    await orders.pipe('c1', { amountCents: 300 }, { timestamp: 5 })
    await customers.pipe('c1', null, { timestamp: 6 })
    await orders.pipe('c1', { amountCents: 400 }, { timestamp: 7 })

    expect(enriched.readAll().map(({ key, value }) => ({ key, value }))).toEqual([
      { key: 'c1', value: { amountCents: 100, customer: null } },
      { key: 'c1', value: { amountCents: 200, customer: { name: 'Ada', tier: 'standard' } } },
      { key: 'c1', value: { amountCents: 300, customer: { name: 'Ada', tier: 'gold' } } },
      { key: 'c1', value: { amountCents: 400, customer: null } },
    ])
  })
})

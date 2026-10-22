import { describe, expect, expectTypeOf, it } from 'vitest'
import {
  AsyncNotAllowedError,
  createStreamsApp,
  createTestDriver,
  type ProcessorContext,
  type Serde,
  type Stream,
  type StreamRecord,
  serdes,
  type Table,
} from '../src/index.js'

const json = <T>(): Serde<T> => ({
  serialize: (value, ctx) => serdes.string.serialize(JSON.stringify(value), ctx),
  deserialize: (bytes, ctx) => JSON.parse(serdes.string.deserialize(bytes, ctx)) as T,
})

interface Order {
  amount: number
}

const orders = { key: serdes.string, value: json<Order>() }

function orderStream(applicationId: string) {
  const app = createStreamsApp({ applicationId })
  const stream = app.stream('orders', orders)
  return { app, stream }
}

describe('groupByKey().aggregate()', () => {
  it('folds the records of each key into a Table, starting from the initializer', async () => {
    const { app, stream } = orderStream('aggregate')
    stream.groupByKey().aggregate(
      () => ({ orders: 0, total: 0 }),
      (_key, order, totals) => ({ orders: totals.orders + 1, total: totals.total + order.amount }),
      { store: 'totals', serde: json<{ orders: number; total: number }>() },
    )

    const driver = createTestDriver(app)
    const input = driver.input<string, Order>('orders')
    await input.pipe('alice', { amount: 10 })
    await input.pipe('bob', { amount: 5 })
    await input.pipe('alice', { amount: 7 })

    expect(driver.store('totals').all()).toEqual([
      ['alice', { orders: 2, total: 17 }],
      ['bob', { orders: 1, total: 5 }],
    ])
  })

  it('skips records with a null key or a null value (tombstone), like count', async () => {
    const { app, stream } = orderStream('aggregate-nulls')
    const calls: string[] = []
    stream.groupByKey().aggregate(
      () => 0,
      (key, order, total) => {
        calls.push(key)
        return total + order.amount
      },
      { store: 'totals', serde: serdes.number },
    )

    const driver = createTestDriver(app)
    const input = driver.input<string, Order>('orders')
    await input.pipe('alice', { amount: 1 })
    await input.pipe('alice', null)
    await input.pipe(null, { amount: 2 })

    expect(calls).toEqual(['alice'])
    expect(driver.store('totals').all()).toEqual([['alice', 1]])
  })

  it('names the State Store when no name is given', async () => {
    const { app, stream } = orderStream('aggregate-unnamed')
    const table = stream.groupByKey().aggregate(
      () => 0,
      (_key, order, total) => total + order.amount,
      { serde: serdes.number },
    )

    const driver = createTestDriver(app)
    await driver.input<string, Order>('orders').pipe('alice', { amount: 3 })

    expect(driver.store<string, number>(table.store).get('alice')).toBe(3)
  })

  it('types the Table by the Stream key and the aggregate Serde', () => {
    const { stream } = orderStream('aggregate-types')
    const table = stream.groupByKey().aggregate(
      () => [] as number[],
      (_key, order, amounts) => {
        expectTypeOf(order).toEqualTypeOf<Order>()
        expectTypeOf(amounts).toEqualTypeOf<number[]>()
        return [...amounts, order.amount]
      },
      { serde: json<number[]>() },
    )
    expectTypeOf(table).toEqualTypeOf<Table<string, number[]>>()
  })
})

describe('groupByKey().reduce()', () => {
  it('combines the values of each key, starting from the first one', async () => {
    const { app, stream } = orderStream('reduce')
    const seen: [Order, Order][] = []
    stream.groupByKey().reduce(
      (largest, order) => {
        seen.push([largest, order])
        return order.amount > largest.amount ? order : largest
      },
      { store: 'largest' },
    )

    const driver = createTestDriver(app)
    const input = driver.input<string, Order>('orders')
    await input.pipe('alice', { amount: 10 })
    await input.pipe('alice', { amount: 30 })
    await input.pipe('alice', { amount: 20 })
    await input.pipe('bob', { amount: 5 })

    expect(seen).toEqual([
      [{ amount: 10 }, { amount: 30 }],
      [{ amount: 30 }, { amount: 20 }],
    ])
    expect(driver.store('largest').all()).toEqual([
      ['alice', { amount: 30 }],
      ['bob', { amount: 5 }],
    ])
  })

  it('stores with the value Serde given to reduce, else the one of the grouped Stream', async () => {
    const { app, stream } = orderStream('reduce-serdes')
    const amounts = stream.mapValues((_key, order) => order?.amount ?? 0)
    expect(() => amounts.groupByKey({ key: serdes.string }).reduce((a, b) => a + b)).toThrow(
      /value Serde/,
    )
    amounts
      .groupByKey({ key: serdes.string })
      .reduce((a, b) => a + b, { store: 'given', serde: serdes.number })
    amounts
      .groupByKey({ key: serdes.string, value: serdes.number })
      .reduce((a, b) => a + b, { store: 'grouped' })

    const driver = createTestDriver(app)
    const input = driver.input<string, Order>('orders')
    await input.pipe('alice', { amount: 1 })
    await input.pipe('alice', { amount: 2 })

    expect(driver.store('given').all()).toEqual([['alice', 3]])
    expect(driver.store('grouped').all()).toEqual([['alice', 3]])
  })

  it('skips tombstones and types the Table like the grouped values', async () => {
    const { app, stream } = orderStream('reduce-nulls')
    const table = stream.groupByKey().reduce((a, b) => ({ amount: a.amount + b.amount }), {
      store: 'sum',
    })
    expectTypeOf(table).toEqualTypeOf<Table<string, Order>>()

    const driver = createTestDriver(app)
    const input = driver.input<string, Order>('orders')
    await input.pipe('alice', { amount: 1 })
    await input.pipe('alice', null)
    await input.pipe('alice', { amount: 2 })

    expect(driver.store('sum').all()).toEqual([['alice', { amount: 3 }]])
  })
})

describe('Table.toStream()', () => {
  it('turns every update of a count Table into a record, written with the Table Serdes', async () => {
    const app = createStreamsApp({ applicationId: 'count-to-stream' })
    app
      .stream('clicks', { key: serdes.string, value: serdes.string })
      .groupByKey()
      .count()
      .toStream()
      .to('clicks-per-user')

    const driver = createTestDriver(app)
    const clicks = driver.input<string, string>('clicks')
    await clicks.pipe('alice', 'home', { timestamp: 1 })
    await clicks.pipe('bob', 'home', { timestamp: 2 })
    await clicks.pipe('alice', 'cart', { timestamp: 3 })

    expect(driver.output<string, number>('clicks-per-user').readAll()).toEqual([
      { key: 'alice', value: 1, timestamp: 1 },
      { key: 'bob', value: 1, timestamp: 2 },
      { key: 'alice', value: 2, timestamp: 3 },
    ])
  })

  it('writes aggregate and reduce updates out, keeping the Event Time of each input', async () => {
    const { app, stream } = orderStream('aggregate-to-stream')
    const grouped = stream.groupByKey()
    grouped
      .aggregate(
        () => 0,
        (_key, order, total) => total + order.amount,
        { serde: serdes.number },
      )
      .toStream()
      .to('totals')
    grouped
      .reduce((a, b) => (b.amount > a.amount ? b : a))
      .toStream()
      .to('largest')

    const driver = createTestDriver(app)
    const input = driver.input<string, Order>('orders')
    await input.pipe('alice', { amount: 10 }, { timestamp: 5 })
    await input.pipe('alice', { amount: 3 }, { timestamp: 6 })

    expect(driver.output<string, number>('totals').readAll()).toEqual([
      { key: 'alice', value: 10, timestamp: 5 },
      { key: 'alice', value: 13, timestamp: 6 },
    ])
    expect(driver.output<string, Order>('largest').readAll()).toEqual([
      { key: 'alice', value: { amount: 10 }, timestamp: 5 },
      { key: 'alice', value: { amount: 10 }, timestamp: 6 },
    ])
  })

  it('can be filtered, mapped and processed like any other Stream', async () => {
    const app = createStreamsApp({ applicationId: 'table-stream-chain' })
    const recorded: StreamRecord<string, number | null>[] = []
    const record = app.processor('record', (ctx: ProcessorContext<string, number | null>) => ({
      process(update: StreamRecord<string, number | null>) {
        recorded.push(update)
        ctx.forward(update)
      },
    }))
    app
      .stream('clicks', { key: serdes.string, value: serdes.string })
      .groupByKey()
      .count()
      .toStream()
      .filter((_user, count) => count !== null && count >= 2)
      .process(record)

    const driver = createTestDriver(app)
    const clicks = driver.input<string, string>('clicks')
    await clicks.pipe('alice', 'home', { timestamp: 1 })
    await clicks.pipe('alice', 'cart', { timestamp: 2 })

    expect(recorded).toEqual([{ key: 'alice', value: 2, timestamp: 2 }])
  })
})

describe('deletes', () => {
  it('deletes the key and sends a tombstone downstream when aggregate returns null', async () => {
    const { app, stream } = orderStream('aggregate-delete')
    stream
      .groupByKey()
      .aggregate(
        () => 0,
        (_key, order, balance) => {
          const next = balance + order.amount
          return next === 0 ? null : next
        },
        { store: 'balances', serde: serdes.number },
      )
      .toStream()
      .to('balances')

    const driver = createTestDriver(app)
    const input = driver.input<string, Order>('orders')
    await input.pipe('alice', { amount: 10 }, { timestamp: 1 })
    await input.pipe('bob', { amount: 4 }, { timestamp: 2 })
    await input.pipe('alice', { amount: -10 }, { timestamp: 3 })

    expect(driver.store('balances').all()).toEqual([['bob', 4]])
    expect(driver.output<string, number>('balances').readAll()).toEqual([
      { key: 'alice', value: 10, timestamp: 1 },
      { key: 'bob', value: 4, timestamp: 2 },
      { key: 'alice', value: null, timestamp: 3 },
    ])

    // A deleted key starts again from the initializer.
    await input.pipe('alice', { amount: 1 }, { timestamp: 4 })
    expect(driver.store('balances').get('alice')).toBe(1)
  })

  it('deletes the key and sends a tombstone downstream when reduce returns null', async () => {
    const { app, stream } = orderStream('reduce-delete')
    stream
      .groupByKey()
      .reduce((a, b) => (a.amount + b.amount === 0 ? null : { amount: a.amount + b.amount }), {
        store: 'sum',
      })
      .toStream()
      .to('sum')

    const driver = createTestDriver(app)
    const input = driver.input<string, Order>('orders')
    await input.pipe('alice', { amount: 2 }, { timestamp: 1 })
    await input.pipe('alice', { amount: -2 }, { timestamp: 2 })

    expect(driver.store('sum').all()).toEqual([])
    expect(driver.output<string, Order>('sum').readAll()).toEqual([
      { key: 'alice', value: { amount: 2 }, timestamp: 1 },
      { key: 'alice', value: null, timestamp: 2 },
    ])
  })
})

describe('tombstones in Streams', () => {
  it('reach stateless operators and Processors as a null value, and are typed so', async () => {
    const { app, stream } = orderStream('tombstones')
    expectTypeOf(stream).toEqualTypeOf<Stream<string, Order | null>>()
    const seen: (Order | null)[] = []
    const amounts = stream
      .peek((_key, order) => seen.push(order))
      .map((key, order) => ({ key, value: order === null ? null : order.amount }))
    expectTypeOf(amounts).toEqualTypeOf<Stream<string, number | null>>()
    amounts.to('amounts', { key: serdes.string, value: serdes.number })

    const driver = createTestDriver(app)
    const input = driver.input<string, Order>('orders')
    await input.pipe('alice', { amount: 1 }, { timestamp: 1 })
    await input.pipe('alice', null, { timestamp: 2 })

    expect(seen).toEqual([{ amount: 1 }, null])
    expect(driver.output('amounts').readAll()).toEqual([
      { key: 'alice', value: 1, timestamp: 1 },
      { key: 'alice', value: null, timestamp: 2 },
    ])
  })

  it('are dropped by a filter type guard, which narrows the value type', async () => {
    const { app, stream } = orderStream('tombstones-filter')
    const present = stream.filter((_key, order): order is Order => order !== null)
    expectTypeOf(present).toEqualTypeOf<Stream<string, Order>>()
    present
      .map((key, order) => ({ key, value: order.amount }))
      .to('amounts', {
        key: serdes.string,
        value: serdes.number,
      })

    const driver = createTestDriver(app)
    const input = driver.input<string, Order>('orders')
    await input.pipe('alice', null, { timestamp: 1 })
    await input.pipe('alice', { amount: 2 }, { timestamp: 2 })

    expect(driver.output('amounts').readAll()).toEqual([{ key: 'alice', value: 2, timestamp: 2 }])
  })

  it('are typed into the Stream of a Table, and never into grouped values', () => {
    const { stream } = orderStream('tombstone-types')
    const grouped = stream.groupByKey()
    expectTypeOf(grouped.reduce((a) => a).toStream()).toEqualTypeOf<Stream<string, Order | null>>()
    expectTypeOf(grouped.count().toStream()).toEqualTypeOf<Stream<string, number | null>>()
    // A Processor reading a topic Stream must accept tombstones.
    const strict = createStreamsApp({ applicationId: 'strict' })
    const p = strict.processor('p', (_ctx: ProcessorContext<string, Order>) => ({
      process(_record: StreamRecord<string, Order>) {},
    }))
    // @ts-expect-error the Processor does not accept tombstones
    expect(() => strict.stream('orders', orders).process(p)).not.toThrow()
  })
})

describe('aggregate and reduce functions must be synchronous', () => {
  // biome-ignore lint/suspicious/noExplicitAny: simulates a caller without types
  type Untyped = any

  it('rejects async functions when the app is built', () => {
    const grouped: Untyped = orderStream('async-build').stream.groupByKey()
    const serde = serdes.number
    expect(() =>
      grouped.aggregate(
        async () => 0,
        () => 0,
        { serde },
      ),
    ).toThrow(/aggregate\(\)/)
    expect(() =>
      grouped.aggregate(
        () => 0,
        async () => 0,
        { serde },
      ),
    ).toThrow(AsyncNotAllowedError)
    expect(() => grouped.reduce(async () => 0)).toThrow(/reduce\(\)/)
  })

  it('stops on the first record when one returns a Promise', async () => {
    const { app, stream } = orderStream('async-record')
    const grouped: Untyped = stream.groupByKey()
    grouped.aggregate(
      () => Promise.resolve(0),
      () => 0,
      { serde: serdes.number },
    )
    grouped.reduce(() => Promise.resolve({ amount: 0 }))

    const driver = createTestDriver(app)
    const input = driver.input<string, Order>('orders')
    await expect(input.pipe('alice', { amount: 1 })).rejects.toThrow(/aggregate\(\)/)
  })

  it('stops when reduce returns a Promise', async () => {
    const { app, stream } = orderStream('async-reduce')
    ;(stream.groupByKey() as Untyped).reduce(() => Promise.resolve({ amount: 0 }))

    const input = createTestDriver(app).input<string, Order>('orders')
    await input.pipe('alice', { amount: 1 })
    await expect(input.pipe('alice', { amount: 1 })).rejects.toThrow(AsyncNotAllowedError)
  })

  it('fails to compile with an async function', () => {
    const grouped = orderStream('async-types').stream.groupByKey()
    const options = { serde: serdes.number }
    const zero = () => 0
    const keep = (_key: string, _order: Order, total: number) => total
    // @ts-expect-error DSL functions must be synchronous
    expect(() => grouped.aggregate(async () => 0, keep, options)).toThrow()
    // @ts-expect-error DSL functions must be synchronous
    expect(() => grouped.aggregate(zero, async (_k, _v, a) => a, options)).toThrow()
    // @ts-expect-error DSL functions must be synchronous
    expect(() => grouped.reduce(async (a) => a)).toThrow()
    type Reducer = Parameters<typeof grouped.reduce<Promise<Order>>>[0]
    expectTypeOf<ReturnType<Reducer>>().toHaveProperty(
      'DSL functions must be synchronous; use a Processor for async work',
    )
  })
})

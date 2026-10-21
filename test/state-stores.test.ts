import { describe, expect, expectTypeOf, it } from 'vitest'
import {
  createStreamsApp,
  createTestDriver,
  type KeyValueStore,
  type ProcessorContext,
  type Serde,
  type StateStoreContext,
  type StateStoreSupplier,
  type StreamRecord,
  serdes,
  stores,
  type Table,
} from '../src/index.js'

const strings = { key: serdes.string, value: serdes.string }

describe('groupByKey().count()', () => {
  it('counts records per key into a named State Store the Test Driver can read', async () => {
    const app = createStreamsApp({ applicationId: 'count' })
    app.stream('clicks', strings).groupByKey().count({ store: 'clicks-per-user' })

    const driver = createTestDriver(app)
    const clicks = driver.input<string, string>('clicks')
    await clicks.pipe('alice', 'home')
    await clicks.pipe('bob', 'home')
    await clicks.pipe('alice', 'cart')

    const counts = driver.store<string, number>('clicks-per-user')
    expect(counts.get('alice')).toBe(2)
    expect(counts.get('bob')).toBe(1)
    expect(counts.get('carol')).toBeUndefined()
    expect(counts.all()).toEqual([
      ['alice', 2],
      ['bob', 1],
    ])
  })

  it('skips records with a null key or a null value (tombstone), like Kafka Streams', async () => {
    const app = createStreamsApp({ applicationId: 'count-nulls' })
    app.stream('clicks', strings).groupByKey().count({ store: 'counts' })

    const driver = createTestDriver(app)
    const clicks = driver.input<string, string>('clicks')
    await clicks.pipe('alice', 'home')
    await clicks.pipe('alice', null)
    await clicks.pipe(null, 'home')

    expect(driver.store<string, number>('counts').all()).toEqual([['alice', 1]])
  })

  it('names the State Store when no name is given, and tells the Table its name', async () => {
    const app = createStreamsApp({ applicationId: 'count-unnamed' })
    const table = app.stream('clicks', strings).groupByKey().count()
    expect(table.store).toEqual(expect.any(String))

    const driver = createTestDriver(app)
    await driver.input<string, string>('clicks').pipe('alice', 'home')

    expect(driver.store<string, number>(table.store).get('alice')).toBe(1)
  })

  it('keeps the state of each Test Driver apart', async () => {
    const app = createStreamsApp({ applicationId: 'count-drivers' })
    app.stream('clicks', strings).groupByKey().count({ store: 'counts' })

    const first = createTestDriver(app)
    const second = createTestDriver(app)
    await first.input<string, string>('clicks').pipe('alice', 'home')

    expect(first.store<string, number>('counts').get('alice')).toBe(1)
    expect(second.store<string, number>('counts').get('alice')).toBeUndefined()
  })

  it('needs a key Serde when the Stream has none, e.g. after map', async () => {
    const app = createStreamsApp({ applicationId: 'count-serdes' })
    const byPage = app.stream('clicks', strings).map((user, page) => ({ key: page, value: user }))
    expect(() => byPage.groupByKey()).toThrow(/key Serde/)

    byPage.groupByKey({ key: serdes.string }).count({ store: 'per-page' })
    const driver = createTestDriver(app)
    await driver.input<string, string>('clicks').pipe('alice', 'home')

    expect(driver.store<string, number>('per-page').all()).toEqual([['home', 1]])
  })

  it('uses the State Store supplier it is given', async () => {
    const opened: StateStoreContext[] = []
    const recording: StateStoreSupplier = (context) => {
      opened.push(context)
      return stores.inMemory()(context)
    }
    const app = createStreamsApp({ applicationId: 'count-supplier' })
    app.stream('clicks', strings).groupByKey().count({ store: 'counts', storeSupplier: recording })

    const driver = createTestDriver(app)
    await driver.input<string, string>('clicks').pipe('alice', 'home')

    expect(opened).toEqual([{ name: 'counts' }])
    expect(driver.store<string, number>('counts').get('alice')).toBe(1)
  })

  it('types the count Table by the Stream key, with number values', () => {
    const app = createStreamsApp({ applicationId: 'count-types' })
    const table = app.stream('clicks', strings).groupByKey().count()
    expectTypeOf(table).toEqualTypeOf<Table<string, number>>()
  })
})

describe('State Stores in Processors', () => {
  it('lets a Processor read and write a State Store it gets with ctx.store', async () => {
    const app = createStreamsApp({ applicationId: 'dedupe' })
    const seen = app.store('seen', { key: serdes.string, value: serdes.number })
    const dedupe = app.processor('dedupe', (ctx: ProcessorContext<string, string>) => {
      const store = ctx.store(seen)
      return {
        process(record: StreamRecord<string, string>) {
          if (store.get(record.value) !== undefined) return
          store.put(record.value, record.timestamp)
          ctx.forward(record)
        },
      }
    })
    app.stream('input', strings).process(dedupe).to('output', strings)

    const driver = createTestDriver(app)
    const input = driver.input<string, string>('input')
    await input.pipe('k', 'a', { timestamp: 1 })
    await input.pipe('k', 'a', { timestamp: 2 })
    await input.pipe('k', 'b', { timestamp: 3 })

    expect(driver.output<string, string>('output').readAll()).toEqual([
      { key: 'k', value: 'a', timestamp: 1 },
      { key: 'k', value: 'b', timestamp: 3 },
    ])
    expect(driver.store(seen).all()).toEqual([
      ['a', 1],
      ['b', 3],
    ])
  })

  it('finds a State Store by name, and deletes keys with delete or a null value', async () => {
    const app = createStreamsApp({ applicationId: 'latest' })
    app.store('latest', strings)
    const latest = app.processor('latest', (ctx: ProcessorContext<string, string>) => ({
      process(record: StreamRecord<string, string | null>) {
        const store = ctx.store<string, string | null>('latest')
        if (record.value === 'delete') store.delete(record.key)
        else store.put(record.key, record.value)
      },
    }))
    app.stream('input', strings).process(latest)

    const driver = createTestDriver(app)
    const input = driver.input<string, string>('input')
    for (const key of ['a', 'b', 'c']) await input.pipe(key, `${key}1`)
    await input.pipe('a', 'a2')
    await input.pipe('b', 'delete')
    await input.pipe('c', null)

    expect(driver.store<string, string>('latest').all()).toEqual([['a', 'a2']])
  })

  it('gives a Processor typed access through the named State Store', () => {
    const app = createStreamsApp({ applicationId: 'store-types' })
    const counts = app.store('counts', { key: serdes.string, value: serdes.number })
    app.processor('typed', (ctx: ProcessorContext<string, string>) => {
      expectTypeOf(ctx.store(counts)).toEqualTypeOf<KeyValueStore<string, number>>()
      return { process() {} }
    })
    expectTypeOf(createTestDriver(app).store(counts).get).returns.toEqualTypeOf<
      number | undefined
    >()
  })

  it('uses the given State Store supplier, else the in-memory store', async () => {
    const opened: string[] = []
    const app = createStreamsApp({ applicationId: 'store-supplier' })
    app.store('custom', strings, {
      supplier: (context) => {
        opened.push(context.name)
        return stores.inMemory()(context)
      },
    })
    app.store('default', strings)
    createTestDriver(app)
    expect(opened).toEqual(['custom'])
  })

  it('fails the record when a Processor asks for an unknown State Store', async () => {
    const app = createStreamsApp({ applicationId: 'unknown-store' })
    const p = app.processor('p', (ctx: ProcessorContext<string, string>) => ({
      process() {
        ctx.store('nope')
      },
    }))
    app.stream('input', strings).process(p)

    const driver = createTestDriver(app)
    await expect(driver.input('input').pipe('k', 'v')).rejects.toThrow(/"nope"/)
  })

  it('reads store values with the store Serdes', async () => {
    const upper: Serde<string> = {
      serialize: (value, ctx) => serdes.string.serialize(value.toUpperCase(), ctx),
      deserialize: (bytes, ctx) => serdes.string.deserialize(bytes, ctx),
    }
    const app = createStreamsApp({ applicationId: 'store-serdes' })
    const names = app.store('names', { key: serdes.string, value: upper })
    const p = app.processor('p', (ctx: ProcessorContext<string, string>) => ({
      process(record: StreamRecord<string, string>) {
        const store = ctx.store(names)
        store.put(record.key, record.value)
        ctx.forward({ ...record, value: store.get(record.key) ?? '' })
      },
    }))
    app.stream('input', strings).process(p).to('output', strings)

    const driver = createTestDriver(app)
    await driver.input<string, string>('input').pipe('k', 'ada')

    expect(driver.output('output').read().value).toBe('ADA')
    expect(driver.store(names).get('k')).toBe('ADA')
  })

  it('tells the store Serdes the Changelog topic', async () => {
    const topics: string[] = []
    const spy: Serde<string> = {
      serialize: (value, ctx) => {
        topics.push(ctx.topic)
        return serdes.string.serialize(value, ctx)
      },
      deserialize: (bytes, ctx) => serdes.string.deserialize(bytes, ctx),
    }
    const app = createStreamsApp({ applicationId: 'orders-app' })
    app.store('totals', { key: spy, value: serdes.string })
    const p = app.processor('p', (ctx: ProcessorContext<string, string>) => ({
      process(record: StreamRecord<string, string>) {
        ctx.store<string, string>('totals').put(record.key, record.value)
      },
    }))
    app.stream('input', strings).process(p)

    await createTestDriver(app).input<string, string>('input').pipe('k', 'v')

    expect(topics).toEqual(['orders-app-totals-changelog'])
  })
})

describe('State Store definition errors', () => {
  it('rejects two State Stores with the same name', () => {
    const app = createStreamsApp({ applicationId: 'dup-store' })
    app.store('counts', strings)
    expect(() => app.store('counts', strings)).toThrow(/"counts"/)
    expect(() => app.stream('input', strings).groupByKey().count({ store: 'counts' })).toThrow(
      /"counts"/,
    )
  })

  it('rejects a State Store name that cannot be part of a topic name', () => {
    const app = createStreamsApp({ applicationId: 'bad-store' })
    expect(() => app.store('', strings)).toThrow(/State Store name/)
    expect(() => app.store('a/b', strings)).toThrow(/State Store name/)
  })

  it('throws when the Test Driver is asked for an unknown State Store', () => {
    const app = createStreamsApp({ applicationId: 'no-store' })
    app.stream('input', strings).to('output')
    expect(() => createTestDriver(app).store('nope')).toThrow(/"nope"/)
  })
})

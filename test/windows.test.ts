import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, describe, expect, expectTypeOf, it } from 'vitest'
import {
  createStreamsApp,
  createTestDriver,
  type Duration,
  type Serde,
  serdes,
  type Table,
  tumbling,
  type Windowed,
} from '../src/index.js'

const strings = { key: serdes.string, value: serdes.string }

const json = <T>(): Serde<T> => ({
  serialize: (value, ctx) => serdes.string.serialize(JSON.stringify(value), ctx),
  deserialize: (bytes, ctx) => JSON.parse(serdes.string.deserialize(bytes, ctx)) as T,
})

const MINUTE = 60_000

/** The window of `key` that starts at `start`, for tumbling windows of one minute. */
const inMinute = <K>(key: K, start: number): Windowed<K> => ({
  key,
  window: { start, end: start + MINUTE },
})

/** A Streams App counting clicks per user per minute, writing the counts to `counts`. */
function clickCounts(applicationId: string, grace?: Duration) {
  const app = createStreamsApp({ applicationId })
  app
    .stream('clicks', strings)
    .groupByKey()
    .window(tumbling('1m', grace === undefined ? undefined : { grace }))
    .count({ store: 'counts' })
    .toStream()
    .to('counts')
  return app
}

describe('tumbling windows', () => {
  it('counts the records of each key per window, in a store ordered by window start', async () => {
    const driver = createTestDriver(clickCounts('window-count'))
    const clicks = driver.input<string, string>('clicks')
    await clicks.pipe('alice', 'home', { timestamp: 1_000 })
    await clicks.pipe('bob', 'home', { timestamp: 2_000 })
    await clicks.pipe('alice', 'cart', { timestamp: 59_999 })
    await clicks.pipe('alice', 'pay', { timestamp: MINUTE })

    const counts = driver.store<Windowed<string>, number>('counts')
    expect(counts.all()).toEqual([
      [inMinute('alice', 0), 2],
      [inMinute('bob', 0), 1],
      [inMinute('alice', MINUTE), 1],
    ])
    expect(counts.get(inMinute('alice', 0))).toBe(2)
    expect(counts.range(inMinute('', MINUTE), inMinute('\u{10FFFF}', MINUTE))).toEqual([
      [inMinute('alice', MINUTE), 1],
    ])
  })

  it('forwards each update with the window end as its timestamp and a windowed key', async () => {
    const driver = createTestDriver(clickCounts('window-output'))
    const clicks = driver.input<string, string>('clicks')
    await clicks.pipe('alice', 'home', { timestamp: 1_000 })
    await clicks.pipe('alice', 'cart', { timestamp: 30_000 })
    await clicks.pipe('alice', 'pay', { timestamp: 61_000 })

    expect(driver.output<Windowed<string>, number>('counts').readAll()).toEqual([
      { key: inMinute('alice', 0), value: 1, timestamp: MINUTE },
      { key: inMinute('alice', 0), value: 2, timestamp: MINUTE },
      { key: inMinute('alice', MINUTE), value: 1, timestamp: 2 * MINUTE },
    ])
  })

  it('writes windowed keys as the key Serde bytes followed by the 8-byte window start', async () => {
    const driver = createTestDriver(clickCounts('window-key-bytes'))
    await driver.input<string, string>('clicks').pipe('ab', 'home', { timestamp: MINUTE + 5 })

    const [record] = driver.output('counts', { key: serdes.bytes }).readAll()
    const expected = new Uint8Array(10)
    expected.set([0x61, 0x62])
    new DataView(expected.buffer).setBigInt64(2, BigInt(MINUTE))
    expect(record?.key).toEqual(expected)
  })

  it('includes records that arrive late but within the Grace Period', async () => {
    const driver = createTestDriver(clickCounts('window-grace', '10s'))
    const clicks = driver.input<string, string>('clicks')
    await clicks.pipe('alice', 'home', { timestamp: 50_000 })
    await clicks.pipe('alice', 'cart', { timestamp: 69_999 }) // Stream Time 69_999
    await clicks.pipe('alice', 'late', { timestamp: 55_000 }) // window [0, 60s) still open

    expect(driver.store('counts').all()).toEqual([
      [inMinute('alice', 0), 2],
      [inMinute('alice', MINUTE), 1],
    ])
    expect(driver.metrics().lateRecordsDropped).toBe(0)
  })

  it('drops and counts records later than the Grace Period', async () => {
    const driver = createTestDriver(clickCounts('window-late', '10s'))
    const clicks = driver.input<string, string>('clicks')
    await clicks.pipe('alice', 'home', { timestamp: 50_000 })
    await clicks.pipe('alice', 'cart', { timestamp: 70_000 }) // Stream Time 70s: [0, 60s) closes
    await clicks.pipe('alice', 'late', { timestamp: 59_999 })
    await clicks.pipe('bob', 'late', { timestamp: 1_000 })
    // An out-of-order record doesn't move Stream Time back, so the window stays closed.
    await clicks.pipe('alice', 'late again', { timestamp: 30_000 })

    expect(driver.store('counts').all()).toEqual([
      [inMinute('alice', 0), 1],
      [inMinute('alice', MINUTE), 1],
    ])
    expect(driver.output('counts').readAll()).toHaveLength(2)
    expect(driver.metrics().lateRecordsDropped).toBe(3)
  })

  it('has no Grace Period by default: a window closes once Stream Time reaches its end', async () => {
    const driver = createTestDriver(clickCounts('window-no-grace'))
    const clicks = driver.input<string, string>('clicks')
    await clicks.pipe('alice', 'home', { timestamp: 59_000 })
    await clicks.pipe('alice', 'next', { timestamp: MINUTE })
    await clicks.pipe('alice', 'late', { timestamp: 59_999 })

    expect(driver.store('counts').all()).toEqual([
      [inMinute('alice', 0), 1],
      [inMinute('alice', MINUTE), 1],
    ])
    expect(driver.metrics().lateRecordsDropped).toBe(1)
  })

  it('skips records with a null key or a null value, without counting them as late', async () => {
    const driver = createTestDriver(clickCounts('window-nulls'))
    const clicks = driver.input<string, string>('clicks')
    await clicks.pipe('alice', 'home', { timestamp: MINUTE })
    await clicks.pipe(null, 'home', { timestamp: 0 })
    await clicks.pipe('alice', null, { timestamp: 0 })

    expect(driver.store('counts').all()).toEqual([[inMinute('alice', MINUTE), 1]])
    expect(driver.metrics().lateRecordsDropped).toBe(0)
  })

  it('aggregates and reduces per window', async () => {
    const app = createStreamsApp({ applicationId: 'window-aggregate' })
    const grouped = app
      .stream('amounts', { key: serdes.string, value: serdes.number })
      .groupByKey()
      .window(tumbling('1h'))
    const totals = grouped.aggregate(
      () => ({ n: 0, sum: 0 }),
      (_k, v, a) => ({ n: a.n + 1, sum: a.sum + v }),
      {
        store: 'totals',
        serde: json<{ n: number; sum: number }>(),
      },
    )
    const maxima = grouped.reduce((a, b) => Math.max(a, b), { store: 'maxima' })
    expectTypeOf(totals).toEqualTypeOf<Table<Windowed<string>, { n: number; sum: number }>>()
    expectTypeOf(maxima).toEqualTypeOf<Table<Windowed<string>, number>>()

    const driver = createTestDriver(app)
    const amounts = driver.input<string, number>('amounts')
    await amounts.pipe('alice', 3, { timestamp: 0 })
    await amounts.pipe('alice', 5, { timestamp: 10 * MINUTE })
    await amounts.pipe('alice', 1, { timestamp: 60 * MINUTE })

    const hour = 60 * MINUTE
    expect(driver.store('totals').all()).toEqual([
      [
        { key: 'alice', window: { start: 0, end: hour } },
        { n: 2, sum: 8 },
      ],
      [
        { key: 'alice', window: { start: hour, end: 2 * hour } },
        { n: 1, sum: 1 },
      ],
    ])
    expect(driver.store('maxima').all()).toEqual([
      [{ key: 'alice', window: { start: 0, end: hour } }, 5],
      [{ key: 'alice', window: { start: hour, end: 2 * hour } }, 1],
    ])
  })

  it('types the windowed Table by the grouped key', () => {
    const app = createStreamsApp({ applicationId: 'window-types' })
    const table = app.stream('clicks', strings).groupByKey().window(tumbling(MINUTE)).count()
    expectTypeOf(table).toEqualTypeOf<Table<Windowed<string>, number>>()
  })
})

describe('window retention', () => {
  it('drops a window from its store once Stream Time passes its end plus the Grace Period', async () => {
    const driver = createTestDriver(clickCounts('window-retention', '10s'))
    const clicks = driver.input<string, string>('clicks')
    const counts = driver.store<Windowed<string>, number>('counts')
    await clicks.pipe('alice', 'home', { timestamp: 1_000 })
    await clicks.pipe('bob', 'home', { timestamp: 2_000 })
    await clicks.pipe('alice', 'pay', { timestamp: 70_000 }) // [0, 60s) closed, still kept

    expect(counts.all()).toEqual([
      [inMinute('alice', 0), 1],
      [inMinute('bob', 0), 1],
      [inMinute('alice', MINUTE), 1],
    ])

    await clicks.pipe('bob', 'pay', { timestamp: 70_001 }) // Stream Time passes 60s + 10s
    expect(counts.all()).toEqual([
      [inMinute('alice', MINUTE), 1],
      [inMinute('bob', MINUTE), 1],
    ])
    expect(counts.get(inMinute('alice', 0))).toBeUndefined()
  })

  it('drops every expired window at once, however far Stream Time jumps', async () => {
    const driver = createTestDriver(clickCounts('window-retention-jump'))
    const clicks = driver.input<string, string>('clicks')
    for (let minute = 0; minute < 5; minute++) {
      await clicks.pipe('alice', 'home', { timestamp: minute * MINUTE })
    }
    await clicks.pipe('bob', 'home', { timestamp: 4 * MINUTE + 1 })

    // Stream Time 4m + 1ms passes the end of [3m, 4m): only the last minute is kept.
    expect(driver.store('counts').all()).toEqual([
      [inMinute('alice', 4 * MINUTE), 1],
      [inMinute('bob', 4 * MINUTE), 1],
    ])

    await clicks.pipe('bob', 'away', { timestamp: 60 * MINUTE + 1 })
    expect(driver.store('counts').all()).toEqual([[inMinute('bob', 60 * MINUTE), 1]])
  })

  it('does not expire windows for a skipped record, whose Stream Time is rolled back', async () => {
    const app = createStreamsApp({
      applicationId: 'window-retention-skipped',
      errorHandlers: { processing: () => 'continue' },
    })
    app
      .stream('input', strings)
      .groupByKey()
      .window(tumbling(1_000))
      .aggregate(
        () => '',
        (_key, value: string, aggregate: string) => {
          if (value === 'bad') throw new Error('bad value')
          return aggregate + value
        },
        { serde: serdes.string, store: 'aggregates' },
      )
      .toStream()
      .to('output', { key: serdes.windowed(serdes.string, tumbling(1_000)), value: serdes.string })
    const driver = createTestDriver(app)
    const input = driver.input<string, string>('input')
    await input.pipe('a', 'x', { timestamp: 100 })
    await input.pipe('a', 'y', { timestamp: 200 })
    await input.pipe('b', 'z', { timestamp: 300 })
    await input.pipe('b', 'bad', { timestamp: 90_000 }) // skipped
    await input.pipe('a', 'w', { timestamp: 400 })

    const window = { start: 0, end: 1_000 }
    expect(driver.store('aggregates').all()).toEqual([
      [{ key: 'a', window }, 'xyw'],
      [{ key: 'b', window }, 'z'],
    ])
    expect(
      driver
        .output('output')
        .readAll()
        .map((record) => record.value),
    ).toEqual(['x', 'xy', 'z', 'xyw'])
  })

  it('does not forward anything when windows expire', async () => {
    const driver = createTestDriver(clickCounts('window-retention-quiet'))
    const clicks = driver.input<string, string>('clicks')
    await clicks.pipe('alice', 'home', { timestamp: 0 })
    await clicks.pipe('bob', 'home', { timestamp: 10 * MINUTE })

    expect(driver.output('counts').readAll()).toHaveLength(2)
  })

  it('drops expired windows from SQLite stores', async () => {
    const stateDir = mkdtempSync(join(tmpdir(), 'kstreams-windows-'))
    try {
      const driver = createTestDriver(clickCounts('window-retention-sqlite'), { stateDir })
      const clicks = driver.input<string, string>('clicks')
      await clicks.pipe('alice', 'home', { timestamp: 0 })
      await clicks.pipe('bob', 'home', { timestamp: 2 * MINUTE })

      expect(driver.store('counts').all()).toEqual([[inMinute('bob', 2 * MINUTE), 1]])
    } finally {
      rmSync(stateDir, { recursive: true, force: true })
    }
  })
})

describe('serdes.windowed', () => {
  it('reads and writes windowed keys as the key Serde bytes and the 8-byte window start', async () => {
    const driver = createTestDriver(clickCounts('window-serde'))
    await driver.input<string, string>('clicks').pipe('ab', 'home', { timestamp: MINUTE + 5 })

    const key = serdes.windowed(serdes.string, '1m')
    const context = { topic: 'counts' }
    const [record] = driver.output('counts', { key: serdes.bytes }).readAll()
    expect(key.deserialize(record?.key as Uint8Array, context)).toEqual(inMinute('ab', MINUTE))
    expect(key.serialize(inMinute('ab', MINUTE), context)).toEqual(record?.key)
  })

  it('takes the windows of the aggregation instead of a size', () => {
    const windows = tumbling('1h', { grace: '1m' })
    const key = serdes.windowed(serdes.number, windows)
    const context = { topic: 't' }
    const hour = 60 * MINUTE
    const windowed = { key: 7, window: { start: hour, end: 2 * hour } }
    expect(key.deserialize(key.serialize(windowed, context), context)).toEqual(windowed)
    expectTypeOf(key).toEqualTypeOf<Serde<Windowed<number>>>()
  })

  it('rejects window sizes that are not positive and bytes too short for a window start', () => {
    expect(() => serdes.windowed(serdes.string, 0)).toThrow(/window size/i)
    const key = serdes.windowed(serdes.string, '1m')
    expect(() => key.deserialize(new Uint8Array(7), { topic: 't' })).toThrow(/8 bytes/)
  })
})

describe('tumbling()', () => {
  it('takes sizes and Grace Periods in milliseconds or as durations like "1m"', () => {
    expect(tumbling('1m', { grace: '10s' })).toEqual({ sizeMs: MINUTE, graceMs: 10_000 })
    expect(tumbling(500)).toEqual({ sizeMs: 500, graceMs: 0 })
    expect(tumbling('250ms', { grace: 0 })).toEqual({ sizeMs: 250, graceMs: 0 })
    expect(tumbling('2h')).toEqual({ sizeMs: 2 * 60 * MINUTE, graceMs: 0 })
    expect(tumbling('1d')).toEqual({ sizeMs: 24 * 60 * MINUTE, graceMs: 0 })
  })

  it('rejects sizes that are not positive and Grace Periods that are negative', () => {
    expect(() => tumbling(0)).toThrow(/window size/i)
    expect(() => tumbling(-1)).toThrow(/window size/i)
    expect(() => tumbling(1.5)).toThrow(/window size/i)
    expect(() => tumbling('1m', { grace: -1 })).toThrow(/grace period/i)
    // @ts-expect-error not a duration
    expect(() => tumbling('one minute')).toThrow(/duration/i)
  })
})

describe('tumbling windows in SQLite', () => {
  const tempDirs: string[] = []
  afterEach(() => {
    for (const dir of tempDirs.splice(0)) rmSync(dir, { recursive: true, force: true })
  })

  it('keeps windowed counts, aggregates and reductions in SQLite stores', async () => {
    const stateDir = mkdtempSync(join(tmpdir(), 'kstreams-windows-'))
    tempDirs.push(stateDir)
    const app = createStreamsApp({ applicationId: 'window-sqlite' })
    const grouped = app.stream('clicks', strings).groupByKey().window(tumbling('1m'))
    grouped.count({ store: 'counts' })
    grouped.aggregate(
      () => '',
      (_k, v, a) => a + v,
      { store: 'joined', serde: serdes.string },
    )
    grouped.reduce((a, b) => (a > b ? a : b), { store: 'last' })

    const driver = createTestDriver(app, { stateDir })
    const clicks = driver.input<string, string>('clicks')
    await clicks.pipe('bob', 'a', { timestamp: 0 })
    await clicks.pipe('alice', 'b', { timestamp: 1 })
    await clicks.pipe('alice', 'c', { timestamp: MINUTE })
    await clicks.pipe('alice', 'd', { timestamp: 2 })

    expect(driver.store('counts').all()).toEqual([
      [inMinute('alice', 0), 1],
      [inMinute('bob', 0), 1],
      [inMinute('alice', MINUTE), 1],
    ])
    expect(driver.store('joined').all()).toEqual([
      [inMinute('alice', 0), 'b'],
      [inMinute('bob', 0), 'a'],
      [inMinute('alice', MINUTE), 'c'],
    ])
    expect(driver.store('last').get(inMinute('alice', MINUTE))).toBe('c')
    expect(driver.metrics().lateRecordsDropped).toBe(3)
  })
})

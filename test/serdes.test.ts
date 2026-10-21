import { describe, expect, it } from 'vitest'
import {
  createStreamsApp,
  createTestDriver,
  DeserializationError,
  type Serde,
  type SerdeContext,
  type Serdes,
  serdes,
} from '../src/index.js'

function passThrough<K, V>(topicSerdes: Serdes<K, V>) {
  const app = createStreamsApp({ applicationId: 'serdes' })
  app.stream('input', topicSerdes).to('output')
  return createTestDriver(app)
}

interface Order {
  id: string
  items: { sku: string; quantity: number }[]
  paid: boolean
  note: string | null
}

describe('built-in Serdes', () => {
  it('round-trips JSON values through the Test Driver', () => {
    const driver = passThrough({ key: serdes.string, value: serdes.json<Order>() })
    const order: Order = {
      id: 'o-1',
      items: [{ sku: 'café', quantity: 2 }],
      paid: false,
      note: null,
    }
    driver.input<string, Order>('input').pipe('o-1', order, { timestamp: 3 })

    expect(driver.output<string, Order>('output').read()).toEqual({
      key: 'o-1',
      value: order,
      timestamp: 3,
    })
  })

  it('writes JSON as UTF-8 text', () => {
    const bytes = serdes.json<{ a: number }>().serialize({ a: 1 }, { topic: 't' })
    expect(new TextDecoder().decode(bytes)).toBe('{"a":1}')
  })

  it('refuses to serialize a value JSON cannot represent', () => {
    expect(() => serdes.json<unknown>().serialize(undefined, { topic: 't' })).toThrow(TypeError)
  })

  it.each([0, -1.5, 42, Number.MAX_SAFE_INTEGER, Number.POSITIVE_INFINITY])(
    'round-trips the number %d',
    (n) => {
      const driver = passThrough({ key: serdes.number, value: serdes.number })
      driver.input<number, number>('input').pipe(n, n, { timestamp: 1 })

      expect(driver.output<number, number>('output').read()).toEqual({
        key: n,
        value: n,
        timestamp: 1,
      })
    },
  )

  it('writes numbers as 8-byte big-endian doubles', () => {
    expect([...serdes.number.serialize(1, { topic: 't' })]).toEqual([0x3f, 0xf0, 0, 0, 0, 0, 0, 0])
  })

  it('round-trips raw bytes unchanged', () => {
    const driver = passThrough({ key: serdes.bytes, value: serdes.bytes })
    const key = new Uint8Array([0, 255, 7])
    const value = new Uint8Array([0xde, 0xad, 0xbe, 0xef])
    driver.input<Uint8Array, Uint8Array>('input').pipe(key, value, { timestamp: 1 })

    expect(driver.output<Uint8Array, Uint8Array>('output').read()).toEqual({
      key,
      value,
      timestamp: 1,
    })
  })

  it('lets a test read an output topic with its own Serdes', () => {
    const driver = passThrough({ key: serdes.string, value: serdes.json<{ n: number }>() })
    driver.input<string, { n: number }>('input').pipe('k', { n: 1 }, { timestamp: 1 })

    const record = driver.output<string, Uint8Array>('output', { value: serdes.bytes }).read()
    expect(record.value).toEqual(new TextEncoder().encode('{"n":1}'))
  })
})

describe('custom Serdes', () => {
  it('plugs a user-defined Serde in through the typed interface', () => {
    interface Point {
      x: number
      y: number
    }
    const point: Serde<Point> = {
      serialize: ({ x, y }) => new TextEncoder().encode(`${x},${y}`),
      deserialize: (bytes) => {
        const [x, y] = new TextDecoder().decode(bytes).split(',').map(Number)
        return { x: x ?? Number.NaN, y: y ?? Number.NaN }
      },
    }
    const driver = passThrough({ key: serdes.string, value: point })
    driver.input<string, Point>('input').pipe('p', { x: 1, y: -2 }, { timestamp: 1 })

    expect(driver.output<string, Point>('output').read().value).toEqual({ x: 1, y: -2 })
  })

  it('tells the Serde which topic the bytes belong to', () => {
    const seen: string[] = []
    const recording: Serde<string> = {
      serialize(value, context: SerdeContext) {
        seen.push(`serialize ${context.topic}`)
        return serdes.string.serialize(value, context)
      },
      deserialize(bytes, context: SerdeContext) {
        seen.push(`deserialize ${context.topic}`)
        return serdes.string.deserialize(bytes, context)
      },
    }
    const app = createStreamsApp({ applicationId: 'context' })
    app.stream('input', { key: serdes.string, value: recording }).to('output')
    const driver = createTestDriver(app)
    driver.input<string, string>('input').pipe('k', 'v')
    driver.output('output').read()

    expect(seen).toEqual([
      'serialize input',
      'deserialize input',
      'serialize output',
      'deserialize output',
    ])
  })
})

describe('tombstones', () => {
  const failing: Serde<never> = {
    serialize: () => {
      throw new Error('a Serde must never see a null')
    },
    deserialize: () => {
      throw new Error('a Serde must never see a null')
    },
  }

  it('passes a null value through as a tombstone', () => {
    const driver = passThrough({ key: serdes.string, value: serdes.json<Order>() })
    driver.input<string, Order>('input').pipe('o-1', null, { timestamp: 9 })

    expect(driver.output<string, Order>('output').read()).toEqual({
      key: 'o-1',
      value: null,
      timestamp: 9,
    })
  })

  it('never hands a null key or value to a Serde', () => {
    const driver = passThrough({ key: failing, value: failing })
    driver.input('input').pipe(null, null, { timestamp: 1 })

    expect(driver.output('output').read()).toEqual({ key: null, value: null, timestamp: 1 })
  })

  it('keeps tombstones in order with ordinary records', () => {
    const driver = passThrough({ key: serdes.string, value: serdes.number })
    const input = driver.input<string, number>('input')
    input.pipe('a', 1, { timestamp: 1 })
    input.pipe('a', null, { timestamp: 2 })
    input.pipe('a', 3, { timestamp: 3 })

    expect(driver.output<string, number>('output').readAll()).toEqual([
      { key: 'a', value: 1, timestamp: 1 },
      { key: 'a', value: null, timestamp: 2 },
      { key: 'a', value: 3, timestamp: 3 },
    ])
  })
})

describe('deserialization errors', () => {
  function jsonApp() {
    const app = createStreamsApp({ applicationId: 'orders' })
    app.stream('orders', { key: serdes.string, value: serdes.json<Order>() }).to('output')
    return createTestDriver(app)
  }

  it('raises a typed DeserializationError for malformed JSON', () => {
    const driver = jsonApp()
    // Write raw text with the test's own Serde, bypassing the app's JSON Serde.
    const raw = driver.input<string, string>('orders', { value: serdes.string })

    let error: unknown
    try {
      raw.pipe('o-1', '{"id": "o-1",', { timestamp: 5 })
    } catch (e) {
      error = e
    }
    expect(error).toBeInstanceOf(DeserializationError)
    expect(error).toMatchObject({
      name: 'DeserializationError',
      topic: 'orders',
      part: 'value',
      timestamp: 5,
      cause: expect.any(SyntaxError),
    })
    expect((error as Error).message).toMatch(/"orders"/)
    expect(driver.output('output').readAll()).toEqual([])
  })

  it('reports the key as the failing part when the key is malformed', () => {
    const app = createStreamsApp({ applicationId: 'keys' })
    app.stream('input', { key: serdes.number, value: serdes.string }).to('output')
    const driver = createTestDriver(app)

    expect(() =>
      driver.input<string, string>('input', { key: serdes.string }).pipe('nope', 'v'),
    ).toThrow(expect.objectContaining({ name: 'DeserializationError', part: 'key' }))
  })

  it('stops the Streams App: later records are refused', () => {
    const driver = jsonApp()
    expect(() =>
      driver.input<string, string>('orders', { value: serdes.string }).pipe('k', 'not json'),
    ).toThrow(DeserializationError)

    const valid: Order = { id: 'o-2', items: [], paid: true, note: null }
    expect(() => driver.input<string, Order>('orders').pipe('o-2', valid)).toThrow(
      expect.objectContaining({
        message: expect.stringMatching(/stopped/i),
        cause: expect.any(DeserializationError),
      }),
    )
    expect(driver.output('output').readAll()).toEqual([])
  })
})

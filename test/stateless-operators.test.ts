import { describe, expect, expectTypeOf, it } from 'vitest'
import {
  createStreamsApp,
  createTestDriver,
  type Serde,
  type Stream,
  serdes,
} from '../src/index.js'

const json = <T>(): Serde<T> => ({
  serialize: (value, ctx) => serdes.string.serialize(JSON.stringify(value), ctx),
  deserialize: (bytes, ctx) => JSON.parse(serdes.string.deserialize(bytes, ctx)) as T,
})

function stringStream(applicationId = 'stateless') {
  const app = createStreamsApp({ applicationId })
  const stream = app.stream('input', { key: serdes.string, value: serdes.string })
  return { app, stream }
}

describe('filter', () => {
  it('keeps only the records the predicate accepts', () => {
    const { app, stream } = stringStream()
    stream.filter((key, value) => key !== 'skip' && value !== null && value.length > 1).to('output')

    const driver = createTestDriver(app)
    const input = driver.input<string, string>('input')
    input.pipe('a', 'yes', { timestamp: 1 })
    input.pipe('skip', 'long enough', { timestamp: 2 })
    input.pipe('b', 'n', { timestamp: 3 })
    input.pipe('c', 'ok', { timestamp: 4 })

    expect(driver.output('output').readAll()).toEqual([
      { key: 'a', value: 'yes', timestamp: 1 },
      { key: 'c', value: 'ok', timestamp: 4 },
    ])
  })

  it('keeps the Serdes of the Stream it filters', () => {
    const { app, stream } = stringStream()
    stream.filter(() => true).to('output')

    const driver = createTestDriver(app)
    driver.input<string, string>('input').pipe('k', 'v', { timestamp: 1 })

    expect(driver.output('output').read()).toEqual({ key: 'k', value: 'v', timestamp: 1 })
  })
})

describe('map', () => {
  it('turns each record into a new key and value, keeping its timestamp', () => {
    const { app, stream } = stringStream()
    stream
      .map((key, value) => ({ key: key.toUpperCase(), value: `${key}=${value}` }))
      .to('output', { key: serdes.string, value: serdes.string })

    const driver = createTestDriver(app)
    driver.input<string, string>('input').pipe('k', 'v', { timestamp: 9 })

    expect(driver.output('output').read()).toEqual({ key: 'K', value: 'k=v', timestamp: 9 })
  })

  it('changes key and value types, written with the Serdes given to .to()', () => {
    const { app, stream } = stringStream()
    stream
      .map((key, value) => ({ key: { user: key }, value: value?.length ?? null }))
      .to('output', { key: json<{ user: string }>(), value: json<number>() })

    const driver = createTestDriver(app)
    driver.input<string, string>('input').pipe('ann', 'hello', { timestamp: 3 })

    expect(driver.output('output').read()).toEqual({
      key: { user: 'ann' },
      value: 5,
      timestamp: 3,
    })
  })

  it('drops the inherited Serdes, so .to() needs both of them', () => {
    const { stream } = stringStream()
    const mapped = stream.map((key, value) => ({ key, value }))

    expect(() => mapped.to('no-serdes')).toThrow(/key Serde.*"no-serdes"/)
    expect(() => mapped.to('no-value-serde', { key: serdes.string })).toThrow(
      /value Serde.*"no-value-serde"/,
    )
  })
})

describe('peek', () => {
  it('sees every record and passes it on unchanged', () => {
    const seen: string[] = []
    const { app, stream } = stringStream()
    stream.peek((key, value) => seen.push(`${key}:${value}`)).to('output')

    const driver = createTestDriver(app)
    const input = driver.input<string, string>('input')
    input.pipe('a', '1', { timestamp: 1 })
    input.pipe('b', '2', { timestamp: 2 })

    expect(seen).toEqual(['a:1', 'b:2'])
    expect(driver.output('output').readAll()).toEqual([
      { key: 'a', value: '1', timestamp: 1 },
      { key: 'b', value: '2', timestamp: 2 },
    ])
  })
})

describe('chained stateless operators', () => {
  it('runs filter, map and peek in order and fans out to several topics', () => {
    const peeked: number[] = []
    const { app, stream } = stringStream()
    const lengths = stream
      .filter((_key, value): value is string => value !== null && value !== '')
      .map((key, value) => ({ key, value: value.length }))
      .peek((_key, length) => peeked.push(length))
    lengths
      .filter((_key, length) => length > 2)
      .to('long', { key: serdes.string, value: json<number>() })
    lengths.to('all', { key: serdes.string, value: json<number>() })

    const driver = createTestDriver(app)
    const input = driver.input<string, string>('input')
    input.pipe('a', 'abc', { timestamp: 1 })
    input.pipe('b', '', { timestamp: 2 })
    input.pipe('c', 'x', { timestamp: 3 })

    expect(peeked).toEqual([3, 1])
    expect(driver.output('all').readAll()).toEqual([
      { key: 'a', value: 3, timestamp: 1 },
      { key: 'c', value: 1, timestamp: 3 },
    ])
    expect(driver.output('long').readAll()).toEqual([{ key: 'a', value: 3, timestamp: 1 }])
  })

  it('reuses one Stream in several independent chains', () => {
    const { app, stream } = stringStream()
    stream.filter((key) => key === 'a').to('only-a')
    stream.filter((key) => key === 'b').to('only-b')

    const driver = createTestDriver(app)
    const input = driver.input<string, string>('input')
    input.pipe('a', '1', { timestamp: 1 })
    input.pipe('b', '2', { timestamp: 2 })

    expect(driver.output('only-a').readAll()).toEqual([{ key: 'a', value: '1', timestamp: 1 }])
    expect(driver.output('only-b').readAll()).toEqual([{ key: 'b', value: '2', timestamp: 2 }])
  })
})

// Type-level tests: checked by `npm run typecheck`, not at runtime.
describe('type inference through the chain', () => {
  it('infers key and value types from the input Serdes, with null for tombstones', () => {
    const { stream } = stringStream()
    expectTypeOf(stream).toEqualTypeOf<Stream<string, string | null>>()
    stream.filter((key, value) => {
      expectTypeOf(key).toEqualTypeOf<string>()
      expectTypeOf(value).toEqualTypeOf<string | null>()
      return true
    })
  })

  it('carries the types map returns through the rest of the chain, with no annotations', () => {
    const { stream } = stringStream()
    const chained = stream
      .filter((_key, value): value is string => value !== null)
      .map((key, value) => ({ key: value.length, value: { name: key, tags: [value] } }))
      .filter((key, value) => {
        expectTypeOf(key).toEqualTypeOf<number>()
        expectTypeOf(value).toEqualTypeOf<{ name: string; tags: string[] }>()
        return key > 0
      })
      .peek((key, value) => {
        expectTypeOf(key).toEqualTypeOf<number>()
        expectTypeOf(value.tags).toEqualTypeOf<string[]>()
      })
      .map((key, value) => ({ key: value.name, value: key > 1 }))

    expectTypeOf(chained).toEqualTypeOf<Stream<string, boolean>>()
  })

  it('narrows the value type when filter is given a type guard', () => {
    const app = createStreamsApp({ applicationId: 'types' })
    const stream = app.stream('input', { key: serdes.string, value: json<string | number>() })
    const numbers = stream.filter((_key, value): value is number => typeof value === 'number')
    expectTypeOf(numbers).toEqualTypeOf<Stream<string, number>>()
  })

  it('requires .to() Serdes that match the mapped types', () => {
    const { stream } = stringStream()
    const mapped = stream.map((key, value) => ({ key, value: value?.length ?? null }))
    expect(() =>
      // @ts-expect-error a string Serde cannot write number values
      mapped.to('typed', { key: serdes.string, value: serdes.string }),
    ).not.toThrow()
  })

  it('rejects mappers that do not return a key and a value', () => {
    const { stream } = stringStream()
    // @ts-expect-error map must return { key, value }
    expect(() => stream.map((key) => ({ key }))).not.toThrow()
  })
})

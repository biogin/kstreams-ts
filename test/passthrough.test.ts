import { describe, expect, it } from 'vitest'
import { createStreamsApp, createTestDriver, type Serde, serdes } from '../src/index.js'

function passThroughApp() {
  const app = createStreamsApp({ applicationId: 'passthrough' })
  app.stream('input', { key: serdes.string, value: serdes.string }).to('output')
  return app
}

describe('pass-through Streams App in the Test Driver', () => {
  it('pipes a record from the input topic to the output topic unchanged', () => {
    const driver = createTestDriver(passThroughApp())
    driver.input<string, string>('input').pipe('k1', 'hello', { timestamp: 42 })

    expect(driver.output<string, string>('output').read()).toEqual({
      key: 'k1',
      value: 'hello',
      timestamp: 42,
    })
  })

  it('keeps record order and drains the output topic with readAll', () => {
    const driver = createTestDriver(passThroughApp())
    const input = driver.input<string, string>('input')
    input.pipe('a', '1', { timestamp: 1 })
    input.pipe('b', '2', { timestamp: 2 })
    input.pipe('a', '3', { timestamp: 3 })

    const output = driver.output<string, string>('output')
    expect(output.readAll()).toEqual([
      { key: 'a', value: '1', timestamp: 1 },
      { key: 'b', value: '2', timestamp: 2 },
      { key: 'a', value: '3', timestamp: 3 },
    ])
    expect(output.readAll()).toEqual([])
  })

  it('reads records one at a time and throws once the output topic is empty', () => {
    const driver = createTestDriver(passThroughApp())
    const input = driver.input<string, string>('input')
    input.pipe('a', '1', { timestamp: 1 })
    input.pipe('b', '2', { timestamp: 2 })

    const output = driver.output<string, string>('output')
    expect(output.read().value).toBe('1')
    expect(output.read().value).toBe('2')
    expect(() => output.read()).toThrow(/no records/i)
  })

  it('uses the Test Driver wall-clock time (starting at 0) when no timestamp is given', () => {
    const driver = createTestDriver(passThroughApp())
    driver.input<string, string>('input').pipe('k', 'v')

    expect(driver.output('output').read()).toEqual({ key: 'k', value: 'v', timestamp: 0 })
  })

  it('handles non-ASCII text through the string Serde', () => {
    const driver = createTestDriver(passThroughApp())
    driver.input<string, string>('input').pipe('ключ', 'héllo 👋', { timestamp: 5 })

    expect(driver.output('output').read()).toEqual({
      key: 'ключ',
      value: 'héllo 👋',
      timestamp: 5,
    })
  })

  it('sends records through the real Serdes on the way in and out', () => {
    const calls: string[] = []
    const tracing = (name: string): Serde<string> => ({
      serialize(value) {
        calls.push(`${name}.serialize(${value})`)
        return serdes.string.serialize(value)
      },
      deserialize(bytes) {
        const value = serdes.string.deserialize(bytes)
        calls.push(`${name}.deserialize(${value})`)
        return value
      },
    })
    const shouting: Serde<string> = {
      serialize: (value) => serdes.string.serialize(value.toUpperCase()),
      deserialize: (bytes) => serdes.string.deserialize(bytes),
    }

    const app = createStreamsApp({ applicationId: 'serdes' })
    app
      .stream('input', { key: tracing('inKey'), value: tracing('inValue') })
      .to('output', { value: shouting })

    const driver = createTestDriver(app)
    driver.input<string, string>('input').pipe('k', 'v', { timestamp: 1 })

    // The value was written with the output topic's own Serde; the key kept the input's.
    expect(driver.output('output').read()).toEqual({ key: 'k', value: 'V', timestamp: 1 })
    expect(calls).toEqual(
      expect.arrayContaining([
        'inKey.serialize(k)',
        'inKey.deserialize(k)',
        'inValue.serialize(v)',
        'inValue.deserialize(v)',
      ]),
    )
  })

  it('writes one Stream to several output topics', () => {
    const app = createStreamsApp({ applicationId: 'fan-out' })
    const stream = app.stream('input', { key: serdes.string, value: serdes.string })
    stream.to('out-a')
    stream.to('out-b')

    const driver = createTestDriver(app)
    driver.input<string, string>('input').pipe('k', 'v', { timestamp: 7 })

    expect(driver.output('out-a').readAll()).toEqual([{ key: 'k', value: 'v', timestamp: 7 }])
    expect(driver.output('out-b').readAll()).toEqual([{ key: 'k', value: 'v', timestamp: 7 }])
  })

  it('runs several independent pipelines in one Streams App', () => {
    const app = createStreamsApp({ applicationId: 'two-pipelines' })
    app.stream('in-a', { key: serdes.string, value: serdes.string }).to('out-a')
    app.stream('in-b', { key: serdes.string, value: serdes.string }).to('out-b')

    const driver = createTestDriver(app)
    driver.input<string, string>('in-b').pipe('k', 'b', { timestamp: 1 })

    expect(driver.output('out-a').readAll()).toEqual([])
    expect(driver.output('out-b').readAll()).toEqual([{ key: 'k', value: 'b', timestamp: 1 }])
  })

  it('gives each Test Driver its own topics', () => {
    const app = passThroughApp()
    const first = createTestDriver(app)
    const second = createTestDriver(app)
    first.input<string, string>('input').pipe('k', 'v', { timestamp: 1 })

    expect(second.output('output').readAll()).toEqual([])
    expect(first.output('output').readAll()).toHaveLength(1)
  })
})

describe('Streams App definition errors', () => {
  it('exposes its Application ID', () => {
    expect(createStreamsApp({ applicationId: 'orders-app.v1_2' }).applicationId).toBe(
      'orders-app.v1_2',
    )
  })

  it.each(['', 'has space', 'slash/name', 'ümlaut'])('rejects the Application ID %j', (id) => {
    expect(() => createStreamsApp({ applicationId: id })).toThrow(/application id/i)
  })

  it('rejects declaring the same input topic twice', () => {
    const app = createStreamsApp({ applicationId: 'dup' })
    app.stream('input', { key: serdes.string, value: serdes.string })
    expect(() => app.stream('input', { key: serdes.string, value: serdes.string })).toThrow(
      /"input"/,
    )
  })

  it('rejects piping into a topic the Streams App does not read', () => {
    const driver = createTestDriver(passThroughApp())
    expect(() => driver.input('nope')).toThrow(/"nope"/)
  })

  it('rejects reading a topic the Streams App does not write', () => {
    const driver = createTestDriver(passThroughApp())
    expect(() => driver.output('nope')).toThrow(/"nope"/)
  })
})

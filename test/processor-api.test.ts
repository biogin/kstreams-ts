import { describe, expect, it } from 'vitest'
import {
  createStreamsApp,
  createTestDriver,
  type ProcessorContext,
  type Serde,
  type StreamRecord,
  serdes,
} from '../src/index.js'

const strings = { key: serdes.string, value: serdes.string }

const delay = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms))

describe('Processor API in the Test Driver', () => {
  it('runs a Processor between DSL nodes (DSL → Processor → DSL)', async () => {
    const app = createStreamsApp({ applicationId: 'processor' })
    const shout = app.processor('shout', (ctx: ProcessorContext<string, string | null>) => ({
      process(record: StreamRecord<string, string | null>) {
        ctx.forward({ ...record, value: record.value?.toUpperCase() ?? null })
      },
    }))
    app.stream('input', strings).process(shout).to('output', strings)

    const driver = createTestDriver(app)
    await driver.input<string, string>('input').pipe('k', 'hi', { timestamp: 3 })

    expect(driver.output('output').readAll()).toEqual([{ key: 'k', value: 'HI', timestamp: 3 }])
  })

  it('lets a Processor forward any number of records, with new key and value types', async () => {
    const app = createStreamsApp({ applicationId: 'split' })
    const split = app.processor('split', (ctx: ProcessorContext<number, string>) => ({
      process(record: StreamRecord<string, string | null>) {
        ;(record.value ?? '').split(' ').forEach((word, i) => {
          if (word !== '') ctx.forward({ key: i, value: word, timestamp: record.timestamp })
        })
      },
    }))
    const numberKey: Serde<number> = {
      serialize: (n, ctx) => serdes.string.serialize(String(n), ctx),
      deserialize: (bytes, ctx) => Number(serdes.string.deserialize(bytes, ctx)),
    }
    app
      .stream('input', strings)
      .process(split)
      .to('output', { key: numberKey, value: serdes.string })

    const driver = createTestDriver(app)
    const input = driver.input<string, string>('input')
    await input.pipe('k', 'a b', { timestamp: 1 })
    await input.pipe('k', '', { timestamp: 2 })

    expect(driver.output('output').readAll()).toEqual([
      { key: 0, value: 'a', timestamp: 1 },
      { key: 1, value: 'b', timestamp: 1 },
    ])
  })

  it('forwards to every child of the Processor', async () => {
    const app = createStreamsApp({ applicationId: 'fan-out' })
    const pass = app.processor('pass', (ctx: ProcessorContext<string, string | null>) => ({
      process(record: StreamRecord<string, string | null>) {
        ctx.forward(record)
      },
    }))
    const out = app.stream('input', strings).process(pass)
    out.to('out-a', strings)
    out.to('out-b', strings)

    const driver = createTestDriver(app)
    await driver.input<string, string>('input').pipe('k', 'v', { timestamp: 1 })

    expect(driver.output('out-a').readAll()).toEqual([{ key: 'k', value: 'v', timestamp: 1 }])
    expect(driver.output('out-b').readAll()).toEqual([{ key: 'k', value: 'v', timestamp: 1 }])
  })

  it('chains Processors, and one Processor can read from several Streams', async () => {
    const app = createStreamsApp({ applicationId: 'chain' })
    const tag = (name: string) =>
      app.processor(name, (ctx: ProcessorContext<string, string | null>) => ({
        process(record: StreamRecord<string, string | null>) {
          ctx.forward({ ...record, value: `${record.value}>${name}` })
        },
      }))
    const first = tag('first')
    const second = tag('second')
    app.stream('in-a', strings).process(first).process(second).to('output', strings)
    app.stream('in-b', strings).process(second)

    const driver = createTestDriver(app)
    await driver.input<string, string>('in-a').pipe('k', 'a', { timestamp: 1 })
    await driver.input<string, string>('in-b').pipe('k', 'b', { timestamp: 2 })

    expect(driver.output<string, string>('output').readAll()).toEqual([
      { key: 'k', value: 'a>first>second', timestamp: 1 },
      { key: 'k', value: 'b>second', timestamp: 2 },
    ])
  })

  it('creates one Processor instance per Test Driver', async () => {
    const app = createStreamsApp({ applicationId: 'instances' })
    const counter = app.processor('counter', (ctx: ProcessorContext<string, string | null>) => {
      let seen = 0
      return {
        process(record: StreamRecord<string, string | null>) {
          seen += 1
          ctx.forward({ ...record, value: String(seen) })
        },
      }
    })
    app.stream('input', strings).process(counter).to('output', strings)

    const first = createTestDriver(app)
    const second = createTestDriver(app)
    await first.input<string, string>('input').pipe('k', 'v')
    await first.input<string, string>('input').pipe('k', 'v')
    await second.input<string, string>('input').pipe('k', 'v')

    expect(
      first
        .output<string, string>('output')
        .readAll()
        .map((r) => r.value),
    ).toEqual(['1', '2'])
    expect(
      second
        .output<string, string>('output')
        .readAll()
        .map((r) => r.value),
    ).toEqual(['1'])
  })
})

describe('async Processors', () => {
  it('awaits an async Processor before the piped record counts as processed', async () => {
    const app = createStreamsApp({ applicationId: 'async' })
    const enrich = app.processor('enrich', (ctx: ProcessorContext<string, string | null>) => ({
      async process(record: StreamRecord<string, string | null>) {
        await delay(5)
        ctx.forward({ ...record, value: `${record.value}+enriched` })
      },
    }))
    app.stream('input', strings).process(enrich).to('output', strings)

    const driver = createTestDriver(app)
    await driver.input<string, string>('input').pipe('k', 'v', { timestamp: 1 })

    expect(driver.output('output').readAll()).toEqual([
      { key: 'k', value: 'v+enriched', timestamp: 1 },
    ])
  })

  it('never overlaps records within one Task, and keeps their order', async () => {
    const events: string[] = []
    let active = 0
    let maxActive = 0
    const app = createStreamsApp({ applicationId: 'ordering' })
    const slow = app.processor('slow', (ctx: ProcessorContext<string, string | null>) => ({
      async process(record: StreamRecord<string, string | null>) {
        active += 1
        maxActive = Math.max(maxActive, active)
        events.push(`start ${record.value}`)
        // Earlier records wait longest, so overlapping records would finish out of order.
        await delay(Number(record.value))
        events.push(`end ${record.value}`)
        active -= 1
        ctx.forward(record)
      },
    }))
    app.stream('input', strings).process(slow).to('output', strings)

    const driver = createTestDriver(app)
    const input = driver.input<string, string>('input')
    // Piped without awaiting in between: the Task queues them.
    await Promise.all([input.pipe('k', '20'), input.pipe('j', '10'), input.pipe('k', '1')])

    expect(maxActive).toBe(1)
    expect(events).toEqual(['start 20', 'end 20', 'start 10', 'end 10', 'start 1', 'end 1'])
    expect(
      driver
        .output<string, string>('output')
        .readAll()
        .map((r) => r.value),
    ).toEqual(['20', '10', '1'])
  })

  it('awaits async children of a Processor, one record after another, even if forward is not awaited', async () => {
    let active = 0
    let maxActive = 0
    const app = createStreamsApp({ applicationId: 'async-children' })
    const twice = app.processor('twice', (ctx: ProcessorContext<string, string | null>) => ({
      process(record: StreamRecord<string, string | null>) {
        ctx.forward({ ...record, value: `${record.value}-1` })
        ctx.forward({ ...record, value: `${record.value}-2` })
      },
    }))
    const slow = app.processor('slow', (ctx: ProcessorContext<string, string | null>) => ({
      async process(record: StreamRecord<string, string | null>) {
        active += 1
        maxActive = Math.max(maxActive, active)
        await delay(record.value?.endsWith('-1') ? 10 : 1)
        active -= 1
        ctx.forward(record)
      },
    }))
    app.stream('input', strings).process(twice).process(slow).to('output', strings)

    const driver = createTestDriver(app)
    await driver.input<string, string>('input').pipe('k', 'v')

    expect(maxActive).toBe(1)
    expect(
      driver
        .output<string, string>('output')
        .readAll()
        .map((r) => r.value),
    ).toEqual(['v-1', 'v-2'])
  })

  it('rejects the pipe when an async Processor fails, and stops processing after it', async () => {
    const app = createStreamsApp({ applicationId: 'async-failure' })
    const failing = app.processor('failing', (ctx: ProcessorContext<string, string | null>) => ({
      async process(record: StreamRecord<string, string | null>) {
        await delay(1)
        if (record.value === 'bad') throw new Error('enrichment failed')
        ctx.forward(record)
      },
    }))
    app.stream('input', strings).process(failing).to('output', strings)

    const driver = createTestDriver(app)
    const input = driver.input<string, string>('input')
    const bad = input.pipe('k', 'bad')
    const next = input.pipe('k', 'good')

    await expect(bad).rejects.toThrow('enrichment failed')
    await expect(next).rejects.toThrow('enrichment failed')
    expect(driver.output('output').readAll()).toEqual([])
  })
})

describe('Processor definition errors', () => {
  const noop = () => ({ process() {} })

  it('exposes the Processor name', () => {
    expect(createStreamsApp({ applicationId: 'names' }).processor('enrich', noop).name).toBe(
      'enrich',
    )
  })

  it('rejects two Processors with the same name', () => {
    const app = createStreamsApp({ applicationId: 'dup' })
    app.processor('enrich', noop)
    expect(() => app.processor('enrich', noop)).toThrow(/"enrich"/)
  })

  it('rejects an empty Processor name', () => {
    expect(() => createStreamsApp({ applicationId: 'empty' }).processor('', noop)).toThrow(/name/i)
  })

  it('rejects a Processor from another Streams App', () => {
    const other = createStreamsApp({ applicationId: 'other' }).processor('p', noop)
    const app = createStreamsApp({ applicationId: 'mine' })
    expect(() => app.stream('input', strings).process(other)).toThrow(/"p"/)
  })

  it('needs output Serdes after a Processor, since its record types may differ', () => {
    const app = createStreamsApp({ applicationId: 'serdes' })
    const p = app.processor('p', (_ctx: ProcessorContext<string, string | null>) => ({
      process(_record: StreamRecord<string, string | null>) {},
    }))
    expect(() => app.stream('input', strings).process(p).to('output')).toThrow(/Serde/)
  })

  it('checks record types between a Stream and a Processor at compile time', () => {
    const app = createStreamsApp({ applicationId: 'types' })
    const numbers = app.processor('numbers', (_ctx: ProcessorContext<string, number>) => ({
      process(_record: StreamRecord<string, number>) {},
    }))
    const stream = app.stream('input', strings)
    // @ts-expect-error: the Stream has string values, the Processor reads numbers
    expect(() => stream.process(numbers)).not.toThrow()
  })
})

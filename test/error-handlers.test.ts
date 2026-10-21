import { describe, expect, it } from 'vitest'
import {
  AsyncNotAllowedError,
  createStreamsApp,
  createTestDriver,
  DeserializationError,
  deadLetter,
  type ErrorHandler,
  type ErrorHandlerContext,
  type ErrorHandlers,
  type ProcessorContext,
  type StreamRecord,
  serdes,
} from '../src/index.js'

interface Order {
  id: string
  amount: number
}

const decoder = new TextDecoder()
const text = (bytes: Uint8Array | null | undefined) =>
  bytes === null || bytes === undefined ? bytes : decoder.decode(bytes)

/**
 * Reads JSON orders from `orders` and writes their amounts to `amounts`; an order with a negative
 * amount fails processing.
 */
function ordersApp(errorHandlers?: ErrorHandlers) {
  const app = createStreamsApp({
    applicationId: 'orders',
    ...(errorHandlers === undefined ? {} : { errorHandlers }),
  })
  app
    .stream('orders', { key: serdes.string, value: serdes.json<Order>() })
    .map((key, order) => {
      if (order === null) return { key, value: null }
      if (order.amount < 0) throw new RangeError(`Negative amount in order ${order.id}`)
      return { key, value: order.amount }
    })
    .to('amounts', { key: serdes.string, value: serdes.number })
  return createTestDriver(app)
}

async function failureOf(work: Promise<unknown>): Promise<unknown> {
  return work.then(
    () => {
      throw new Error('expected a failure')
    },
    (error: unknown) => error,
  )
}

describe('error handlers', () => {
  it('stop the Streams App on a processing error by default', async () => {
    const driver = ordersApp()
    const orders = driver.input<string, Order>('orders')

    await expect(orders.pipe('o-1', { id: 'o-1', amount: -1 })).rejects.toThrow(RangeError)
    await expect(orders.pipe('o-2', { id: 'o-2', amount: 2 })).rejects.toThrow(/stopped/)
  })

  it('skip a malformed record when the deserialization handler continues', async () => {
    const seen: ErrorHandlerContext[] = []
    const driver = ordersApp({
      deserialization: (context) => {
        seen.push(context)
        return 'continue'
      },
    })
    const raw = driver.input<string, string>('orders', { value: serdes.string })

    await raw.pipe('o-1', 'not json', { timestamp: 7 })
    await driver.input<string, Order>('orders').pipe('o-2', { id: 'o-2', amount: 2 })

    expect(driver.output('amounts').readAll()).toEqual([{ key: 'o-2', value: 2, timestamp: 0 }])
    expect(seen).toHaveLength(1)
    const [context] = seen
    expect(context?.kind).toBe('deserialization')
    expect(context?.error).toBeInstanceOf(DeserializationError)
    expect(context?.record).toMatchObject({ topic: 'orders', partition: 0, offset: 0n })
    expect(text(context?.record.value)).toBe('not json')
    expect(text(context?.record.key)).toBe('o-1')
    expect(context?.record.timestamp).toBe(7)
  })

  it('stop the Streams App when the handler answers fail', async () => {
    const driver = ordersApp({ processing: () => 'fail' })
    const orders = driver.input<string, Order>('orders')

    await expect(orders.pipe('o-1', { id: 'o-1', amount: -1 })).rejects.toThrow(RangeError)
    await expect(orders.pipe('o-2', { id: 'o-2', amount: 2 })).rejects.toThrow(/stopped/)
  })

  it('send processing errors to the processing handler only', async () => {
    const kinds: string[] = []
    const record: ErrorHandler = ({ kind }) => {
      kinds.push(kind)
      return 'continue'
    }
    const driver = ordersApp({ processing: record })
    const orders = driver.input<string, Order>('orders')

    await orders.pipe('o-1', { id: 'o-1', amount: -1 })
    await orders.pipe('o-2', { id: 'o-2', amount: 2 })
    expect(kinds).toEqual(['processing'])
    expect(driver.output('amounts').readAll()).toEqual([{ key: 'o-2', value: 2, timestamp: 0 }])

    // No deserialization handler: a malformed record still stops the app.
    const raw = driver.input<string, string>('orders', { value: serdes.string })
    await expect(raw.pipe('o-3', '{')).rejects.toThrow(DeserializationError)
  })

  it('count offsets per input topic in the Test Driver', async () => {
    const offsets: bigint[] = []
    const driver = ordersApp({
      processing: ({ record }) => {
        offsets.push(record.offset)
        return 'continue'
      },
    })
    const orders = driver.input<string, Order>('orders')
    await orders.pipe('o-1', { id: 'o-1', amount: 1 })
    await orders.pipe('o-2', { id: 'o-2', amount: -2 })
    await orders.pipe('o-3', { id: 'o-3', amount: -3 })

    expect(offsets).toEqual([1n, 2n])
  })

  it('wait for every async Processor of the failed record before going on', async () => {
    const events: string[] = []
    const app = createStreamsApp({
      applicationId: 'async-fail',
      errorHandlers: {
        processing: ({ error }) => {
          events.push(`handled ${(error as Error).message}`)
          return 'continue'
        },
      },
    })
    const slow = app.processor('slow', (ctx: ProcessorContext<string, string | null>) => ({
      async process(record: StreamRecord<string, string | null>) {
        await new Promise((resolve) => setTimeout(resolve, 5))
        events.push(`slow ${record.value}`)
        ctx.forward(record)
      },
    }))
    const failing = app.processor('failing', () => ({
      async process(record: StreamRecord<string, string | null>) {
        if (record.value === 'bad') throw new Error('bad record')
      },
    }))
    const input = app.stream('input', { key: serdes.string, value: serdes.string })
    input.process(failing)
    input.process(slow).to('output', { key: serdes.string, value: serdes.string })
    const driver = createTestDriver(app)

    await driver.input<string, string>('input').pipe('k', 'bad')
    await driver.input<string, string>('input').pipe('k', 'good')

    expect(events).toEqual(['slow bad', 'handled bad record', 'slow good'])
    expect(
      driver
        .output('output')
        .readAll()
        .map(({ value }) => value),
    ).toEqual(['bad', 'good'])
  })

  it('never get an AsyncNotAllowedError: it always stops the Streams App', async () => {
    const handled: unknown[] = []
    const app = createStreamsApp({
      applicationId: 'async-not-allowed',
      errorHandlers: {
        processing: (context) => {
          handled.push(context.error)
          return 'continue'
        },
      },
    })
    app
      .stream('input', { key: serdes.string, value: serdes.string })
      .filter((_key, value) => (value === 'async' ? (Promise.resolve(true) as never) : true))
      .to('output')
    const driver = createTestDriver(app)
    const input = driver.input<string, string>('input')

    await expect(input.pipe('k', 'async')).rejects.toThrow(AsyncNotAllowedError)
    await expect(input.pipe('k', 'sync')).rejects.toThrow(/stopped/)
    expect(handled).toEqual([])
  })

  it('stop the Streams App with the error a handler throws', async () => {
    const driver = ordersApp({
      processing: () => {
        throw new Error('handler broke')
      },
    })
    await expect(
      driver.input<string, Order>('orders').pipe('o-1', { id: 'o-1', amount: -1 }),
    ).rejects.toThrow('handler broke')
  })

  it('must be synchronous', async () => {
    const driver = ordersApp({ processing: (async () => 'continue') as unknown as ErrorHandler })
    const error = await failureOf(
      driver.input<string, Order>('orders').pipe('o-1', { id: 'o-1', amount: -1 }),
    )
    expect(error).toBeInstanceOf(TypeError)
    expect((error as Error).message).toMatch(/synchronous/)
    expect((error as Error).cause).toBeInstanceOf(RangeError)
  })

  it('reject an answer other than continue or fail', async () => {
    const driver = ordersApp({ processing: (() => 'skip') as unknown as ErrorHandler })
    await expect(
      driver.input<string, Order>('orders').pipe('o-1', { id: 'o-1', amount: -1 }),
    ).rejects.toThrow(/'continue' or 'fail'/)
  })

  it('do not get errors raised by timers', async () => {
    let handled = 0
    const app = createStreamsApp({
      applicationId: 'timer-fail',
      errorHandlers: {
        processing: () => {
          handled++
          return 'continue'
        },
      },
    })
    const ticking = app.processor('ticking', (ctx: ProcessorContext<string, string | null>) => {
      ctx.schedule(10, 'wall-clock-time', () => {
        throw new Error('timer failed')
      })
      return { process: (record: StreamRecord<string, string | null>) => ctx.forward(record) }
    })
    app
      .stream('input', { key: serdes.string, value: serdes.string })
      .process(ticking)
      .to('output', { key: serdes.string, value: serdes.string })
    const driver = createTestDriver(app)

    await expect(driver.advanceWallClockTime(10)).rejects.toThrow('timer failed')
    expect(handled).toBe(0)
  })

  it('are checked when the app is defined', () => {
    expect(() =>
      createStreamsApp({
        applicationId: 'bad-handlers',
        errorHandlers: { processing: 'dead-letter' as unknown as ErrorHandler },
      }),
    ).toThrow(/errorHandlers.processing must be a function/)
    expect(() =>
      createStreamsApp({
        applicationId: 'bad-handlers',
        errorHandlers: { serialization: deadLetter('dlq') } as ErrorHandlers,
      }),
    ).toThrow(/Unknown error handler "serialization"/)
  })
})

describe('deadLetter(topic)', () => {
  it('writes a malformed record as it was read, with the error in its headers', async () => {
    const driver = ordersApp({ deserialization: deadLetter('orders-dlq') })
    const raw = driver.input<string, string>('orders', { value: serdes.string })

    await raw.pipe('o-1', '{"id":', { timestamp: 42 })
    await driver.input<string, Order>('orders').pipe('o-2', { id: 'o-2', amount: 2 })

    expect(driver.output('amounts').readAll()).toEqual([{ key: 'o-2', value: 2, timestamp: 0 }])
    const [dead, ...more] = driver.output('orders-dlq').readAll()
    expect(more).toEqual([])
    expect(text(dead?.key as Uint8Array)).toBe('o-1')
    expect(text(dead?.value as Uint8Array)).toBe('{"id":')
    expect(dead?.timestamp).toBe(42)
    const headers = Object.fromEntries(
      Object.entries(dead?.headers ?? {}).map(([name, value]) => [name, text(value)]),
    )
    expect(headers).toMatchObject({
      'kstreams.error.kind': 'deserialization',
      'kstreams.error.name': 'DeserializationError',
      'kstreams.error.topic': 'orders',
      'kstreams.error.partition': '0',
      'kstreams.error.offset': '0',
    })
    expect(headers['kstreams.error.message']).toMatch(/Cannot deserialize the value/)
    expect(headers['kstreams.error.stack']).toMatch(/DeserializationError/)
  })

  it('writes the input record of a failed processing, keeping tombstones and null keys', async () => {
    const app = createStreamsApp({
      applicationId: 'dlq-processing',
      errorHandlers: { processing: deadLetter('dlq') },
    })
    app
      .stream('input', { key: serdes.string, value: serdes.string })
      .peek(() => {
        throw new Error('always fails')
      })
      .to('output')
    const driver = createTestDriver(app)
    await driver.input<string, string>('input').pipe(null, null, { timestamp: 3 })

    const [dead] = driver.output('dlq').readAll()
    expect(dead).toMatchObject({ key: null, value: null, timestamp: 3 })
    expect(text(dead?.headers?.['kstreams.error.kind'])).toBe('processing')
    expect(text(dead?.headers?.['kstreams.error.message'])).toBe('always fails')
    expect(driver.output('output').readAll()).toEqual([])
  })

  it('is readable in the Test Driver before anything is written, with any Serdes', async () => {
    const driver = ordersApp({ deserialization: deadLetter('orders-dlq') })
    const dlq = driver.output<string, string>('orders-dlq', {
      key: serdes.string,
      value: serdes.string,
    })
    expect(dlq.readAll()).toEqual([])
    await driver.input<string, string>('orders', { value: serdes.string }).pipe('o', 'nope')
    expect(dlq.read()).toMatchObject({ key: 'o', value: 'nope' })
  })

  it('needs a valid topic name', () => {
    expect(() => deadLetter('')).toThrow(/topic/)
    expect(() => deadLetter('no spaces' as string)).toThrow(/topic/)
  })
})

import { describe, expect, it } from 'vitest'
import {
  createStreamsApp,
  createTestDriver,
  type ProcessorContext,
  type StreamRecord,
  serdes,
  type TimerType,
} from '../src/index.js'

const strings = { key: serdes.string, value: serdes.string }

const delay = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms))

/**
 * A Streams App whose Processor schedules one timer when it is created and forwards a record with
 * the fire time each time the timer fires. Records it receives are forwarded unchanged.
 */
function timerApp(intervalMs: number, type: TimerType) {
  const app = createStreamsApp({ applicationId: `timer-${type}` })
  const ticker = app.processor('ticker', (ctx: ProcessorContext<string, string | null>) => {
    ctx.schedule(intervalMs, type, (timestamp) => {
      ctx.forward({ key: 'tick', value: String(timestamp), timestamp })
    })
    return {
      process(record: StreamRecord<string, string | null>) {
        ctx.forward(record)
      },
    }
  })
  app.stream('input', strings).process(ticker).to('output', strings)
  const driver = createTestDriver(app)
  return { driver, input: driver.input<string, string>('input'), output: driver.output('output') }
}

const ticks = (records: StreamRecord<unknown, unknown>[]) =>
  records.filter((r) => r.key === 'tick').map((r) => r.timestamp)

describe('Event Time', () => {
  it('gives output records the Event Time of the input record that produced them', async () => {
    const app = createStreamsApp({ applicationId: 'event-time' })
    app
      .stream('input', strings)
      .filter((_k, v) => v !== 'drop')
      .map((k, v) => ({ key: k.toUpperCase(), value: `${v}!` }))
      .to('output', strings)

    const driver = createTestDriver(app)
    const input = driver.input<string, string>('input')
    await input.pipe('a', '1', { timestamp: 50 })
    await input.pipe('b', 'drop', { timestamp: 60 })
    await input.pipe('c', '2', { timestamp: 10 })

    expect(driver.output('output').readAll()).toEqual([
      { key: 'A', value: '1!', timestamp: 50 },
      { key: 'C', value: '2!', timestamp: 10 },
    ])
  })
})

describe('Stream Time timers', () => {
  it('fire as Stream Time advances, every interval after the first record', async () => {
    const { input, output } = timerApp(10, 'stream-time')
    await input.pipe('k', 'a', { timestamp: 100 })
    expect(ticks(output.readAll())).toEqual([])

    await input.pipe('k', 'b', { timestamp: 109 })
    expect(ticks(output.readAll())).toEqual([])

    await input.pipe('k', 'c', { timestamp: 110 })
    expect(output.readAll()).toEqual([
      { key: 'k', value: 'c', timestamp: 110 },
      { key: 'tick', value: '110', timestamp: 110 },
    ])

    await input.pipe('k', 'd', { timestamp: 125 })
    expect(ticks(output.readAll())).toEqual([125])
  })

  it('only move forward: an out-of-order record does not move Stream Time back', async () => {
    const { input, output } = timerApp(10, 'stream-time')
    await input.pipe('k', 'a', { timestamp: 100 })
    await input.pipe('k', 'late', { timestamp: 50 })
    await input.pipe('k', 'b', { timestamp: 105 })
    expect(ticks(output.readAll())).toEqual([])

    await input.pipe('k', 'c', { timestamp: 110 })
    expect(ticks(output.readAll())).toEqual([110])
  })

  it('fire once when Stream Time jumps over several intervals', async () => {
    const { input, output } = timerApp(10, 'stream-time')
    await input.pipe('k', 'a', { timestamp: 0 })
    await input.pipe('k', 'b', { timestamp: 55 })
    expect(ticks(output.readAll())).toEqual([55])

    await input.pipe('k', 'c', { timestamp: 59 })
    expect(ticks(output.readAll())).toEqual([])
    await input.pipe('k', 'd', { timestamp: 60 })
    expect(ticks(output.readAll())).toEqual([60])
  })

  it('do not fire as wall-clock time passes', async () => {
    const { driver, input, output } = timerApp(10, 'stream-time')
    await input.pipe('k', 'a', { timestamp: 0 })
    await driver.advanceWallClockTime(1000)
    expect(ticks(output.readAll())).toEqual([])
  })

  it('can be cancelled', async () => {
    const app = createStreamsApp({ applicationId: 'cancel' })
    const once = app.processor('once', (ctx: ProcessorContext<string, string | null>) => {
      const timer = ctx.schedule(10, 'stream-time', (timestamp) => {
        ctx.forward({ key: 'tick', value: 'once', timestamp })
        timer.cancel()
      })
      return { process() {} }
    })
    app.stream('input', strings).process(once).to('output', strings)

    const driver = createTestDriver(app)
    const input = driver.input<string, string>('input')
    for (const timestamp of [0, 10, 20, 30]) await input.pipe('k', 'v', { timestamp })

    expect(driver.output('output').readAll()).toEqual([
      { key: 'tick', value: 'once', timestamp: 10 },
    ])
  })

  it('can be scheduled while processing a record, starting from the current Stream Time', async () => {
    const app = createStreamsApp({ applicationId: 'timeout' })
    const timeout = app.processor('timeout', (ctx: ProcessorContext<string, string | null>) => {
      let armed = false
      return {
        process(record: StreamRecord<string, string | null>) {
          if (armed) return
          armed = true
          const timer = ctx.schedule(30, 'stream-time', (timestamp) => {
            ctx.forward({ key: record.key, value: 'timed out', timestamp })
            timer.cancel()
          })
        },
      }
    })
    app.stream('input', strings).process(timeout).to('output', strings)

    const driver = createTestDriver(app)
    const input = driver.input<string, string>('input')
    await input.pipe('k', 'start', { timestamp: 100 })
    await input.pipe('k', 'x', { timestamp: 129 })
    expect(driver.output('output').readAll()).toEqual([])
    await input.pipe('k', 'x', { timestamp: 130 })
    expect(driver.output('output').readAll()).toEqual([
      { key: 'k', value: 'timed out', timestamp: 130 },
    ])
  })

  it('fire after the record that advanced Stream Time is fully processed, async ones awaited', async () => {
    const events: string[] = []
    const app = createStreamsApp({ applicationId: 'async-timer' })
    const slow = app.processor('slow', (ctx: ProcessorContext<string, string | null>) => {
      ctx.schedule(10, 'stream-time', async (timestamp) => {
        events.push(`timer ${timestamp} start`)
        await delay(5)
        events.push(`timer ${timestamp} end`)
        ctx.forward({ key: 'tick', value: String(timestamp), timestamp })
      })
      return {
        async process(record: StreamRecord<string, string | null>) {
          events.push(`record ${record.value} start`)
          await delay(5)
          events.push(`record ${record.value} end`)
        },
      }
    })
    app.stream('input', strings).process(slow).to('output', strings)

    const driver = createTestDriver(app)
    const input = driver.input<string, string>('input')
    input.pipe('k', 'a', { timestamp: 0 })
    input.pipe('k', 'b', { timestamp: 10 })
    await input.pipe('k', 'c', { timestamp: 11 })

    expect(events).toEqual([
      'record a start',
      'record a end',
      'record b start',
      'record b end',
      'timer 10 start',
      'timer 10 end',
      'record c start',
      'record c end',
    ])
    expect(driver.output('output').readAll()).toEqual([{ key: 'tick', value: '10', timestamp: 10 }])
  })

  it('reject an interval that is not a positive number', () => {
    const app = createStreamsApp({ applicationId: 'bad-interval' })
    const bad = app.processor('bad', (ctx: ProcessorContext<string, string | null>) => {
      ctx.schedule(0, 'stream-time', () => {})
      return { process() {} }
    })
    app.stream('input', strings).process(bad).to('output', strings)

    expect(() => createTestDriver(app)).toThrow(/interval/)
  })
})

describe('wall-clock timers', () => {
  it('fire as the Test Driver wall-clock time is advanced', async () => {
    const { driver, output } = timerApp(1000, 'wall-clock-time')
    await driver.advanceWallClockTime(999)
    expect(ticks(output.readAll())).toEqual([])

    await driver.advanceWallClockTime(1)
    expect(output.readAll()).toEqual([{ key: 'tick', value: '1000', timestamp: 1000 }])

    await driver.advanceWallClockTime(2500)
    expect(ticks(output.readAll())).toEqual([3500])
    await driver.advanceWallClockTime(500)
    expect(ticks(output.readAll())).toEqual([4000])
  })

  it('do not fire as Stream Time advances', async () => {
    const { input, output } = timerApp(10, 'wall-clock-time')
    await input.pipe('k', 'a', { timestamp: 0 })
    await input.pipe('k', 'b', { timestamp: 1000 })
    expect(ticks(output.readAll())).toEqual([])
  })

  it('records piped without a timestamp take the current wall-clock time', async () => {
    const { driver, input, output } = timerApp(1000, 'wall-clock-time')
    await driver.advanceWallClockTime(250)
    await input.pipe('k', 'v')
    expect(output.readAll()).toEqual([{ key: 'k', value: 'v', timestamp: 250 }])
  })

  it('wait for queued records before firing', async () => {
    const events: string[] = []
    const app = createStreamsApp({ applicationId: 'wall-clock-queue' })
    const slow = app.processor('slow', (ctx: ProcessorContext<string, string | null>) => {
      ctx.schedule(10, 'wall-clock-time', (timestamp) => {
        events.push(`timer ${timestamp}`)
      })
      return {
        async process(record: StreamRecord<string, string | null>) {
          await delay(5)
          events.push(`record ${record.value}`)
        },
      }
    })
    app.stream('input', strings).process(slow).to('output', strings)

    const driver = createTestDriver(app)
    driver.input<string, string>('input').pipe('k', 'a', { timestamp: 0 })
    await driver.advanceWallClockTime(10)

    expect(events).toEqual(['record a', 'timer 10'])
  })

  it('stop the Streams App when a timer throws', async () => {
    const app = createStreamsApp({ applicationId: 'timer-throws' })
    const boom = app.processor('boom', (ctx: ProcessorContext<string, string | null>) => {
      ctx.schedule(10, 'wall-clock-time', () => {
        throw new Error('boom')
      })
      return { process() {} }
    })
    app.stream('input', strings).process(boom).to('output', strings)

    const driver = createTestDriver(app)
    await expect(driver.advanceWallClockTime(10)).rejects.toThrow('boom')
    await expect(driver.input<string, string>('input').pipe('k', 'v')).rejects.toThrow(/stopped/)
  })

  it('rejects moving wall-clock time backwards', async () => {
    const { driver } = timerApp(10, 'wall-clock-time')
    await expect(driver.advanceWallClockTime(-1)).rejects.toThrow(/backwards/)
  })
})

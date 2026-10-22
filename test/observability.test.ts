import { describe, expect, expectTypeOf, it } from 'vitest'
import {
  type ApplicationState,
  createStreamsApp,
  createTestDriver,
  type Metric,
  type MetricsSink,
  type RestoreProgressEvent,
  type StateChangeEvent,
  serdes,
  type TopicPartition,
  tumbling,
} from '../src/index.js'
import { Listeners, recordMetric } from '../src/observability.js'
import { total } from './support/metrics.js'

// Observability on Kafka (Application States while rebalancing and restoring, rebalance and
// restore events, Worker metrics) is covered by test/integration/observability.test.ts.

const strings = { key: serdes.string, value: serdes.string }

/** A metrics sink that keeps every metric it is given. */
function recordingSink(): MetricsSink & { metrics: Metric[] } {
  const metrics: Metric[] = []
  return { metrics, record: (metric) => metrics.push(metric) }
}

describe('Streams App events', () => {
  it('types each event with its payload', () => {
    const app = createStreamsApp({ applicationId: 'event-types' })
    app.on('state', (event) => {
      expectTypeOf(event).toEqualTypeOf<StateChangeEvent>()
      expectTypeOf(event.state).toEqualTypeOf<ApplicationState>()
      expectTypeOf(event.previous).toEqualTypeOf<ApplicationState>()
    })
    app.on('error', (event) => expectTypeOf(event.error).toEqualTypeOf<unknown>())
    app.on('rebalance', (event) => {
      expectTypeOf(event.worker).toEqualTypeOf<number>()
      expectTypeOf(event.assigned).toEqualTypeOf<TopicPartition[]>()
      expectTypeOf(event.revoked).toEqualTypeOf<TopicPartition[]>()
    })
    app.on('restore-progress', (event) => {
      expectTypeOf(event).toEqualTypeOf<RestoreProgressEvent>()
    })
    // @ts-expect-error: no such event
    app.on('no-such-event', () => {})
    // @ts-expect-error: the wrong payload for the event
    app.on('state', (event: RestoreProgressEvent) => event)
  })

  it('reports state changes, until the listener is removed', async () => {
    const app = createStreamsApp({ applicationId: 'event-states' })
    const changes: StateChangeEvent[] = []
    const listener = (event: StateChangeEvent) => changes.push(event)
    expect(app.on('state', listener)).toBe(app)
    await app.stop()
    expect(changes).toEqual([{ state: 'stopped', previous: 'created' }])

    const other = createStreamsApp({ applicationId: 'event-states-off' })
    other.on('state', listener).off('state', listener)
    await other.stop()
    expect(changes).toHaveLength(1)
  })

  it('ignores exceptions thrown by listeners', async () => {
    const app = createStreamsApp({ applicationId: 'event-throws' })
    const seen: ApplicationState[] = []
    app.on('state', () => {
      throw new Error('listener bug')
    })
    app.on('state', (event) => seen.push(event.state))
    await expect(app.stop()).resolves.toBeUndefined()
    expect(seen).toEqual(['stopped'])
  })
})

describe('Metrics sink', () => {
  it('needs a record function', () => {
    // biome-ignore lint/suspicious/noExplicitAny: simulates an untyped caller
    const metrics: any = {}
    expect(() => createStreamsApp({ applicationId: 'bad-sink', metrics })).toThrow(
      /metrics must be a metrics sink with a record\(metric\) function/,
    )
  })

  it('types each metric with its labels', () => {
    const sink: MetricsSink = {
      record(metric) {
        if (metric.name === 'consumer-lag') {
          expectTypeOf(metric.type).toEqualTypeOf<'gauge'>()
          expectTypeOf(metric.labels).toEqualTypeOf<{
            worker: number
            topic: string
            partition: number
          }>()
        }
        if (metric.name === 'records-dropped') {
          expectTypeOf(metric.labels.kind).toEqualTypeOf<'deserialization' | 'processing'>()
        }
        if (metric.name === 'wait-duration') {
          expectTypeOf(metric.labels.wait).toEqualTypeOf<
            'task-lock' | 'stable-offsets' | 'changelog-transactions'
          >()
        }
      },
    }
    expect(sink).toBeDefined()
  })
})

describe('Test Driver metrics', () => {
  it('counts processed records, records skipped by error handlers and late records', async () => {
    const sink = recordingSink()
    const app = createStreamsApp({
      applicationId: 'driver-metrics',
      metrics: sink,
      errorHandlers: { deserialization: () => 'continue', processing: () => 'continue' },
    })
    app
      .stream('clicks', { key: serdes.string, value: serdes.json<{ page: string }>() })
      .filter((_key, value): value is { page: string } => value !== null)
      .peek((_key, value) => {
        if (value.page === 'boom') throw new Error('Processing failed on purpose')
      })
      .groupByKey()
      .window(tumbling('1m'))
      .count()
      .toStream()
      .map((key, count) => ({ key: key.key, value: String(count) }))
      .to('counts', strings)
    const driver = createTestDriver(app)
    const clicks = driver.input<string, { page: string }>('clicks')
    await clicks.pipe('alice', { page: 'home' }, { timestamp: 70_000 })
    await clicks.pipe('alice', { page: 'late' }, { timestamp: 1_000 }) // its window has closed
    await clicks.pipe('alice', { page: 'boom' }, { timestamp: 71_000 }) // processing error
    await driver
      .input('clicks', { value: serdes.string })
      .pipe('alice', 'not json', { timestamp: 72_000 }) // deserialization error

    expect(driver.metrics()).toEqual({
      recordsProcessed: 4,
      recordsDropped: 2,
      lateRecordsDropped: 1,
    })
    // The app's metrics sink gets the same counts, as Worker 0's Task 0.
    expect(total(sink.metrics, 'records-processed')).toBe(4)
    expect(total(sink.metrics, 'late-records-dropped')).toBe(1)
    const dropped = sink.metrics.filter((metric) => metric.name === 'records-dropped')
    expect(dropped).toEqual([
      {
        name: 'records-dropped',
        type: 'counter',
        value: 1,
        labels: { worker: 0, task: 0, kind: 'processing' },
      },
      {
        name: 'records-dropped',
        type: 'counter',
        value: 1,
        labels: { worker: 0, task: 0, kind: 'deserialization' },
      },
    ])
  })
})

describe('async listeners and metrics sinks', () => {
  /** Collects the unhandled rejections reported while `run` executes and one turn of the event loop. */
  async function unhandledRejectionsDuring(run: () => void): Promise<unknown[]> {
    const rejections: unknown[] = []
    const listener = (reason: unknown) => rejections.push(reason)
    process.on('unhandledRejection', listener)
    try {
      run()
      await new Promise((resolve) => setTimeout(resolve, 10))
    } finally {
      process.off('unhandledRejection', listener)
    }
    return rejections
  }

  it('ignores a listener whose returned Promise rejects', async () => {
    const listeners = new Listeners()
    listeners.on('state', (async () => {
      throw new Error('listener bug')
    }) as unknown as () => void)
    const rejections = await unhandledRejectionsDuring(() =>
      listeners.emit('state', { state: 'running', previous: 'created' } as StateChangeEvent),
    )
    expect(rejections).toEqual([])
  })

  it('ignores a metrics sink whose record() returns a rejecting Promise', async () => {
    const sink: MetricsSink = {
      record: (async () => {
        throw new Error('sink bug')
      }) as unknown as MetricsSink['record'],
    }
    const rejections = await unhandledRejectionsDuring(() =>
      recordMetric(sink, {
        name: 'records-processed',
        type: 'counter',
        value: 1,
        labels: { worker: 0, task: 0 },
      }),
    )
    expect(rejections).toEqual([])
  })
})

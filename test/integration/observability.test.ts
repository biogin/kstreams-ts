import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, expect, it, vi } from 'vitest'
import {
  type ApplicationState,
  createStreamsApp,
  type Metric,
  type RebalanceEvent,
  type RestoreEndEvent,
  type RestoreProgressEvent,
  type RestoreStartEvent,
  type StreamsApp,
  type StreamsAppErrorEvent,
  serdes,
} from '../../src/index.js'
import type { WorkersCountParams } from './fixtures/workers-count-app.js'
import {
  brokers,
  createKafka,
  describeWithKafka,
  eventually,
  type TestRecord,
  uniqueName,
} from './kafka.js'

const strings = { key: serdes.string, value: serdes.string }

/** Everything a Streams App reported: its state changes, events and metrics. */
interface Observed {
  states: ApplicationState[]
  rebalances: RebalanceEvent[]
  restoreStarts: RestoreStartEvent[]
  restoreProgress: RestoreProgressEvent[]
  restoreEnds: RestoreEndEvent[]
  errors: StreamsAppErrorEvent[]
  metrics: Metric[]
}

/** Listens to every event of `app`. */
function observe(app: StreamsApp, metrics: Metric[]): Observed {
  const observed: Observed = {
    states: [app.state],
    rebalances: [],
    restoreStarts: [],
    restoreProgress: [],
    restoreEnds: [],
    errors: [],
    metrics,
  }
  app
    .on('state', ({ state, previous }) => {
      expect(previous).toBe(observed.states.at(-1))
      observed.states.push(state)
    })
    .on('rebalance', (event) => observed.rebalances.push(event))
    .on('restore-start', (event) => observed.restoreStarts.push(event))
    .on('restore-progress', (event) => observed.restoreProgress.push(event))
    .on('restore-end', (event) => observed.restoreEnds.push(event))
    .on('error', (event) => observed.errors.push(event))
  return observed
}

/** The sum of the counter `name`, over the metrics whose labels match `labels`. */
function total(
  metrics: readonly Metric[],
  name: Metric['name'],
  labels: Record<string, unknown> = {},
): number {
  return withLabels(metrics, name, labels).reduce((sum, metric) => sum + metric.value, 0)
}

function withLabels(
  metrics: readonly Metric[],
  name: Metric['name'],
  labels: Record<string, unknown> = {},
): Metric[] {
  return metrics.filter(
    (metric) =>
      metric.name === name &&
      Object.entries(labels).every(
        ([label, value]) => (metric.labels as Record<string, unknown>)[label] === value,
      ),
  )
}

/** The last value of the gauge `name` with these labels. */
function gauge(metrics: readonly Metric[], name: Metric['name'], labels: Record<string, unknown>) {
  return withLabels(metrics, name, labels).at(-1)?.value
}

describeWithKafka('observability on real Kafka', () => {
  let kafka: ReturnType<typeof createKafka>
  const apps: StreamsApp[] = []
  const directories: string[] = []

  beforeEach(() => {
    kafka = createKafka()
  })

  afterEach(async () => {
    await Promise.allSettled(apps.splice(0).map((app) => app.stop()))
    await kafka.close()
    for (const directory of directories.splice(0)) rmSync(directory, { recursive: true })
  })

  function stateDirectory(): string {
    const directory = mkdtempSync(join(tmpdir(), 'kstreams-observability-'))
    directories.push(directory)
    return directory
  }

  const now = Date.now()

  /** `count` records with the keys `key-0` to `key-9`, spread over `partitions` in turn. */
  function records(count: number, partitions: number): TestRecord[] {
    return Array.from({ length: count }, (_, i) => ({
      key: `key-${i % 10}`,
      value: `value-${i}`,
      timestamp: now + i,
      partition: i % partitions,
    }))
  }

  /** Counts records per key under exactly-once, with SQLite stores in `stateDir`. */
  function countApp(applicationId: string, input: string, output: string, stateDir: string) {
    const metrics: Metric[] = []
    const app = createStreamsApp({
      applicationId,
      brokers,
      guarantee: 'exactly-once',
      stateDir,
      sessionTimeoutMs: 6_000,
      changelogReplicationFactor: 1,
      metrics: { record: (metric) => metrics.push(metric) },
    })
    app
      .stream(input, strings)
      .groupByKey()
      .count({ store: 'counts' })
      .toStream()
      .map((key, count) => ({ key, value: String(count) }))
      .to(output, strings)
    apps.push(app)
    return { app, observed: observe(app, metrics) }
  }

  it('reports states, rebalances, Restores and metrics (exactly-once, workers: 0)', async () => {
    const input = await kafka.createTopic('observability-input', 2)
    const output = await kafka.createTopic('observability-output', 2)
    const applicationId = uniqueName('observability')
    kafka.deleteOnClose(`${applicationId}-counts-changelog`)
    await kafka.produce(input, records(200, 2))

    // First run: the stores start empty.
    const first = countApp(applicationId, input, output, stateDirectory())
    await first.app.start()
    await kafka.consume(output, 200, { timeoutMs: 30_000, settleMs: 0 })
    const { observed } = first
    await eventually(() => total(observed.metrics, 'records-processed') === 200, {
      message: '200 records processed',
    })
    await eventually(() => first.app.state === 'running', { message: 'the running state' })
    // Joining the group, then restoring the new Tasks' (empty) stores, then running.
    expect(observed.states.slice(0, 2)).toEqual(['created', 'rebalancing'])
    expect(observed.states).toContain('restoring')
    expect(observed.rebalances.flatMap((event) => event.assigned)).toEqual(
      expect.arrayContaining([
        { topic: input, partition: 0 },
        { topic: input, partition: 1 },
      ]),
    )
    expect(observed.rebalances.every((event) => event.worker === 0)).toBe(true)
    for (const task of [0, 1]) {
      expect(observed.restoreStarts).toContainEqual({
        worker: 0,
        task,
        store: 'counts',
        full: true,
        remaining: 0,
      })
      expect(observed.restoreEnds).toContainEqual(
        expect.objectContaining({
          worker: 0,
          task,
          store: 'counts',
          restored: 0,
          cancelled: false,
        }),
      )
      // A starting Task waits for its lock, its stable offsets and its Changelog's transactions.
      for (const wait of ['task-lock', 'stable-offsets', 'changelog-transactions']) {
        expect(withLabels(observed.metrics, 'wait-duration', { task, wait })).not.toHaveLength(0)
      }
    }
    expect(withLabels(observed.metrics, 'commit-duration', { worker: 0 })).not.toHaveLength(0)
    expect(withLabels(observed.metrics, 'aborts')).toHaveLength(0)
    // Lag: once everything is committed, nothing is left on either partition.
    await eventually(
      () =>
        [0, 1].every(
          (partition) => gauge(observed.metrics, 'consumer-lag', { topic: input, partition }) === 0,
        ),
      { message: 'no consumer lag' },
    )
    await first.app.stop()
    expect(observed.states.slice(-2)).toEqual(['stopping', 'stopped'])

    // Second run, on a new state directory: every Changelog record is restored.
    const second = countApp(applicationId, input, output, stateDirectory())
    await second.app.start()
    // A Task starts (and restores) when its first record arrives.
    await kafka.produce(input, [
      { key: 'key-0', value: 'more', timestamp: now, partition: 0 },
      { key: 'key-0', value: 'more', timestamp: now, partition: 1 },
    ])
    await eventually(() => second.observed.restoreEnds.length === 2, {
      timeoutMs: 30_000,
      message: 'both Restores to end',
    })
    await eventually(() => second.app.state === 'running', { message: 'the running state' })
    const restored = second.observed
    expect(restored.states).toContain('restoring')
    expect(restored.restoreStarts.every((event) => event.full && event.remaining > 0)).toBe(true)
    expect(restored.restoreEnds.reduce((sum, event) => sum + event.restored, 0)).toBe(200)
    expect(restored.restoreEnds.every((event) => !event.cancelled)).toBe(true)
    expect(total(restored.metrics, 'records-restored')).toBe(200)
    for (const task of [0, 1]) {
      expect(gauge(restored.metrics, 'restore-remaining', { task, store: 'counts' })).toBe(0)
      const progress = restored.restoreProgress.filter((event) => event.task === task)
      expect(progress.at(-1)?.remaining).toBe(0)
    }
    await second.app.stop()
  })

  it('passes worker threads’ states, events and metrics on, and the root cause’s class', async () => {
    const input = await kafka.createTopic('observability-input', 4)
    const output = await kafka.createTopic('observability-output', 4)
    const applicationId = uniqueName('observability-workers')
    const params: WorkersCountParams = {
      applicationId,
      brokers,
      input,
      output,
      workers: 2,
      stateDir: stateDirectory(),
      failOn: 'boom',
      failWithRangeError: true,
      recordMetrics: true,
    }
    process.env.KSTREAMS_APP_PARAMS = JSON.stringify(params)
    // A fresh instance of the App Module, built from these params (see workers.test.ts).
    vi.resetModules()
    const { default: app, recordedMetrics } = await import('./fixtures/workers-count-app.js')
    if (app === undefined) throw new Error('The App Module exports no app')
    const observed = observe(app, recordedMetrics)
    try {
      await app.start()
      kafka.deleteOnClose(`${applicationId}-counts-changelog`)
      await kafka.produce(input, records(100, 4))
      await eventually(() => total(recordedMetrics, 'records-processed') === 100, {
        timeoutMs: 30_000,
        message: 'the Workers’ metrics: 100 records processed',
      })
      await eventually(() => app.state === 'running', { message: 'the running state' })
      expect(observed.states.slice(0, 2)).toEqual(['created', 'rebalancing'])
      expect(new Set(observed.rebalances.map((event) => event.worker))).toEqual(new Set([0, 1]))
      expect(new Set(observed.restoreStarts.map((event) => event.task))).toEqual(
        new Set([0, 1, 2, 3]),
      )
      expect(withLabels(recordedMetrics, 'commit-duration', { worker: 1 })).not.toHaveLength(0)

      // A batch with outputs, then a failing record: its transaction aborts.
      await kafka.produce(input, [
        ...Array.from({ length: 5 }, () => ({ key: 'k', value: 'fine', timestamp: now })),
        { key: 'k', value: 'boom', timestamp: now },
      ])
      await eventually(() => app.state === 'error', { message: 'the error state' })
      expect(observed.errors).toHaveLength(1)
      const [{ error } = { error: undefined }] = observed.errors
      expect(error).toBeInstanceOf(RangeError)
      expect((error as Error).message).toBe('Processor failed on purpose')
      await expect(app.stop()).rejects.toBe(error)
      expect(withLabels(recordedMetrics, 'aborts', { reason: 'error' })).toHaveLength(1)
    } finally {
      await app.stop().catch(() => {})
      delete process.env.KSTREAMS_APP_PARAMS
    }
  })
})

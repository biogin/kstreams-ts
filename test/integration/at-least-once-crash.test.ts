import { afterEach, beforeEach, expect, it } from 'vitest'
import type { CountParams } from './fixtures/count-app.js'
import type { PassThroughParams } from './fixtures/pass-through-app.js'
import { type AppProcess, launchAppModule, randomBetween } from './harness/app-process.js'
import {
  brokers,
  createKafka,
  delay,
  describeWithKafka,
  eventually,
  type TestRecord,
  uniqueName,
} from './kafka.js'

const passThroughApp = new URL('./fixtures/pass-through-app.ts', import.meta.url)
const countApp = new URL('./fixtures/count-app.ts', import.meta.url)

describeWithKafka('at-least-once survives SIGKILL (Streams App in a child process)', () => {
  let kafka: ReturnType<typeof createKafka>
  const processes: AppProcess[] = []
  const producers: { stop(): Promise<number> }[] = []

  beforeEach(() => {
    kafka = createKafka()
  })

  afterEach(async () => {
    await Promise.allSettled(producers.splice(0).map((producer) => producer.stop()))
    await Promise.allSettled(processes.splice(0).map((app) => app.kill()))
    await kafka.close()
  })

  async function launch(
    params: PassThroughParams,
    appModule: URL = passThroughApp,
  ): Promise<AppProcess> {
    const app = await launchAppModule(appModule, params, { timeoutMs: 60_000 })
    processes.push(app)
    return app
  }

  function records(count: number, from = 0): TestRecord[] {
    return Array.from({ length: count }, (_, i) => ({
      key: `key-${(from + i) % 13}`,
      value: `value-${from + i}`,
      timestamp: 1_700_000_000_000 + from + i,
    }))
  }

  it('runs an App Module in a child process and stops it gracefully on SIGTERM', async () => {
    const input = await kafka.createTopic('input')
    const output = await kafka.createTopic('output')
    const app = await launch({ applicationId: uniqueName('child'), brokers, input, output })

    await kafka.produce(input, records(10))
    expect(await kafka.consume(output, 10)).toHaveLength(10)

    expect(await app.stop()).toEqual({ code: 0, signal: null })
  })

  it('reports a child that fails to start', async () => {
    await expect(
      launchAppModule(passThroughApp, {
        applicationId: uniqueName('child'),
        brokers: [],
        input: 'unused',
        output: 'unused',
      }),
    ).rejects.toThrow(/exited before running/)
  })

  /**
   * Waits until this incarnation of the app is processing: it has committed input offsets since
   * the call. A SIGKILLed Worker's partitions move to the restarted one only once its session
   * times out, so a fixed wait could kill the app before it got any partition.
   */
  async function processing(applicationId: string, input: string, app: AppProcess, kill: number) {
    const before = await kafka.consumed(applicationId, input)
    await eventually(async () => (await kafka.consumed(applicationId, input)) > before, {
      timeoutMs: 60_000,
      message: `progress before kill ${kill + 1}; app output:\n${app.log()}`,
    })
  }

  /**
   * Produces at least `minimum` records to `input`, in chunks of 100, and goes on until `stop()`,
   * so that every kill lands mid-stream however long the restarts take. Resolves `stop()` with
   * the number of records sent.
   */
  function keepProducing(input: string, minimum: number) {
    let killing = true
    let sent = 0
    const producing = (async () => {
      while (killing || sent < minimum) {
        await kafka.produce(input, records(100, sent))
        sent += 100
        await delay(40)
      }
    })()
    const producer = {
      async stop(): Promise<number> {
        killing = false
        await producing
        return sent
      },
    }
    producers.push(producer)
    return producer
  }

  /** Waits until the app has committed the offsets of all `total` input records. */
  async function consumedAll(applicationId: string, input: string, total: number, app: AppProcess) {
    await eventually(async () => (await kafka.consumed(applicationId, input)) >= BigInt(total), {
      timeoutMs: 90_000,
      message: `every input record to be consumed; app output:\n${app.log()}`,
    })
  }

  it('loses no record when the Worker is SIGKILLed mid-stream and restarted', async () => {
    const input = await kafka.createTopic('input', 3)
    const output = await kafka.createTopic('output', 3)
    const params = { applicationId: uniqueName('sigkill'), brokers, input, output }
    const kills = 3

    // Keep producing while the app is killed and restarted, so every kill lands mid-stream.
    const producer = keepProducing(input, 3_000)

    let app = await launch(params)
    for (let kill = 0; kill < kills; kill++) {
      await processing(params.applicationId, input, app, kill)
      await delay(randomBetween(0, 600))
      const exit = await app.kill()
      expect(exit.signal).toBe('SIGKILL')
      app = await launch(params)
    }
    const total = await producer.stop()
    await consumedAll(params.applicationId, input, total, app)

    // Duplicates are allowed under at-least-once; nothing may be missing.
    const missing = new Set(records(total).map((record) => record.value))
    const received = await kafka
      .consumeUntil(
        output,
        (record) => {
          missing.delete(record.value)
          return missing.size === 0
        },
        { timeoutMs: 30_000 },
      )
      .catch((error: Error) => {
        throw new Error(`${error.message}; ${missing.size} input records never arrived`)
      })
    expect(missing.size).toBe(0)
    expect(new Set(received.map((record) => record.value)).size).toBe(total)
    expect(received.length).toBeGreaterThanOrEqual(total)
  }, 120_000)

  it('count() survives SIGKILL: Restore brings back every count, over-counting only uncommitted input', async () => {
    const input = await kafka.createTopic('input', 3)
    const output = await kafka.createTopic('output', 3)
    const applicationId = uniqueName('sigkill-count')
    kafka.deleteOnClose(`${applicationId}-counts-changelog`)
    const commitMaxRecords = 100
    const params: CountParams = { applicationId, brokers, input, output, commitMaxRecords }
    const kills = 3

    const producer = keepProducing(input, 3_000)

    let app = await launch(params, countApp)
    for (let kill = 0; kill < kills; kill++) {
      await processing(applicationId, input, app, kill)
      await delay(randomBetween(0, 600))
      const exit = await app.kill()
      expect(exit.signal).toBe('SIGKILL')
      app = await launch(params, countApp)
    }
    const total = await producer.stop()
    await consumedAll(applicationId, input, total, app)

    const expected = new Map<string, number>()
    for (const { key } of records(total))
      expected.set(key as string, (expected.get(key as string) ?? 0) + 1)
    // The highest count seen per key: counts only grow, also across Restores.
    const counted = new Map<string, number>()
    const reached = () => [...expected].every(([key, count]) => (counted.get(key) ?? 0) >= count)
    await kafka
      .consumeUntil(
        output,
        ({ key, value }) => {
          const count = Number(value)
          if (count > (counted.get(key as string) ?? 0)) counted.set(key as string, count)
          return reached()
        },
        { timeoutMs: 60_000 },
      )
      .catch((error: Error) => {
        throw new Error(
          `${error.message}; ${total} input records; counts ${JSON.stringify([...counted])}; expected ${JSON.stringify([...expected])}`,
        )
      })

    // The precise at-least-once assertion. No count is lost: every key's count reaches its number
    // of input records (without Restore, the restarted in-memory stores would start from 0). A
    // count can only exceed it by input records counted again after a crash: ones whose count
    // reached the Changelog but whose offsets were not committed yet. That is at most the one
    // uncommitted batch per kill, i.e. `commitMaxRecords` records per kill over all keys.
    let overCounted = 0
    for (const [key, count] of expected) {
      const final = counted.get(key) ?? 0
      expect(final).toBeGreaterThanOrEqual(count)
      overCounted += final - count
    }
    expect(overCounted).toBeLessThanOrEqual(kills * commitMaxRecords)
  }, 180_000)
})

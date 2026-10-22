import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, expect, it } from 'vitest'
import { createStreamsApp, type StreamsApp, serdes, stores } from '../../src/index.js'
import {
  readCheckpoint,
  readCommitted as readCommittedRecords,
  storedEntries,
  taskDirectory,
} from '../support/internals.js'
import type { CountParams } from './fixtures/count-app.js'
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

const countApp = new URL('./fixtures/count-app.ts', import.meta.url)
const strings = { key: serdes.string, value: serdes.string }

describeWithKafka('exactly-once with State Stores (SQLite, workers: 0)', () => {
  let kafka: ReturnType<typeof createKafka>
  const apps: StreamsApp[] = []
  const processes: AppProcess[] = []
  const directories: string[] = []

  beforeEach(() => {
    kafka = createKafka()
  })

  afterEach(async () => {
    await Promise.allSettled([
      ...apps.splice(0).map((app) => app.stop()),
      ...processes.splice(0).map((app) => app.kill()),
    ])
    await kafka.close()
    for (const directory of directories.splice(0)) rmSync(directory, { recursive: true })
  })

  function stateDirectory(): string {
    const directory = mkdtempSync(join(tmpdir(), 'kstreams-eos-'))
    directories.push(directory)
    return directory
  }

  /** Every committed record of `topic` (read_committed, aborted transactions skipped). */
  function readCommitted(topic: string, partitions: number) {
    return readCommittedRecords(brokers, uniqueName('test-reader'), topic, partitions)
  }

  it('a Kafka abort rolls back the SQLite transaction; the Checkpoint stays usable', async () => {
    const input = await kafka.createTopic('input', 1)
    const applicationId = uniqueName('eos-abort')
    const changelog = `${applicationId}-latest-changelog`
    kafka.deleteOnClose(changelog)
    const stateDir = stateDirectory()
    const app = createStreamsApp({
      applicationId,
      brokers,
      guarantee: 'exactly-once',
      stateDir,
      sessionTimeoutMs: 6_000,
      changelogReplicationFactor: 1,
    })
    const latest = app.store('latest', strings, { supplier: stores.sqlite() })
    const keeper = app.processor<string, string | null, never, never>('keeper', (ctx) => ({
      process({ key, value }) {
        const store = ctx.store(latest)
        store.put(key, value ?? '')
        if (value === 'poison') {
          store.put('extra', 'written by the failing record')
          throw new Error('Processor failed on purpose')
        }
      },
    }))
    app.stream(input, strings).process(keeper)
    apps.push(app)
    await app.start()

    const now = Date.now()
    await kafka.produce(input, [
      { key: 'a', value: '1', timestamp: now },
      { key: 'b', value: '1', timestamp: now + 1 },
    ])
    // The first batch is committed: its offsets, then its store transaction and Checkpoint.
    await eventually(
      async () => (await kafka.committedOffsets(applicationId, input)).get(0) === 2n,
      { timeoutMs: 30_000, message: 'the first batch to commit' },
    )
    await kafka.produce(input, [{ key: 'a', value: 'poison', timestamp: now + 2 }])
    await eventually(() => app.state === 'error', {
      timeoutMs: 30_000,
      message: 'the app to fail',
    })
    await app.stop().catch(() => {})

    // The failing batch's writes (to an existing key and a new one) are rolled back.
    expect(storedEntries(stateDir, applicationId, 0, 'latest', serdes.string)).toEqual([
      ['a', '1'],
      ['b', '1'],
    ])
    // Its Changelog records were aborted with the Kafka transaction.
    expect(await readCommitted(changelog, 1)).toEqual([
      { key: 'a', value: '1', partition: 0 },
      { key: 'b', value: '1', partition: 0 },
    ])
    // The Checkpoint of the first batch is still there: a restart restores only after it.
    expect(readCheckpoint(taskDirectory(stateDir, applicationId, 0))).toEqual(
      new Map([['latest', 2n]]),
    )
    expect((await kafka.committedOffsets(applicationId, input)).get(0)).toBe(2n)
  }, 90_000)

  it('count() is exact after several SIGKILLs and restarts: outputs, offsets and SQLite stores', async () => {
    const partitions = 3
    const input = await kafka.createTopic('input', partitions)
    const output = await kafka.createTopic('output', partitions)
    const applicationId = uniqueName('eos-sigkill')
    kafka.deleteOnClose(`${applicationId}-counts-changelog`)
    const stateDir = stateDirectory()
    const params: CountParams = {
      applicationId,
      brokers,
      input,
      output,
      commitMaxRecords: 100,
      guarantee: 'exactly-once',
      stateDir,
    }
    const kills = 4
    const keys = 13
    const start = Date.now()
    const sent: TestRecord[] = []

    // Keeps producing until every kill has happened, so each one lands mid-stream.
    let killing = true
    const producing = (async () => {
      while (killing || sent.length < 2_000) {
        const chunk = Array.from({ length: 20 }, (_, i) => {
          const n = sent.length + i
          return { key: `key-${n % keys}`, value: `value-${n}`, timestamp: start + n }
        })
        await kafka.produce(input, chunk)
        sent.push(...chunk)
        await delay(100)
      }
    })()

    const consumed = () => kafka.consumed(applicationId, input)
    const launch = async () => {
      const app = await launchAppModule(countApp, params, { timeoutMs: 60_000 })
      processes.push(app)
      return app
    }

    try {
      let app = await launch()
      for (let kill = 0; kill < kills; kill++) {
        // Kill only once this incarnation is processing: it has committed a transaction.
        const before = await consumed()
        await eventually(async () => (await consumed()) > before, {
          timeoutMs: 60_000,
          message: `progress before kill ${kill + 1}; app output:\n${app.log()}`,
        })
        await delay(randomBetween(0, 600))
        const exit = await app.kill()
        expect(exit.signal).toBe('SIGKILL')
        app = await launch()
      }
      killing = false
      await producing

      const expected = new Map<string, number>()
      for (const { key } of sent)
        expected.set(key as string, (expected.get(key as string) ?? 0) + 1)

      // Wait until the last incarnation has consumed every input record.
      await eventually(async () => (await consumed()) === BigInt(sent.length), {
        timeoutMs: 90_000,
        message: `every input record to be consumed; app output:\n${app.log()}`,
      })

      // Exactly once: each key's committed counts are 1, 2, ... n, in order, with no gap and no
      // duplicate, although a crash aborted a transaction (and rolled back SQLite) each time.
      const counts = new Map<string, number[]>()
      for (const { key, value } of await readCommitted(output, partitions)) {
        counts.set(key, [...(counts.get(key) ?? []), Number(value)])
      }
      expect(new Set(counts.keys())).toEqual(new Set(expected.keys()))
      for (const [key, n] of expected) {
        expect(counts.get(key), key).toEqual(Array.from({ length: n }, (_, i) => i + 1))
      }

      // A graceful stop commits and checkpoints; the SQLite stores hold the exact counts.
      expect(await app.stop()).toEqual({ code: 0, signal: null })
      const stored = new Map<string, number>()
      for (let task = 0; task < partitions; task++) {
        for (const [key, count] of storedEntries(
          stateDir,
          applicationId,
          task,
          'counts',
          serdes.number,
        )) {
          stored.set(key, count)
        }
      }
      expect(stored).toEqual(expected)
    } finally {
      killing = false
      await producing
    }
  }, 300_000)
})

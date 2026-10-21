// Revoke and graceful shutdown (ADR-0006): a rebalance that takes Tasks away mid-batch, and a
// stop that commits the current batch or, when the shutdown timeout runs out, aborts it.

import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { Consumer, FetchIsolationLevels } from '@platformatic/kafka'
import { afterEach, beforeEach, expect, it } from 'vitest'
import {
  createStreamsApp,
  type DeliveryGuarantee,
  type StreamsApp,
  serdes,
  stores,
} from '../../src/index.js'
import { createChangelogReader } from '../../src/restore.js'
import { keyValueStore, taskDirectory } from '../../src/state-store.js'
import {
  brokers,
  createKafka,
  delay,
  describeWithKafka,
  eventually,
  type TestRecord,
  uniqueName,
} from './kafka.js'

const strings = { key: serdes.string, value: serdes.string }

describeWithKafka('revoke and graceful shutdown (workers: 0)', () => {
  let kafka: ReturnType<typeof createKafka>
  const apps: StreamsApp[] = []
  const consumers: Consumer[] = []
  const directories: string[] = []

  beforeEach(() => {
    kafka = createKafka()
  })

  afterEach(async () => {
    await Promise.allSettled([
      ...apps.splice(0).map((app) => app.stop()),
      ...consumers.splice(0).map((consumer) => consumer.close(true)),
    ])
    await kafka.close()
    for (const directory of directories.splice(0)) rmSync(directory, { recursive: true })
  })

  function stateDirectory(): string {
    const directory = mkdtempSync(join(tmpdir(), 'kstreams-revoke-'))
    directories.push(directory)
    return directory
  }

  /** Counts `input` records per key in SQLite and writes each new count to `output`. */
  function countApp(options: {
    applicationId: string
    input: string
    output: string
    guarantee: DeliveryGuarantee
    stateDir: string
    commitIntervalMs?: number
    shutdownTimeoutMs?: number
  }): StreamsApp {
    const { input, output, ...config } = options
    const app = createStreamsApp({
      brokers,
      sessionTimeoutMs: 6_000,
      changelogReplicationFactor: 1,
      ...config,
    })
    app
      .stream(input, strings)
      .groupByKey()
      .count({ store: 'counts', storeSupplier: stores.sqlite() })
      .toStream()
      .map((key, count) => ({ key, value: String(count) }))
      .to(output, strings)
    apps.push(app)
    return app
  }

  /** Every committed record of `topic` (read_committed, aborted transactions skipped). */
  async function readCommitted(
    topic: string,
    partitions: number,
  ): Promise<{ key: string; value: string | null }[]> {
    const reader = createChangelogReader({ brokers, clientId: uniqueName('test-reader') })
    const records: { key: string; value: string | null }[] = []
    try {
      for (let partition = 0; partition < partitions; partition++) {
        await reader.read(topic, partition, (chunk) => {
          for (const { key, value } of chunk) {
            records.push({
              key: Buffer.from(key ?? new Uint8Array()).toString(),
              value: value === null ? null : Buffer.from(value).toString(),
            })
          }
        })
      }
    } finally {
      await reader.close()
    }
    return records
  }

  /** The counts in the SQLite stores of `tasks`, as the app left them on disk. */
  function storedCounts(stateDir: string, applicationId: string, tasks: number) {
    const counts = new Map<string, number>()
    const countSerdes = { key: serdes.string, value: serdes.number }
    for (let task = 0; task < tasks; task++) {
      const directory = taskDirectory(stateDir, applicationId, task)
      const store = stores.sqlite()({ name: 'counts', applicationId, task, directory })
      try {
        const entries = keyValueStore(store, countSerdes, { topic: 'counts' }).all()
        for (const [key, count] of entries) counts.set(key, count)
      } finally {
        store.close()
      }
    }
    return counts
  }

  /** The sum of the committed offsets of `input` for the Application ID. */
  async function consumed(applicationId: string, input: string): Promise<bigint> {
    const committed = await kafka.committedOffsets(applicationId, input)
    return [...committed.values()].reduce((sum, offset) => sum + offset, 0n)
  }

  /** Whether some partition of `topic` has an open transaction (its LSO is behind its end). */
  async function openTransaction(topic: string): Promise<boolean> {
    const consumer = new Consumer({
      clientId: uniqueName('test-offsets'),
      groupId: uniqueName('test-offsets'),
      bootstrapBrokers: brokers,
    })
    try {
      const [stable, end] = await Promise.all(
        [FetchIsolationLevels.READ_COMMITTED, FetchIsolationLevels.READ_UNCOMMITTED].map(
          async (isolationLevel) =>
            (await consumer.listOffsets({ topics: [topic], isolationLevel })).get(topic) ?? [],
        ),
      )
      return (stable ?? []).some((offset, partition) => offset !== end?.[partition])
    } finally {
      await consumer.close(true)
    }
  }

  /** Produces records with keys `key-0` ... `key-<keys-1>` until `stop()`; `sent` grows meanwhile. */
  function keepProducing(input: string, keys: number) {
    const sent: TestRecord[] = []
    const start = Date.now()
    let producing = true
    const done = (async () => {
      while (producing) {
        const chunk = Array.from({ length: keys }, (_, i) => {
          const n = sent.length + i
          return { key: `key-${n % keys}`, value: `value-${n}`, timestamp: start + n }
        })
        await kafka.produce(input, chunk)
        sent.push(...chunk)
        await delay(50)
      }
    })()
    return {
      sent,
      async stop(): Promise<void> {
        producing = false
        await done
      },
      expected(): Map<string, number> {
        const counts = new Map<string, number>()
        for (const { key } of sent) counts.set(key as string, (counts.get(key as string) ?? 0) + 1)
        return counts
      },
    }
  }

  it('exactly-once: a revoke mid-batch aborts the transaction, rolls back SQLite and loses nothing', async () => {
    const partitions = 3
    const input = await kafka.createTopic('input', partitions)
    const output = await kafka.createTopic('output', partitions)
    const applicationId = uniqueName('eos-revoke')
    kafka.deleteOnClose(`${applicationId}-counts-changelog`)
    const stateDir = stateDirectory()
    // Long batches, so the rebalance lands in the middle of one.
    const app = countApp({
      applicationId,
      input,
      output,
      guarantee: 'exactly-once',
      stateDir,
      commitIntervalMs: 3_000,
    })
    await app.start()
    const producer = keepProducing(input, 9)
    try {
      await eventually(async () => (await consumed(applicationId, input)) > 0n, {
        timeoutMs: 30_000,
        message: 'the first batch to commit',
      })

      // Another member joins the group: the eager rebalance revokes every Task of the app, and
      // the newcomer keeps some of them (it consumes nothing).
      const intruder = new Consumer({
        clientId: uniqueName('intruder'),
        groupId: applicationId,
        bootstrapBrokers: brokers,
        autocommit: false,
        sessionTimeout: 6_000,
        heartbeatInterval: 1_000,
      })
      consumers.push(intruder)
      const stream = await intruder.consume({
        topics: [input],
        mode: 'committed',
        fallbackMode: 'earliest',
        autocommit: false,
      })
      await eventually(() => (intruder.assignments?.[0]?.partitions.length ?? 0) > 0, {
        timeoutMs: 30_000,
        message: 'the intruder to own a partition',
      })
      // The app keeps committing its remaining Tasks.
      let before = await consumed(applicationId, input)
      await eventually(async () => (await consumed(applicationId, input)) > before, {
        timeoutMs: 30_000,
        message: 'progress while the intruder owns some partitions',
      })
      expect(app.state).toBe('running')

      // The intruder leaves: its partitions come back to the app (new Tasks, restored).
      await stream.close()
      await intruder.close()
      before = await consumed(applicationId, input)
      await eventually(async () => (await consumed(applicationId, input)) > before + 50n, {
        timeoutMs: 30_000,
        message: 'progress after the intruder left',
      })
    } finally {
      await producer.stop()
    }
    const total = BigInt(producer.sent.length)
    await eventually(async () => (await consumed(applicationId, input)) === total, {
      timeoutMs: 60_000,
      message: 'every input record to be consumed',
    })
    expect(app.state).toBe('running')
    await app.stop()

    // Exactly once: each key's committed counts are 1, 2, ... n.
    const expected = producer.expected()
    const counts = new Map<string, number[]>()
    const committed = await readCommitted(output, partitions)
    for (const { key, value } of committed) {
      counts.set(key, [...(counts.get(key) ?? []), Number(value)])
    }
    for (const [key, n] of expected) {
      expect(counts.get(key), key).toEqual(Array.from({ length: n }, (_, i) => i + 1))
    }
    // The revoked batch was aborted: read_uncommitted sees its outputs on top of the committed ones.
    await kafka.consume(output, committed.length + 1, {
      uncommitted: true,
      timeoutMs: 30_000,
      settleMs: 0,
    })
    // SQLite kept nothing of the aborted batch.
    expect(storedCounts(stateDir, applicationId, partitions)).toEqual(expected)
  }, 240_000)

  it('at-least-once: Tasks move between two instances and back without losing an update', async () => {
    const partitions = 3
    const input = await kafka.createTopic('input', partitions)
    const output = await kafka.createTopic('output', partitions)
    const applicationId = uniqueName('alos-rebalance')
    kafka.deleteOnClose(`${applicationId}-counts-changelog`)
    const first = stateDirectory()
    const options = { applicationId, input, output, guarantee: 'at-least-once' as const }
    const a = countApp({ ...options, stateDir: first })
    await a.start()
    const producer = keepProducing(input, 9)
    try {
      await eventually(async () => (await consumed(applicationId, input)) > 50n, {
        timeoutMs: 30_000,
        message: 'progress of the first instance',
      })
      // A second instance takes some Tasks, processes them, then hands them back on stop.
      const b = countApp({ ...options, stateDir: stateDirectory() })
      await b.start()
      let before = await consumed(applicationId, input)
      await eventually(async () => (await consumed(applicationId, input)) > before + 100n, {
        timeoutMs: 30_000,
        message: 'progress with two instances',
      })
      await b.stop()
      before = await consumed(applicationId, input)
      await eventually(async () => (await consumed(applicationId, input)) > before + 100n, {
        timeoutMs: 30_000,
        message: 'progress after the second instance stopped',
      })
    } finally {
      await producer.stop()
    }
    const total = BigInt(producer.sent.length)
    await eventually(async () => (await consumed(applicationId, input)) === total, {
      timeoutMs: 60_000,
      message: 'every input record to be consumed',
    })
    expect(a.state).toBe('running')
    await a.stop()

    // At least once: a Task that came back was restored from the Changelog, so no update the
    // other instance made is lost; each key's count reaches at least its number of records.
    const expected = producer.expected()
    const stored = storedCounts(first, applicationId, partitions)
    for (const [key, n] of expected) {
      expect(stored.get(key), key).toBeGreaterThanOrEqual(n)
    }
  }, 240_000)

  it('a graceful stop commits the current batch without waiting for the commit interval', async () => {
    const input = await kafka.createTopic('input', 1)
    const output = await kafka.createTopic('output', 1)
    const applicationId = uniqueName('eos-stop')
    kafka.deleteOnClose(`${applicationId}-counts-changelog`)
    const stateDir = stateDirectory()
    const app = countApp({
      applicationId,
      input,
      output,
      guarantee: 'exactly-once',
      stateDir,
      commitIntervalMs: 120_000,
    })
    await app.start()
    const now = Date.now()
    const sent = Array.from({ length: 20 }, (_, n) => ({
      key: `key-${n % 4}`,
      value: `value-${n}`,
      timestamp: now + n,
    }))
    await kafka.produce(input, sent)
    // Processed into the open transaction, not committed: the batch lasts two minutes.
    await kafka.consume(output, sent.length, { uncommitted: true, timeoutMs: 30_000, settleMs: 0 })
    expect(await consumed(applicationId, input)).toBe(0n)

    const started = Date.now()
    await app.stop()
    expect(Date.now() - started).toBeLessThan(30_000)
    expect(app.state).toBe('stopped')

    expect(await readCommitted(output, 1)).toHaveLength(sent.length)
    expect(await consumed(applicationId, input)).toBe(BigInt(sent.length))
    expect(storedCounts(stateDir, applicationId, 1)).toEqual(
      new Map([0, 1, 2, 3].map((k) => [`key-${k}`, 5])),
    )
    expect(await kafka.groupMembers(applicationId)).toBe(0)
  }, 120_000)

  it('when the shutdown timeout runs out, a stop aborts the batch and still leaves the group', async () => {
    const input = await kafka.createTopic('input', 1)
    const output = await kafka.createTopic('output', 1)
    const applicationId = uniqueName('eos-stop-timeout')
    let release: () => void = () => {}
    const hanging = new Promise<void>((resolve) => {
      release = resolve
    })
    let reachedHang: () => void = () => {}
    const reached = new Promise<void>((resolve) => {
      reachedHang = resolve
    })

    function hangingApp(hang: boolean): StreamsApp {
      const app = createStreamsApp({
        applicationId,
        brokers,
        guarantee: 'exactly-once',
        // Long: a member that did not leave the group would still be listed for the assertions.
        sessionTimeoutMs: 45_000,
        commitIntervalMs: 120_000,
        shutdownTimeoutMs: 1_000,
      })
      const slow = app.processor<string, string | null, string, string | null>('slow', (ctx) => ({
        async process(record) {
          if (hang && record.value === 'hang') {
            reachedHang()
            await hanging
          }
          ctx.forward(record)
        },
      }))
      app.stream(input, strings).process(slow).to(output, strings)
      apps.push(app)
      return app
    }

    const app = hangingApp(true)
    await app.start()
    const now = Date.now()
    const sent = ['a', 'b', 'hang'].map((value, n) => ({ key: 'k', value, timestamp: now + n }))
    await kafka.produce(input, sent)
    await kafka.consume(output, 2, { uncommitted: true, timeoutMs: 30_000, settleMs: 0 })
    await reached

    const started = Date.now()
    await app.stop()
    expect(Date.now() - started).toBeLessThan(15_000)
    expect(app.state).toBe('stopped')
    // The batch was aborted, not left open: the output's LSO caught up with its end.
    expect(await openTransaction(output)).toBe(false)
    expect(await readCommitted(output, 1)).toEqual([])
    expect((await kafka.committedOffsets(applicationId, input)).get(0) ?? -1n).toBe(-1n)
    // It left the group instead of waiting for the session to time out.
    await eventually(async () => (await kafka.groupMembers(applicationId)) === 0, {
      timeoutMs: 10_000,
      message: 'the app to leave the group',
    })
    release()

    // A restart processes the whole batch, once.
    const restarted = hangingApp(false)
    await restarted.start()
    // Processed into its open transaction (on top of the aborted batch's two outputs).
    await kafka.consume(output, 5, { uncommitted: true, timeoutMs: 30_000, settleMs: 0 })
    await restarted.stop()
    expect((await readCommitted(output, 1)).map(({ value }) => value)).toEqual(['a', 'b', 'hang'])
    expect(await consumed(applicationId, input)).toBe(3n)
  }, 120_000)
})

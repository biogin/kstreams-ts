// Restore without blocking other Tasks (ADR-0009): a Task whose State Stores are restoring holds
// its records in memory while the Worker's other Tasks keep processing and committing.
//
// The restoring Task's Restore is held open by a "gate": a transaction of another producer, left
// open on the Task's Changelog partition. Restore waits for every transaction open on its
// partition to end (restore.ts), so it lasts until the test ends the gate's transaction.

import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { Producer } from '@platformatic/kafka'
import { afterEach, beforeEach, expect, it } from 'vitest'
import {
  createStreamsApp,
  type DeliveryGuarantee,
  type StreamsApp,
  type StreamsAppConfig,
  serdes,
  stores,
} from '../../src/index.js'
import { createChangelogReader } from '../../src/restore.js'
import { keyValueStore, taskDirectory } from '../../src/state-store.js'
import {
  brokers,
  createKafka,
  describeWithKafka,
  eventually,
  type TestRecord,
  uniqueName,
} from './kafka.js'

const strings = { key: serdes.string, value: serdes.string }
/** The Task whose Restore is held open; Task 0 must keep going meanwhile. */
const RESTORING = 1
/** Records in the restoring Task's Changelog: a large store. */
const LARGE_STORE = 20_000

describeWithKafka('Restore without blocking other Tasks (SQLite, workers: 0)', () => {
  let kafka: ReturnType<typeof createKafka>
  const apps: StreamsApp[] = []
  const producers: Producer[] = []
  const directories: string[] = []

  beforeEach(() => {
    kafka = createKafka()
  })

  afterEach(async () => {
    await Promise.allSettled(apps.splice(0).map((app) => app.stop()))
    await Promise.allSettled(producers.splice(0).map((producer) => producer.close(true)))
    await kafka.close()
    for (const directory of directories.splice(0)) rmSync(directory, { recursive: true })
  })

  function stateDirectory(): string {
    const directory = mkdtempSync(join(tmpdir(), 'kstreams-non-blocking-'))
    directories.push(directory)
    return directory
  }

  /** `count()` of `input` by key into SQLite store "counts", written to `output`. */
  function counter(
    config: Partial<StreamsAppConfig> & { applicationId: string },
    input: string,
    output: string,
  ): StreamsApp {
    const app = createStreamsApp({
      brokers,
      sessionTimeoutMs: 6_000,
      changelogReplicationFactor: 1,
      ...config,
    })
    app
      .stream(input, strings)
      .groupByKey()
      .count({ store: 'counts' })
      .toStream()
      .to(output, { key: serdes.string, value: serdes.number })
    apps.push(app)
    return app
  }

  function records(key: string, partition: number, count: number): TestRecord[] {
    return Array.from({ length: count }, (_, i) => ({
      key,
      value: `${i}`,
      timestamp: Date.now(),
      partition,
    }))
  }

  /** The committed counts written to `output`, by key, in order (read_committed). */
  async function committedCounts(output: string): Promise<Map<string, number[]>> {
    const reader = createChangelogReader({ brokers, clientId: uniqueName('test-reader') })
    const counts = new Map<string, number[]>()
    try {
      for (const partition of [0, 1]) {
        await reader.read(output, partition, (chunk) => {
          for (const { key, value } of chunk) {
            const name = Buffer.from(key ?? new Uint8Array()).toString()
            const count = serdes.number.deserialize(value ?? new Uint8Array(), { topic: output })
            counts.set(name, [...(counts.get(name) ?? []), count])
          }
        })
      }
    } finally {
      await reader.close()
    }
    return counts
  }

  async function waitForCounts(output: string, key: string, last: number): Promise<void> {
    await eventually(async () => (await committedCounts(output)).get(key)?.at(-1) === last, {
      timeoutMs: 60_000,
      message: `the committed count of "${key}" to reach ${last}`,
    })
  }

  /**
   * A first run that creates the Changelog and counts "a" (Task 0) and "b" (Task 1) once each; then
   * a large store for Task 1, written to its Changelog partition, and an open transaction there
   * (the gate) that holds a later Restore of Task 1 open.
   */
  async function prepare(guarantee: DeliveryGuarantee) {
    const input = await kafka.createTopic('input', 2)
    const output = await kafka.createTopic('output', 2)
    const applicationId = uniqueName(`non-blocking-${guarantee}`)
    const changelog = `${applicationId}-counts-changelog`
    kafka.deleteOnClose(changelog)

    const first = counter({ applicationId, guarantee, stateDir: stateDirectory() }, input, output)
    await first.start()
    await kafka.produce(input, [...records('a', 0, 1), ...records('b', RESTORING, 1)])
    await waitForCounts(output, 'a', 1)
    await waitForCounts(output, 'b', 1)
    await first.stop()

    const one = Buffer.from(serdes.number.serialize(1, { topic: changelog }))
    const large = Array.from({ length: LARGE_STORE }, (_, i) => ({
      topic: changelog,
      key: Buffer.from(`big-${i}`),
      value: one,
      partition: RESTORING,
    }))
    const writer = new Producer({
      clientId: uniqueName('large-store'),
      bootstrapBrokers: brokers,
      idempotent: true,
    })
    producers.push(writer)
    for (let at = 0; at < large.length; at += 2_000) {
      await writer.send({ messages: large.slice(at, at + 2_000) })
    }

    const gate = new Producer({
      clientId: uniqueName('gate'),
      bootstrapBrokers: brokers,
      idempotent: true,
      transactionalId: uniqueName('gate'),
      producerId: -1n,
      producerEpoch: -1,
      timeout: 120_000,
    })
    producers.push(gate)
    const transaction = await gate.beginTransaction()
    await transaction.send({
      messages: [{ topic: changelog, key: Buffer.from('gate'), value: one, partition: RESTORING }],
    })
    return { input, output, applicationId, open: transaction }
  }

  it('keeps processing and committing another Task while a large store restores (exactly-once)', async () => {
    const { input, output, applicationId, open } = await prepare('exactly-once')
    const stateDir = stateDirectory()
    // A full Restore (empty state directory), with room to hold 10 records of the restoring Task:
    // more come, so it drops the rest and reads them again once restored.
    const app = counter(
      { applicationId, guarantee: 'exactly-once', stateDir, commitMaxRecords: 10 },
      input,
      output,
    )
    await app.start()
    await kafka.produce(input, records('b', RESTORING, 30))

    // Task 0 makes progress, committed batch by batch, while Task 1 restores.
    for (let i = 0; i < 20; i++) {
      await kafka.produce(input, records('a', 0, 1))
      await waitForCounts(output, 'a', 2 + i)
    }
    expect((await committedCounts(output)).get('b')).toEqual([1])
    expect(app.state).toBe('running')

    await open.commit()
    await waitForCounts(output, 'b', 31)
    await app.stop()

    const counts = await committedCounts(output)
    expect(counts.get('a')).toEqual(Array.from({ length: 21 }, (_, i) => i + 1))
    expect(counts.get('b')).toEqual(Array.from({ length: 31 }, (_, i) => i + 1))
    const directory = taskDirectory(stateDir, applicationId, RESTORING)
    const local = stores.sqlite()({ name: 'counts', applicationId, task: RESTORING, directory })
    try {
      const entries = keyValueStore(
        local,
        { key: serdes.string, value: serdes.number },
        {
          topic: 'unused',
        },
      ).all()
      expect(entries.length).toBe(LARGE_STORE + 2)
      expect(new Map(entries).get('b')).toBe(31)
    } finally {
      local.close()
    }
  }, 180_000)

  it('stops during a Restore without waiting for it, well within the shutdown timeout', async () => {
    const { input, output, applicationId } = await prepare('at-least-once')
    const app = counter(
      { applicationId, stateDir: stateDirectory(), shutdownTimeoutMs: 30_000 },
      input,
      output,
    )
    await app.start()
    await kafka.produce(input, [...records('b', RESTORING, 1), ...records('a', 0, 1)])
    await waitForCounts(output, 'a', 2)

    const started = Date.now()
    await app.stop()
    expect(Date.now() - started).toBeLessThan(10_000)
    expect(app.state).toBe('stopped')
    expect(await kafka.groupMembers(applicationId)).toBe(0)
    // Task 1 never processed its record, so its offset wasn't committed.
    expect((await kafka.committedOffsets(applicationId, input)).get(RESTORING)).toBe(1n)
  }, 180_000)

  it('a Task revoked during its Restore is given up and restored by its new owner (exactly-once)', async () => {
    const { input, output, applicationId, open } = await prepare('exactly-once')
    // One state directory for both App Processes: the new owner waits for the Task's lock.
    const stateDir = stateDirectory()
    const config = { applicationId, guarantee: 'exactly-once' as const, stateDir }
    const first = counter(config, input, output)
    await first.start()
    await kafka.produce(input, records('b', RESTORING, 5))
    await kafka.produce(input, records('a', 0, 1))
    await waitForCounts(output, 'a', 2)

    // The second App Process takes the highest Task, the restoring one (task-assignor.ts).
    const second = counter(config, input, output)
    await second.start()
    await eventually(
      async () => {
        const group = await kafka.groupAssignments(applicationId, input)
        return group.state === 'STABLE' && group.assigned.size === 2
      },
      { timeoutMs: 60_000, message: 'both App Processes to have a Task' },
    )
    await kafka.produce(input, [...records('a', 0, 1), ...records('b', RESTORING, 5)])
    await waitForCounts(output, 'a', 3)

    await open.commit()
    await waitForCounts(output, 'b', 11)
    expect(first.state).toBe('running')
    expect(second.state).toBe('running')
    await Promise.all([first.stop(), second.stop()])

    const counts = await committedCounts(output)
    expect(counts.get('a')).toEqual([1, 2, 3])
    expect(counts.get('b')).toEqual(Array.from({ length: 11 }, (_, i) => i + 1))
  }, 180_000)
})

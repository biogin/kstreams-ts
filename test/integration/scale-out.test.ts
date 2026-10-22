// Scaling out across App Processes: several processes with the same Application ID share the
// Tasks over all their Workers, with cooperative-sticky assignment, and stay exact under
// exactly-once while processes come and go.

import { mkdtempSync, readdirSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { type Consumer, type Message, Producer } from '@platformatic/kafka'
import { afterEach, beforeEach, expect, it } from 'vitest'
import { createStreamsApp, serdes } from '../../src/index.js'
import type { WorkersCountParams } from './fixtures/workers-count-app.js'
import { type AppProcess, launchAppModule } from './harness/app-process.js'
import {
  brokers,
  createKafka,
  delay,
  describeWithKafka,
  eventually,
  type TestRecord,
  uniqueName,
} from './kafka.js'

const workersCountApp = new URL('./fixtures/workers-count-app.ts', import.meta.url)
const keys = Array.from({ length: 16 }, (_, i) => `key-${i}`)

describeWithKafka('scale out across App Processes (exactly-once, 2 Workers each)', () => {
  let kafka: ReturnType<typeof createKafka>
  const processes: AppProcess[] = []
  const directories: string[] = []

  beforeEach(() => {
    kafka = createKafka()
  })

  afterEach(async () => {
    await Promise.allSettled(processes.splice(0).map((app) => app.kill()))
    await kafka.close()
    for (const directory of directories.splice(0)) rmSync(directory, { recursive: true })
  })

  function stateDirectory(): string {
    const directory = mkdtempSync(join(tmpdir(), 'kstreams-scale-out-'))
    directories.push(directory)
    return directory
  }

  async function launch(params: Omit<WorkersCountParams, 'brokers' | 'workers'>) {
    const app = await launchAppModule(
      workersCountApp,
      { ...params, brokers, workers: 2 },
      { timeoutMs: 60_000 },
    )
    processes.push(app)
    return app
  }

  function records(count: number, from: number, start: number): TestRecord[] {
    return Array.from({ length: count }, (_, i) => ({
      key: keys[(from + i) % keys.length] ?? null,
      value: `value-${from + i}`,
      timestamp: start + from + i,
    }))
  }

  /** Produces records until `stop()`; `sent` counts them. */
  function keepProducing(input: string) {
    const start = Date.now()
    let sent = 0
    let producing = true
    const done = (async () => {
      while (producing) {
        await kafka.produce(input, records(keys.length, sent, start))
        sent += keys.length
        await delay(50)
      }
    })()
    return {
      get sent() {
        return sent
      },
      async stop(): Promise<void> {
        producing = false
        await done
      },
    }
  }

  /** The sum of the committed offsets of `input`. */
  async function consumed(applicationId: string, input: string): Promise<bigint> {
    const committed = await kafka.committedOffsets(applicationId, input)
    return [...committed.values()].reduce((sum, offset) => sum + offset, 0n)
  }

  /**
   * Waits until the group is stable with `members` members and every partition assigned; returns
   * the partitions of each member, by client ID.
   */
  async function balanced(
    applicationId: string,
    input: string,
    members: number,
    partitions: number,
  ): Promise<Map<string, number[]>> {
    let assigned = new Map<string, number[]>()
    await eventually(
      async () => {
        const group = await kafka.groupAssignments(applicationId, input)
        assigned = group.assigned
        const all = [...assigned.values()].flat()
        return (
          group.state === 'STABLE' &&
          assigned.size === members &&
          all.length === partitions &&
          new Set(all).size === partitions
        )
      },
      { timeoutMs: 60_000, message: `a stable group of ${members} members` },
    )
    return assigned
  }

  /** Exactly-once: each key's committed counts are 1, 2, ..., n, each once, in order. */
  async function expectExactCounts(output: string, total: number): Promise<void> {
    const received = await kafka.consume(output, total, { timeoutMs: 60_000, settleMs: 2_000 })
    expect(received).toHaveLength(total)
    for (const key of keys) {
      const counts = received.filter((record) => record.key === key).map(({ value }) => value)
      const n = Math.floor(total / keys.length) + (keys.indexOf(key) < total % keys.length ? 1 : 0)
      expect(counts, key).toEqual(Array.from({ length: n }, (_, i) => String(i + 1)))
    }
  }

  it('2 processes × 2 Workers share the Tasks with exact counts, even with one state directory', async () => {
    const partitions = 8
    const input = await kafka.createTopic('scale-out-input', partitions)
    const output = await kafka.createTopic('scale-out-output', partitions)
    const applicationId = uniqueName('scale-out')
    kafka.deleteOnClose(`${applicationId}-counts-changelog`)
    const start = Date.now()
    await kafka.produce(input, records(400, 0, start))

    // Both processes use the same state directory, as if on one host by mistake: they get
    // different App Process IDs, and lock the Task directories they use.
    const stateDir = stateDirectory()
    const [a, b] = await Promise.all([
      launch({ applicationId, input, output, stateDir }),
      launch({ applicationId, input, output, stateDir }),
    ])
    const assigned = await balanced(applicationId, input, 4, partitions)
    for (const tasks of assigned.values()) expect(tasks).toHaveLength(2)
    // Four Workers, from two App Processes with different IDs.
    const processIds = new Set(
      [...assigned.keys()].map((clientId) => clientId.slice(applicationId.length + 1, -9)),
    )
    expect(processIds.size).toBe(2)
    expect(
      readdirSync(join(stateDir, applicationId, 'processes')).filter((f) => !f.endsWith('.lock')),
    ).toHaveLength(2)

    await kafka.produce(input, records(400, 400, start))
    await expectExactCounts(output, 800)

    expect(await a?.stop()).toEqual({ code: 0, signal: null })
    expect(await b?.stop()).toEqual({ code: 0, signal: null })
  }, 180_000)

  it('adding and removing a process moves only the Tasks it has to, and stays exact', async () => {
    const partitions = 8
    const input = await kafka.createTopic('scale-out-input', partitions)
    const output = await kafka.createTopic('scale-out-output', partitions)
    const applicationId = uniqueName('scale-out-moves')
    kafka.deleteOnClose(`${applicationId}-counts-changelog`)

    const a = await launch({ applicationId, input, output, stateDir: stateDirectory() })
    const producer = keepProducing(input)
    let total: number
    try {
      const alone = await balanced(applicationId, input, 2, partitions)
      for (const tasks of alone.values()) expect(tasks).toHaveLength(4)
      await eventually(async () => (await consumed(applicationId, input)) > 100n, {
        timeoutMs: 30_000,
        message: 'progress of the first process',
      })

      // A second process joins: each Worker of the first one keeps 2 of its 4 Tasks.
      const b = await launch({ applicationId, input, output, stateDir: stateDirectory() })
      const shared = await balanced(applicationId, input, 4, partitions)
      for (const [worker, tasks] of alone) {
        expect(tasks, worker).toEqual(expect.arrayContaining(shared.get(worker) ?? []))
        expect(shared.get(worker), worker).toHaveLength(2)
      }
      let before = await consumed(applicationId, input)
      await eventually(async () => (await consumed(applicationId, input)) > before + 200n, {
        timeoutMs: 30_000,
        message: 'progress with two processes',
      })

      // It leaves: the first process's Workers keep their Tasks and get its Tasks back.
      expect(await b.stop()).toEqual({ code: 0, signal: null })
      const back = await balanced(applicationId, input, 2, partitions)
      for (const [worker, tasks] of back) {
        expect(tasks, worker).toEqual(expect.arrayContaining(shared.get(worker) ?? []))
        expect(tasks, worker).toHaveLength(4)
      }
      before = await consumed(applicationId, input)
      await eventually(async () => (await consumed(applicationId, input)) > before + 200n, {
        timeoutMs: 30_000,
        message: 'progress after the second process left',
      })
    } finally {
      await producer.stop()
      total = producer.sent
    }
    await eventually(async () => (await consumed(applicationId, input)) === BigInt(total), {
      timeoutMs: 60_000,
      message: 'every input record to be consumed',
    })
    await expectExactCounts(output, total)
    expect(await a.stop()).toEqual({ code: 0, signal: null })
  }, 240_000)

  it("a new owner waits for the previous owner's transaction and starts from its offset", async () => {
    const input = await kafka.createTopic('stable-input', 1)
    const output = await kafka.createTopic('stable-output', 1)
    const applicationId = uniqueName('stable-offsets')
    await kafka.produce(input, records(10, 0, Date.now()))

    // The previous owner of the Task: its transaction commits the offset after record 5 (6), and
    // is still open when the app takes the Task over.
    const previous = new Producer({
      clientId: uniqueName('previous-owner'),
      bootstrapBrokers: brokers,
      idempotent: true,
      transactionalId: uniqueName('previous-owner'),
      producerId: -1n,
      producerEpoch: -1,
      timeout: 60_000,
    })
    const app = createStreamsApp({
      applicationId,
      brokers,
      guarantee: 'exactly-once',
      sessionTimeoutMs: 6_000,
    })
    app.stream(input, { key: serdes.string, value: serdes.string }).to(output)
    try {
      const transaction = await previous.beginTransaction()
      await transaction.addConsumer({ groupId: applicationId } as Consumer)
      const metadata = await kafka.admin.metadata({ topics: [input] })
      const [coordinatorId] = [...metadata.brokers.keys()]
      await transaction.addOffset({
        topic: input,
        partition: 0,
        offset: 5n,
        metadata: {
          consumer: { groupId: applicationId, memberId: '', generationId: -1, coordinatorId },
        },
      } as unknown as Message)

      await app.start()
      await balanced(applicationId, input, 1, 1)
      // The client read the committed offset without `requireStable`, so it starts the Task from
      // the beginning; the Worker must not process records 0..5 nonetheless.
      await transaction.commit()
      const received = await kafka.consume(output, 4, { timeoutMs: 30_000, settleMs: 2_000 })
      expect(received.map(({ value }) => value)).toEqual(
        Array.from({ length: 4 }, (_, i) => `value-${6 + i}`),
      )
    } finally {
      await previous.close(true)
      await app.stop()
    }
  }, 120_000)
})

import { EventEmitter } from 'node:events'
import { describe, expect, it } from 'vitest'
import { createStreamsApp, type ProcessorContext, serdes } from '../src/index.js'
import type { StableOffsetReader } from '../src/stable-offsets.js'
import { processorGraphOf } from '../src/streams-app.js'
import {
  type BatchCommitter,
  CommitCycle,
  type CommitCycleClients,
  type CommitGuard,
  type Stream,
  type WorkerOptions,
} from '../src/worker.js'

// Worker-level interleavings under exactly-once, run deterministically: the documented exception
// to "no mocking the Kafka client" (ADR-0016). The consumer is a fake group member (as in
// ownership.test.ts), the stream a queue the test fills, and the committer a fake that records
// what the batch would commit.

/** A consumer-group member: an assignment, a generation and events. */
class FakeMember extends EventEmitter {
  generationId = 5
  assignments: { topic: string; partitions: number[] }[] = [{ topic: 't', partitions: [0] }]

  /** The first round of a cooperative rebalance that takes partitions away. */
  revoke(generationId: number, partitions: number[]): void {
    this.generationId = generationId
    this.assignments = [{ topic: 't', partitions }]
    this.emit('consumer:group:rebalance')
  }

  /** A completed rebalance round. */
  join(generationId: number, partitions: number[]): void {
    this.generationId = generationId
    this.assignments = [{ topic: 't', partitions }]
    this.emit('consumer:group:join')
  }
}

/** A stream of records the test pushes, fetched under the member's generation at push time. */
function fakeStream(member: FakeMember) {
  const queued: unknown[] = []
  let waiting: ((result: IteratorResult<unknown>) => void) | undefined
  const stream = {
    [Symbol.asyncIterator]: () => ({
      next: () =>
        new Promise<IteratorResult<unknown>>((resolve) => {
          const value = queued.shift()
          if (value !== undefined) resolve({ value, done: false })
          else waiting = resolve
        }),
    }),
    close: async () => {},
  }
  return {
    stream: stream as unknown as Stream,
    push(partition: number, offset: bigint) {
      const value = {
        topic: 't',
        partition,
        offset,
        key: null,
        value: Buffer.from('v'),
        timestamp: 0n,
        metadata: { consumer: { generationId: member.generationId } },
      }
      const resolve = waiting
      waiting = undefined
      if (resolve === undefined) queued.push(value)
      else resolve({ value, done: false })
    },
  }
}

/** A committer that records the batches with outputs it commits, and its aborts. */
class FakeCommitter implements BatchCommitter {
  outputs = 0
  readonly commits: CommitGuard[] = []
  aborts = 0
  onCommit: () => void = () => {}
  onAbort: () => void = () => {}

  produce(): void {
    this.outputs++
  }

  async commit(_lastRead: unknown[], guard?: CommitGuard) {
    const empty = this.outputs === 0
    this.outputs = 0
    if (!empty && guard !== undefined) {
      this.commits.push(guard)
      this.onCommit()
    }
    return { produced: new Map(), offsetsCommitted: true, empty }
  }

  async abort() {
    this.aborts++
    this.outputs = 0
    this.onAbort()
    return true
  }
}

/**
 * A Commit Cycle whose Tasks write only from a wall-clock timer (every millisecond), calling
 * `onTick` after each write. The stable committed offset of partition 0 is `stable`.
 */
function setUp(stable: bigint, onTick: () => void = () => {}) {
  const member = new FakeMember()
  const streams: ReturnType<typeof fakeStream>[] = []
  const committer = new FakeCommitter()
  let processors = 0
  const strings = { key: serdes.string, value: serdes.string }
  const app = createStreamsApp({ applicationId: 'cycle' })
  const ticker = app.processor('ticker', (ctx: ProcessorContext<string, string>) => {
    processors++
    ctx.schedule(1, 'wall-clock-time', (timestamp) => {
      ctx.forward({ key: 'tick', value: 'tick', timestamp })
      onTick()
    })
    return { process() {} }
  })
  app.stream('t', strings).process(ticker).to('out', strings)
  const stableOffsets: StableOffsetReader = {
    read: async () => new Map([['t', stable]]),
    close: async () => {},
  }
  const open = () => {
    const stream = fakeStream(member)
    streams.push(stream)
    return stream.stream
  }
  const clients: CommitCycleClients = {
    consumer: member as unknown as CommitCycleClients['consumer'],
    createCommitter: () => committer,
    stream: open(),
    openStream: async () => open(),
    restorer: undefined,
    stableOffsets,
  }
  const options: WorkerOptions = {
    applicationId: 'cycle',
    brokers: [],
    clientId: 'cycle',
    transactionalId: 'cycle-worker-0',
    commitIntervalMs: 20,
    commitMaxRecords: 100,
    maxProduceBytes: 900_000,
    sessionTimeoutMs: 10_000,
    shutdownTimeoutMs: 1_000,
    changelogReplicationFactor: 1,
    autoCreateInternalTopics: false,
    stateDir: undefined,
    lockHolder: 'cycle/worker-0',
    workerNumber: 0,
    collectMetrics: false,
    report: () => {},
    onError: () => {},
  }
  return {
    member,
    committer,
    cycle: new CommitCycle(processorGraphOf(app), options, clients),
    /** The stream the cycle reads now. */
    stream: () => streams[streams.length - 1] as ReturnType<typeof fakeStream>,
    processors: () => processors,
  }
}

describe('a Task that writes only from a timer (exactly-once)', () => {
  it('commits its committed offset again, to fence its writes, though it read no record', async () => {
    // Taken over: the record held while the Task started is below its stable committed offset,
    // so it is skipped, and the Task has a part in the batch only through its timer.
    const { cycle, committer, stream } = setUp(200n)
    const committed = new Promise<void>((resolve) => {
      committer.onCommit = resolve
    })
    const running = cycle.run()
    stream().push(0, 150n)
    await committed
    cycle.requestStop()
    await running
    const fencing = committer.commits[0]?.fencing.map(({ topic, partition, offset }) => ({
      topic,
      partition,
      offset,
    }))
    expect(fencing).toEqual([{ topic: 't', partition: 0, offset: 199n }])
  })

  it('aborts the batch and closes the Task when its partition is revoked and assigned back', async () => {
    let rebalanced = false
    const setup = setUp(200n, () => {
      if (rebalanced) return
      rebalanced = true
      // After the timer's write, before the commit: lost, and assigned back at once.
      setup.member.revoke(6, [])
      setup.member.join(7, [0])
    })
    const { cycle, committer, stream, processors } = setup
    const aborted = new Promise<void>((resolve) => {
      committer.onAbort = resolve
    })
    const running = cycle.run()
    stream().push(0, 150n)
    await aborted
    // The cycle reads every partition again, with a new stream, once it has closed the Tasks.
    await new Promise((resolve) => setTimeout(resolve, 0))
    expect(committer.commits).toEqual([])
    // The Task was closed: a record of its partition now starts a new one, which commits.
    const committed = new Promise<void>((resolve) => {
      committer.onCommit = resolve
    })
    stream().push(0, 210n)
    await committed
    cycle.requestStop()
    await running
    expect(processors()).toBe(2)
    expect(committer.aborts).toBe(1)
  })
})

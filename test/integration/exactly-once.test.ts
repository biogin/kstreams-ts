import { afterEach, beforeEach, expect, it } from 'vitest'
import { createStreamsApp, type StreamsApp, serdes } from '../../src/index.js'
import {
  brokers,
  createKafka,
  describeWithKafka,
  eventually,
  type TestRecord,
  uniqueName,
} from './kafka.js'

describeWithKafka('stateless Streams App on real Kafka (exactly-once, workers: 0)', () => {
  let kafka: ReturnType<typeof createKafka>
  const apps: StreamsApp[] = []

  beforeEach(() => {
    kafka = createKafka()
  })

  afterEach(async () => {
    await Promise.allSettled(apps.splice(0).map((app) => app.stop()))
    await kafka.close()
  })

  /** A pass-through app that fails on the value `failOn`, if given. */
  function exactlyOnceApp(
    input: string,
    output: string,
    options: {
      applicationId?: string
      commitIntervalMs?: number
      commitMaxRecords?: number
      failOn?: string
    } = {},
  ) {
    const { applicationId = uniqueName('eos'), failOn, ...commit } = options
    const app = createStreamsApp({
      applicationId,
      brokers,
      guarantee: 'exactly-once',
      sessionTimeoutMs: 10_000,
      ...commit,
    })
    app
      .stream(input, { key: serdes.string, value: serdes.string })
      .peek((_key, value) => {
        if (value === failOn) throw new Error('Processor failed on purpose')
      })
      .to(output)
    apps.push(app)
    return app
  }

  // Recent timestamps: the broker rolls a log segment on records older than `segment.ms`, which
  // can put an abort marker in a later fetch than its records. The client then fails to filter
  // those aborted records out under read_committed (a client bug).
  const now = Date.now()

  function records(count: number, from = 0): TestRecord[] {
    return Array.from({ length: count }, (_, i) => ({
      key: `key-${(from + i) % 7}`,
      value: `value-${from + i}`,
      timestamp: now + from + i,
    }))
  }

  const withoutPartition = ({ key, value, timestamp }: TestRecord) => ({ key, value, timestamp })
  const byValue = (a: TestRecord, b: TestRecord) => String(a.value).localeCompare(String(b.value))

  async function committedTotal(applicationId: string, topic: string): Promise<bigint> {
    const committed = await kafka.committedOffsets(applicationId, topic)
    return [...committed.values()].reduce((sum, offset) => sum + offset, 0n)
  }

  it('commits outputs and consumed offsets together, in several batches', async () => {
    const input = await kafka.createTopic('input', 3)
    const output = await kafka.createTopic('output', 3)
    const sent = records(300)
    await kafka.produce(input, sent)

    const app = exactlyOnceApp(input, output, { commitMaxRecords: 25 })
    await app.start()

    const received = await kafka.consume(output, sent.length, { settleMs: 1_000 })
    expect(received.map(withoutPartition).sort(byValue)).toEqual([...sent].sort(byValue))
    await eventually(async () => (await committedTotal(app.applicationId, input)) === 300n, {
      message: 'offsets committed in the transaction',
    })

    await app.stop()
    expect(app.state).toBe('stopped')
  })

  it('passes tombstones and null keys through', async () => {
    const input = await kafka.createTopic('input')
    const output = await kafka.createTopic('output')
    const sent: TestRecord[] = [
      { key: 'deleted', value: null, timestamp: now },
      { key: null, value: 'no key', timestamp: now + 1 },
    ]
    await kafka.produce(input, sent)

    await exactlyOnceApp(input, output).start()

    expect((await kafka.consume(output, sent.length)).map(withoutPartition)).toEqual(sent)
  })

  it('aborts a failed batch: read_committed never sees its outputs', async () => {
    const input = await kafka.createTopic('input')
    const output = await kafka.createTopic('output')
    const applicationId = uniqueName('eos-abort')
    // One long batch, so its first outputs are written before the failing record arrives.
    const app = exactlyOnceApp(input, output, {
      applicationId,
      commitIntervalMs: 60_000,
      failOn: 'boom',
    })
    await app.start()

    await kafka.produce(input, records(2))
    // The outputs are in the open transaction: written, but not committed.
    expect(await kafka.consume(output, 2, { uncommitted: true })).toHaveLength(2)
    expect(await kafka.consume(output, 0, { settleMs: 1_000 })).toEqual([])

    await kafka.produce(input, [{ key: 'k', value: 'boom', timestamp: now + 2 }])
    await eventually(() => app.state === 'error', { message: 'the error state' })
    await expect(app.stop()).rejects.toThrow('Processor failed on purpose')

    // Aborted: nothing visible to read_committed, no offsets committed.
    expect(await kafka.consume(output, 0, { settleMs: 1_500 })).toEqual([])
    expect(await committedTotal(applicationId, input)).toBe(0n)

    // A restart reuses the transactional ID and processes the batch again, exactly once.
    await exactlyOnceApp(input, output, { applicationId }).start()
    const received = await kafka.consume(output, 3, { settleMs: 1_500 })
    expect(received.map((record) => record.value)).toEqual(['value-0', 'value-1', 'boom'])
  })

  it('resumes after a restart with the same Application ID without processing anything twice', async () => {
    const input = await kafka.createTopic('input', 2)
    const output = await kafka.createTopic('output', 2)
    const applicationId = uniqueName('eos-restart')

    const first = exactlyOnceApp(input, output, { applicationId })
    await first.start()
    await kafka.produce(input, records(30))
    await kafka.consume(output, 30)
    await first.stop()

    await kafka.produce(input, records(20, 30))
    const second = exactlyOnceApp(input, output, { applicationId })
    await second.start()

    const received = await kafka.consume(output, 50, { settleMs: 1_500 })
    expect(received).toHaveLength(50)
    expect(new Set(received.map((record) => record.value)).size).toBe(50)
    await eventually(async () => (await committedTotal(applicationId, input)) === 50n, {
      message: 'offsets committed after the restart',
    })
  })
})

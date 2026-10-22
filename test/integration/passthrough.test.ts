import { afterEach, beforeEach, expect, it } from 'vitest'
import {
  type ApplicationState,
  createStreamsApp,
  type StreamsApp,
  serdes,
} from '../../src/index.js'
import {
  brokers,
  createKafka,
  describeWithKafka,
  eventually,
  type TestRecord,
  uniqueName,
} from './kafka.js'

describeWithKafka('pass-through Streams App on real Kafka (at-least-once, workers: 0)', () => {
  let kafka: ReturnType<typeof createKafka>
  const apps: StreamsApp[] = []

  beforeEach(() => {
    kafka = createKafka()
  })

  afterEach(async () => {
    await Promise.allSettled(apps.splice(0).map((app) => app.stop()))
    await kafka.close()
  })

  function passThroughApp(
    input: string,
    output: string,
    options: { applicationId?: string; commitIntervalMs?: number; commitMaxRecords?: number } = {},
  ) {
    const { applicationId = uniqueName('passthrough'), ...commit } = options
    const app = createStreamsApp({ applicationId, brokers, sessionTimeoutMs: 10_000, ...commit })
    app.stream(input, { key: serdes.string, value: serdes.string }).to(output)
    apps.push(app)
    return app
  }

  function records(count: number, from = 0): TestRecord[] {
    return Array.from({ length: count }, (_, i) => ({
      key: `key-${(from + i) % 7}`,
      value: `value-${from + i}`,
      timestamp: 1_700_000_000_000 + from + i,
    }))
  }

  const withoutPartition = ({ key, value, timestamp }: TestRecord) => ({ key, value, timestamp })
  const byValue = (a: TestRecord, b: TestRecord) => String(a.value).localeCompare(String(b.value))

  it('copies N records from the input topic to the output topic and commits their offsets', async () => {
    const input = await kafka.createTopic('input', 3)
    const output = await kafka.createTopic('output', 3)
    const sent = records(200)
    await kafka.produce(input, sent)

    const app = passThroughApp(input, output)
    const states: ApplicationState[] = [app.state]
    await app.start()
    states.push(app.state)

    const received = await kafka.consume(output, sent.length)
    expect(received.map(withoutPartition).sort(byValue)).toEqual([...sent].sort(byValue))

    // Same key, same partition: records keep their per-key order.
    for (const key of new Set(sent.map((record) => record.key))) {
      const values = (list: TestRecord[]) =>
        list.filter((record) => record.key === key).map((record) => record.value)
      expect(values(received)).toEqual(values(sent))
      expect(new Set(received.filter((r) => r.key === key).map((r) => r.partition)).size).toBe(1)
    }

    const stopping = app.stop()
    states.push(app.state)
    await stopping
    states.push(app.state)
    expect(states).toEqual(['created', 'running', 'stopping', 'stopped'])

    // The consumer group is the Application ID; every input record's offset is committed.
    expect(await kafka.consumed(app.applicationId, input)).toBe(BigInt(sent.length))
  })

  it('passes tombstones and null keys through', async () => {
    const input = await kafka.createTopic('input')
    const output = await kafka.createTopic('output')
    const sent: TestRecord[] = [
      { key: 'deleted', value: null, timestamp: 1 },
      { key: null, value: 'no key', timestamp: 2 },
      { key: '', value: '', timestamp: 3 },
    ]
    await kafka.produce(input, sent)

    await passThroughApp(input, output).start()

    expect((await kafka.consume(output, sent.length)).map(withoutPartition)).toEqual(sent)
  })

  it('processes records produced while it runs', async () => {
    const input = await kafka.createTopic('input', 2)
    const output = await kafka.createTopic('output', 2)
    await passThroughApp(input, output).start()

    const sent = records(50)
    await kafka.produce(input, sent)

    const received = await kafka.consume(output, sent.length)
    expect(received.map(withoutPartition).sort(byValue)).toEqual([...sent].sort(byValue))
  })

  it('leaves the group on stop and resumes from the committed offsets on restart', async () => {
    const input = await kafka.createTopic('input', 2)
    const output = await kafka.createTopic('output', 2)
    const applicationId = uniqueName('restart')

    const first = passThroughApp(input, output, { applicationId })
    await first.start()
    await kafka.produce(input, records(30))
    await kafka.consume(output, 30)
    await first.stop()
    expect(await kafka.groupMembers(applicationId)).toBe(0)

    await kafka.produce(input, records(20, 30))
    await passThroughApp(input, output, { applicationId }).start()

    // Exactly the 30 + 20 records: nothing processed twice after a graceful stop.
    const received = await kafka.consume(output, 50, { settleMs: 1_500 })
    expect(received).toHaveLength(50)
    expect(new Set(received.map((record) => record.value)).size).toBe(50)
  })

  it('closes a batch by count before the commit interval runs out', async () => {
    const input = await kafka.createTopic('input')
    const output = await kafka.createTopic('output')
    const app = passThroughApp(input, output, { commitIntervalMs: 60_000, commitMaxRecords: 10 })
    await app.start()

    await kafka.produce(input, records(10))

    const received = await kafka.consume(output, 10, { timeoutMs: 15_000 })
    expect(received).toHaveLength(10)
    await eventually(
      async () => (await kafka.committedOffsets(app.applicationId, input)).get(0) === 10n,
      {
        message: 'offsets committed',
      },
    )
  })

  it('goes to the error state when processing fails, without committing the failed batch', async () => {
    const input = await kafka.createTopic('input')
    const output = await kafka.createTopic('output')
    const app = createStreamsApp({ applicationId: uniqueName('failing'), brokers })
    apps.push(app)
    app
      .stream(input, { key: serdes.string, value: serdes.string })
      .peek((_key, value) => {
        if (value === 'boom') throw new Error('Processor failed on purpose')
      })
      .to(output)
    await app.start()

    await kafka.produce(input, [{ key: 'k', value: 'boom', timestamp: 1 }])

    await eventually(() => app.state === 'error', { message: 'the error state' })
    await expect(app.stop()).rejects.toThrow('Processor failed on purpose')
    expect(app.state).toBe('error')
    expect((await kafka.committedOffsets(app.applicationId, input)).get(0) ?? -1n).toBe(-1n)
  })
})

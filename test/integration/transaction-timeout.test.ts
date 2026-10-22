import { Producer } from '@platformatic/kafka'
import { afterEach, beforeEach, expect, it } from 'vitest'
import { createStreamsApp, type Metric, type StreamsApp, serdes } from '../../src/index.js'
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

describeWithKafka('exactly-once batches and the transaction timeout (workers: 0)', () => {
  let kafka: ReturnType<typeof createKafka>
  const apps: StreamsApp[] = []

  beforeEach(() => {
    kafka = createKafka()
  })

  afterEach(async () => {
    await Promise.allSettled(apps.splice(0).map((app) => app.stop()))
    await kafka.close()
  })

  /**
   * An exactly-once pass-through app with a 4 s transaction timeout, whose Processor takes
   * `delayMs(value)` per record; it records the app's `aborts` metrics.
   */
  function slowApp(input: string, output: string, delayMs: (value: string | null) => number) {
    const aborts: Metric[] = []
    const app = createStreamsApp({
      applicationId: uniqueName('eos-slow'),
      brokers,
      guarantee: 'exactly-once',
      sessionTimeoutMs: 10_000,
      transactionTimeoutMs: 4_000,
      metrics: {
        record: (metric) => {
          if (metric.name === 'aborts') aborts.push(metric)
        },
      },
    })
    const slow = app.processor<string, string | null, string, string | null>('slow', (ctx) => ({
      async process(record) {
        await delay(delayMs(record.value))
        ctx.forward(record)
      },
    }))
    app.stream(input, strings).process(slow).to(output, strings)
    apps.push(app)
    return { app, aborts }
  }

  const now = Date.now()
  const records = (count: number): TestRecord[] =>
    Array.from({ length: count }, (_, i) => ({
      key: `key-${i}`,
      value: `value-${i}`,
      timestamp: now + i,
    }))

  it('keeps batches of slow async work within the transaction timeout', async () => {
    const input = await kafka.createTopic('input', 1)
    const output = await kafka.createTopic('output', 1)
    // 300 records at 30 ms each: 9 s of work, more than twice the transaction timeout.
    const { app, aborts } = slowApp(input, output, () => 30)
    const sent = records(300)
    await kafka.produce(input, sent)
    await app.start()

    const received = await kafka.consume(output, sent.length, { timeoutMs: 60_000 })
    expect(received.map(({ value }) => value)).toEqual(sent.map(({ value }) => value))
    await eventually(
      async () => (await kafka.committedOffsets(app.applicationId, input)).get(0) === 300n,
      { timeoutMs: 30_000, message: 'every offset committed' },
    )
    expect(app.state).toBe('running')
    // Several short transactions, none aborted.
    expect(aborts).toEqual([])
  }, 120_000)

  it('aborts and replays a batch whose transaction the broker aborted for its timeout', async () => {
    const input = await kafka.createTopic('input', 1)
    const output = await kafka.createTopic('output', 1)
    // One record takes 10 s, every time: past the timeout and the broker's check after it (5 s).
    const { app, aborts } = slowApp(input, output, (value) => (value === 'value-3' ? 10_000 : 0))
    const sent = records(6)
    await kafka.produce(input, sent)
    await app.start()

    const received = await kafka.consume(output, sent.length, {
      timeoutMs: 90_000,
      settleMs: 2_000,
    })
    // Exactly once, in order: the aborted batch's outputs are never read.
    expect(received.map(({ value }) => value)).toEqual(sent.map(({ value }) => value))
    expect(app.state).toBe('running')
    expect(aborts.length).toBeGreaterThan(0)
    expect(
      aborts.every(({ labels }) => 'reason' in labels && labels.reason === 'transaction-timeout'),
    ).toBe(true)
  }, 150_000)

  // What keeps a zombie fenced: a Worker takes its transactional ID up again with its old epoch
  // after a timed-out transaction, which the broker refuses once another producer has the ID.
  it('the broker refuses an old epoch once another producer took the transactional ID', async () => {
    const topic = await kafka.createTopic('fenced', 1)
    const transactionalId = uniqueName('fenced')
    const producers: Producer<Buffer, Buffer, Buffer, Buffer>[] = []
    const producer = () => {
      const created = new Producer<Buffer, Buffer, Buffer, Buffer>({
        clientId: uniqueName('fenced'),
        bootstrapBrokers: brokers,
        idempotent: true,
        transactionalId,
        producerId: -1n,
        producerEpoch: -1,
      })
      producers.push(created)
      return created
    }
    try {
      const zombie = producer()
      const old = await zombie.beginTransaction()
      await old.send({ messages: [{ topic, value: Buffer.from('zombie') }] })
      const retake = {
        producerId: zombie.producerId as bigint,
        producerEpoch: zombie.producerEpoch as number,
      }
      const successor = producer()
      await (await successor.beginTransaction()).send({
        messages: [{ topic, value: Buffer.from('successor') }],
      })
      await old.cancel()
      await expect(
        (async () => {
          await zombie.beginTransaction(retake)
          if (zombie.producerEpoch === retake.producerEpoch) {
            await zombie.initIdempotentProducer(retake)
          }
        })(),
      ).rejects.toThrow()
    } finally {
      await Promise.allSettled(producers.map((created) => created.close()))
    }
  })
})

import { afterEach, beforeEach, expect, it } from 'vitest'
import {
  createStreamsApp,
  type ProcessorContext,
  type StreamRecord,
  type StreamsApp,
  serdes,
} from '../../src/index.js'
import { brokers, createKafka, describeWithKafka, type TestRecord, uniqueName } from './kafka.js'

const strings = { key: serdes.string, value: serdes.string }

describeWithKafka('Co-partitioned input topics on real Kafka', () => {
  let kafka: ReturnType<typeof createKafka>
  const apps: StreamsApp[] = []

  beforeEach(() => {
    kafka = createKafka()
  })

  afterEach(async () => {
    await Promise.allSettled(apps.splice(0).map((app) => app.stop()))
    await kafka.close()
  })

  /** Counts the records of each key across both input topics, in one State Store per Task. */
  function countBothApp(orders: string, customers: string, output: string): StreamsApp {
    const app = createStreamsApp({
      applicationId: uniqueName('copartitioned'),
      brokers,
      sessionTimeoutMs: 10_000,
    })
    const merge = app.processor('merge', (ctx: ProcessorContext<string, string | null>) => ({
      process(record: StreamRecord<string, string | null>) {
        ctx.forward(record)
      },
    }))
    app.stream(orders, strings).process(merge)
    app
      .stream(customers, strings)
      .process(merge)
      .groupByKey({ key: serdes.string })
      .count()
      .toStream()
      .map((key, count) => ({ key, value: String(count) }))
      .to(output, strings)
    apps.push(app)
    return app
  }

  function records(topic: string, count: number): TestRecord[] {
    return Array.from({ length: count }, (_, i) => ({
      key: `key-${i % 5}`,
      value: `${topic}-${i}`,
      timestamp: 1_700_000_000_000 + i,
    }))
  }

  it('forms each Task from one partition number across all input topics', async () => {
    const orders = await kafka.createTopic('orders', 3)
    const customers = await kafka.createTopic('customers', 3)
    const output = await kafka.createTopic('counts', 3)
    // Keyed records land on the same partition number in both topics (same partitioner).
    await kafka.produce(orders, records('order', 30))
    await kafka.produce(customers, records('customer', 20))

    await countBothApp(orders, customers, output).start()

    // A key's records from both topics reach the same Task, so one count sees all 6 + 4 of them.
    const received = await kafka.consume(output, 50)
    const finalCounts = new Map<string, number>()
    for (const { key, value } of received) {
      finalCounts.set(String(key), Math.max(finalCounts.get(String(key)) ?? 0, Number(value)))
    }
    expect(Object.fromEntries(finalCounts)).toEqual({
      'key-0': 10,
      'key-1': 10,
      'key-2': 10,
      'key-3': 10,
      'key-4': 10,
    })
  })

  it('refuses to start when the input topics have different partition counts', async () => {
    const orders = await kafka.createTopic('orders', 3)
    const customers = await kafka.createTopic('customers', 2)
    const output = await kafka.createTopic('counts', 3)
    const app = countBothApp(orders, customers, output)

    const started = app.start()
    await expect(started).rejects.toThrow(/not Co-partitioned/)
    await expect(started).rejects.toThrow(`"${orders}" has 3 partitions`)
    await expect(started).rejects.toThrow(`"${customers}" has 2 partitions`)
    expect(app.state).toBe('error')
    // It never joined the consumer group.
    expect(await kafka.groupMembers(app.applicationId)).toBe(0)
  })
})

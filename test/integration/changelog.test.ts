import { afterEach, beforeEach, expect, it } from 'vitest'
import {
  createStreamsApp,
  type StateStore,
  type StateStoreSupplier,
  type StreamsApp,
  serdes,
  stores,
  tumbling,
} from '../../src/index.js'
import { keyOf } from '../../src/store-key.js'
import { windowedSerde, windowKeyEncoding } from '../../src/windows.js'
import {
  type BytesRecord,
  brokers,
  createKafka,
  describeWithKafka,
  type TestRecord,
  uniqueName,
} from './kafka.js'

describeWithKafka('Changelogs on real Kafka (at-least-once, workers: 0)', () => {
  let kafka: ReturnType<typeof createKafka>
  const apps: StreamsApp[] = []

  beforeEach(() => {
    kafka = createKafka()
  })

  afterEach(async () => {
    await Promise.allSettled(apps.splice(0).map((app) => app.stop()))
    await kafka.close()
  })

  /** A Streams App whose Changelogs are deleted after the test. */
  function app(prefix: string, storeNames: string[]): StreamsApp {
    const applicationId = uniqueName(prefix)
    const created = createStreamsApp({
      applicationId,
      brokers,
      sessionTimeoutMs: 10_000,
      changelogReplicationFactor: 1,
    })
    for (const store of storeNames) kafka.deleteOnClose(`${applicationId}-${store}-changelog`)
    apps.push(created)
    return created
  }

  /** In-memory stores, kept by Task so the test can read them once the app has stopped. */
  function capturingSupplier(): { supplier: StateStoreSupplier; byTask: Map<number, StateStore> } {
    const byTask = new Map<number, StateStore>()
    return {
      byTask,
      supplier: (context) => {
        const store = stores.inMemory()(context)
        byTask.set(context.task, store)
        return store
      },
    }
  }

  /** What a compacted Changelog partition boils down to: the last value of each key. */
  function compact(records: BytesRecord[], partition: number): Map<string, Buffer> {
    const latest = new Map<string, Buffer>()
    for (const record of records.filter((r) => r.partition === partition)) {
      const key = (record.key as Buffer).toString('hex')
      if (record.value === null) latest.delete(key)
      else latest.set(key, record.value)
    }
    return latest
  }

  function words(count: number): TestRecord[] {
    return Array.from({ length: count }, (_, i) => ({
      key: `word-${i % 17}`,
      value: `${i}`,
      timestamp: 1_700_000_000_000 + i * 1_000,
    }))
  }

  it('creates a compacted Changelog per State Store with the source partition count', async () => {
    const input = await kafka.createTopic('input', 3)
    const counter = app('changelog-topic', ['counts'])
    counter
      .stream(input, { key: serdes.string, value: serdes.string })
      .groupByKey()
      .count({ store: 'counts' })
    await counter.start()

    const changelog = `${counter.applicationId}-counts-changelog`
    const metadata = await kafka.admin.metadata({ topics: [changelog], forceUpdate: true })
    const topic = metadata.topics.get(changelog)
    expect(topic?.partitionsCount).toBe(3)
    expect(topic?.partitions.every((partition) => partition.replicas.length === 1)).toBe(true)

    const [described] = await kafka.admin.describeConfigs({
      resources: [
        { resourceType: 2, resourceName: changelog, configurationKeys: ['cleanup.policy'] },
      ],
    })
    expect(described?.configs.find(({ name }) => name === 'cleanup.policy')?.value).toBe('compact')
  })

  it('fails to start if an existing Changelog has the wrong partition count', async () => {
    const input = await kafka.createTopic('input', 2)
    const counter = app('changelog-mismatch', ['counts'])
    const changelog = `${counter.applicationId}-counts-changelog`
    await kafka.admin.createTopics({ topics: [changelog], partitions: 1, replicas: 1 })
    counter
      .stream(input, { key: serdes.string, value: serdes.string })
      .groupByKey()
      .count({ store: 'counts' })

    await expect(counter.start()).rejects.toThrow(
      `Changelog "${changelog}" has 1 partition, but the input topics have 2`,
    )
  })

  it('after count(), the Changelog contents match the final store contents', async () => {
    const input = await kafka.createTopic('input', 3)
    const output = await kafka.createTopic('output', 3)
    const { supplier, byTask } = capturingSupplier()
    const counter = app('changelog-count', ['counts'])
    counter
      .stream(input, { key: serdes.string, value: serdes.string })
      .groupByKey()
      .count({ store: 'counts', storeSupplier: supplier })
      .toStream()
      .to(output, { key: serdes.string, value: serdes.number })
    await counter.start()

    const sent = words(200)
    await kafka.produce(input, sent)
    await kafka.consume(output, sent.length)
    await counter.stop()

    const changelog = await kafka.consumeBytes(
      `${counter.applicationId}-counts-changelog`,
      sent.length,
    )
    // One Changelog record per store write, in the partition of the Task that wrote it.
    expect(changelog).toHaveLength(sent.length)
    expect(byTask.size).toBe(3)
    const context = { topic: `${counter.applicationId}-counts-changelog` }
    let total = 0
    for (const [task, store] of byTask) {
      const contents = new Map(
        [...store.all()].map(([key, value]) => [
          Buffer.from(keyOf(key, serdes.string, context)).toString('hex'),
          serdes.number.deserialize(value, context),
        ]),
      )
      const logged = new Map(
        [...compact(changelog, task)].map(([key, value]) => [
          key,
          serdes.number.deserialize(value, context),
        ]),
      )
      expect(logged).toEqual(contents)
      for (const count of contents.values()) total += count
    }
    expect(total).toBe(sent.length)
  })

  it('writes deletes as tombstones', async () => {
    const input = await kafka.createTopic('input', 2)
    const { supplier, byTask } = capturingSupplier()
    const latest = app('changelog-delete', ['latest'])
    const store = latest.store('latest', { key: serdes.string, value: serdes.string }, { supplier })
    // Keeps the latest value per key; a tombstone deletes the key.
    const keeper = latest.processor<string, string | null, never, never>('keeper', (ctx) => ({
      process({ key, value }) {
        if (value === null) ctx.store(store).delete(key)
        else ctx.store(store).put(key, value)
      },
    }))
    latest.stream(input, { key: serdes.string, value: serdes.string }).process(keeper)
    await latest.start()

    const sent: TestRecord[] = [
      { key: 'a', value: '1', timestamp: 1, partition: 0 },
      { key: 'b', value: '2', timestamp: 2, partition: 1 },
      { key: 'a', value: null, timestamp: 3, partition: 0 },
      { key: 'c', value: '3', timestamp: 4, partition: 1 },
    ]
    await kafka.produce(input, sent)
    const changelog = await kafka.consumeBytes(`${latest.applicationId}-latest-changelog`, 4)
    await latest.stop()

    const readable = changelog.map(({ key, value, partition }) => ({
      key: key?.toString(),
      value: value?.toString() ?? null,
      partition,
    }))
    expect(readable.filter((r) => r.partition === 0)).toEqual([
      { key: 'a', value: '1', partition: 0 },
      { key: 'a', value: null, partition: 0 },
    ])
    expect(readable.filter((r) => r.partition === 1)).toEqual([
      { key: 'b', value: '2', partition: 1 },
      { key: 'c', value: '3', partition: 1 },
    ])
    expect([...(byTask.get(0)?.all() ?? [])]).toEqual([])
  })

  it('writes window store keys with the windowed key Serde', async () => {
    const input = await kafka.createTopic('input', 2)
    const output = await kafka.createTopic('output', 2)
    const { supplier, byTask } = capturingSupplier()
    const windows = tumbling('1m')
    const counter = app('changelog-window', ['windowed'])
    counter
      .stream(input, { key: serdes.string, value: serdes.string })
      .groupByKey()
      .window(windows)
      .count({ store: 'windowed', storeSupplier: supplier })
      .toStream()
      .to(output, { value: serdes.number })
    await counter.start()

    const sent = words(150)
    await kafka.produce(input, sent)
    await kafka.consume(output, sent.length)
    await counter.stop()

    const topic = `${counter.applicationId}-windowed-changelog`
    const changelog = await kafka.consumeBytes(topic, sent.length)
    const context = { topic }
    const keySerde = windowedSerde(serdes.string, windows)
    const encoding = windowKeyEncoding(serdes.string, windows)
    for (const [task, store] of byTask) {
      const contents = [...store.all()]
        .map(([key, value]) => ({
          key: encoding.decode(key, context),
          count: serdes.number.deserialize(value, context),
        }))
        .sort(byWindowAndKey)
      const logged = [...compact(changelog, task)]
        .map(([key, value]) => ({
          key: keySerde.deserialize(Buffer.from(key, 'hex'), context),
          count: serdes.number.deserialize(value, context),
        }))
        .sort(byWindowAndKey)
      expect(logged).toEqual(contents)
    }
  })
})

function byWindowAndKey(
  a: { key: { key: string; window: { start: number } } },
  b: { key: { key: string; window: { start: number } } },
): number {
  return a.key.window.start - b.key.window.start || a.key.key.localeCompare(b.key.key)
}

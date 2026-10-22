import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { Producer } from '@platformatic/kafka'
import { afterEach, beforeEach, expect, it } from 'vitest'
import {
  createStreamsApp,
  type StateStore,
  type StateStoreSupplier,
  type StreamsApp,
  type StreamsAppConfig,
  serdes,
  stores,
  tumbling,
} from '../../src/index.js'
import {
  CHECKPOINT_FILE,
  type ChangelogRecord,
  createChangelogReader,
  keyValueStore,
  readCheckpoint,
  storedEntries,
  taskDirectory,
} from '../support/internals.js'
import {
  brokers,
  createKafka,
  describeWithKafka,
  keptOpen,
  type TestRecord,
  uniqueName,
} from './kafka.js'

const strings = { key: serdes.string, value: serdes.string }

describeWithKafka('Restore from the Changelog (at-least-once, workers: 0)', () => {
  let kafka: ReturnType<typeof createKafka>
  const apps: StreamsApp[] = []
  const directories: string[] = []
  const changelogs = new Set<string>()

  beforeEach(() => {
    kafka = createKafka()
    changelogs.clear()
  })

  afterEach(async () => {
    await Promise.allSettled(apps.splice(0).map((app) => app.stop()))
    await kafka.close()
    for (const directory of directories.splice(0)) rmSync(directory, { recursive: true })
  })

  /** A Streams App whose Changelogs are deleted after the test. */
  function app(
    config: Partial<StreamsAppConfig> & { applicationId: string },
    storeNames: string[],
  ) {
    const created = createStreamsApp({
      brokers,
      sessionTimeoutMs: 6_000,
      changelogReplicationFactor: 1,
      ...config,
    })
    for (const store of storeNames) {
      const changelog = `${config.applicationId}-${store}-changelog`
      // Restarted apps share their Changelogs: delete each once.
      if (!changelogs.has(changelog)) kafka.deleteOnClose(changelog)
      changelogs.add(changelog)
    }
    apps.push(created)
    return created
  }

  /** In-memory stores, kept open by Task so the test can read them once the app has stopped. */
  function capturingSupplier(): { supplier: StateStoreSupplier; byTask: Map<number, StateStore> } {
    const byTask = new Map<number, StateStore>()
    return {
      byTask,
      supplier: (context) => {
        const store = stores.inMemory()(context)
        byTask.set(context.task, store)
        return keptOpen(store)
      },
    }
  }

  function words(keys: string[], timestamp = 1_700_000_000_000): TestRecord[] {
    return keys.map((key, i) => ({ key, value: `${i}`, timestamp: timestamp + i }))
  }

  it('continues count() from the restored counts after a restart with empty local stores', async () => {
    const input = await kafka.createTopic('input', 2)
    const output = await kafka.createTopic('output', 2)
    const applicationId = uniqueName('restore-count')
    // In-memory stores: each run starts empty, so only Restore can bring the counts back.
    const counter = () => {
      const created = app({ applicationId }, ['counts'])
      created
        .stream(input, strings)
        .groupByKey()
        .count({ store: 'counts', storeSupplier: stores.inMemory() })
        .toStream()
        .to(output, { key: serdes.string, value: serdes.number })
      return created
    }

    const first = counter()
    await first.start()
    await kafka.produce(input, words(['a', 'b', 'a', 'c', 'a', 'b']))
    await kafka.consume(output, 6)
    await first.stop()

    const second = counter()
    await second.start()
    await kafka.produce(input, words(['a', 'b', 'c', 'd']))
    const counts = await kafka.consumeBytes(output, 10)
    expect(
      new Map(counts.map(({ key, value }) => [key?.toString(), value?.readDoubleBE()])),
    ).toEqual(
      new Map([
        ['a', 4],
        ['b', 3],
        ['c', 2],
        ['d', 1],
      ]),
    )
  })

  it('restores deletes: a key whose last Changelog record is a Tombstone stays deleted', async () => {
    const input = await kafka.createTopic('input', 1)
    const applicationId = uniqueName('restore-delete')
    const { supplier, byTask } = capturingSupplier()
    const keeper = () => {
      const created = app({ applicationId }, ['latest'])
      const store = created.store('latest', strings, { supplier })
      const processor = created.processor<string, string | null, never, never>('keeper', (ctx) => ({
        process({ key, value }) {
          if (value === null) ctx.store(store).delete(key)
          else ctx.store(store).put(key, value)
        },
      }))
      created.stream(input, strings).process(processor)
      return created
    }

    const first = keeper()
    await first.start()
    const sent: TestRecord[] = [
      { key: 'a', value: '1', timestamp: 1 },
      { key: 'b', value: '2', timestamp: 2 },
      { key: 'a', value: null, timestamp: 3 },
      { key: 'c', value: '3', timestamp: 4 },
      { key: 'c', value: null, timestamp: 5 },
    ]
    await kafka.produce(input, sent)
    await kafka.consumeBytes(`${applicationId}-latest-changelog`, sent.length)
    await first.stop()

    const second = keeper()
    await second.start()
    // The Task (and its Restore) starts with its first record.
    await kafka.produce(input, [{ key: 'd', value: '4', timestamp: 6 }])
    await kafka.consumeBytes(`${applicationId}-latest-changelog`, sent.length + 1)
    await second.stop()

    const restored = keyValueStore(byTask.get(0) as StateStore, strings, { topic: 'unused' })
    expect(restored.all()).toEqual([
      ['b', '2'],
      ['d', '4'],
    ])
  })

  it('wipes a persisted SQLite store and fully restores it when its Checkpoint is missing', async () => {
    const input = await kafka.createTopic('input', 1)
    const output = await kafka.createTopic('output', 1)
    const applicationId = uniqueName('restore-sqlite')
    const stateDir = mkdtempSync(join(tmpdir(), 'kstreams-restore-'))
    directories.push(stateDir)
    const counter = () => {
      const created = app({ applicationId, stateDir }, ['counts'])
      created
        .stream(input, strings)
        .groupByKey()
        .count({ store: 'counts' })
        .toStream()
        .to(output, { key: serdes.string, value: serdes.number })
      return created
    }

    const first = counter()
    await first.start()
    await kafka.produce(input, words(['a', 'a']))
    await kafka.consume(output, 2)
    await first.stop()
    const directory = taskDirectory(stateDir, applicationId, 0)
    expect(readCheckpoint(directory)).toBeDefined()

    // A write the Changelog never got, e.g. one made just before a crash. The Task removes its
    // Checkpoint before such a write.
    rmSync(join(directory, CHECKPOINT_FILE))
    const local = stores.sqlite()({
      name: 'counts',
      applicationId,
      task: 0,
      directory,
    })
    const topic = `${applicationId}-counts-changelog`
    const view = keyValueStore(local, { key: serdes.string, value: serdes.number }, { topic })
    view.put('a', 100)
    view.put('ghost', 7)
    local.close()

    const second = counter()
    await second.start()
    await kafka.produce(input, words(['a', 'ghost']))
    const counts = await kafka.consume(output, 4)
    expect(counts.slice(2).map(({ key }) => key)).toEqual(['a', 'ghost'])
    const read = await kafka.consumeBytes(output, 4)
    expect(read.slice(2).map(({ value }) => value?.readDoubleBE())).toEqual([3, 1])
  })

  it('restarts from the Checkpoint: replays only the Changelog records after it', async () => {
    const input = await kafka.createTopic('input', 1)
    const applicationId = uniqueName('restore-checkpoint')
    const stateDir = mkdtempSync(join(tmpdir(), 'kstreams-checkpoint-'))
    directories.push(stateDir)
    const changelog = `${applicationId}-latest-changelog`
    /** Counts the writes to the SQLite store of each run, Restore's included. */
    const keeper = () => {
      const run = { writes: 0 }
      const created = app({ applicationId, stateDir }, ['latest'])
      const store = created.store('latest', strings, {
        supplier: (context) => countingWrites(stores.sqlite()(context), run),
      })
      const processor = created.processor<string, string | null, never, never>('keeper', (ctx) => ({
        process({ key, value }) {
          if (value !== null) ctx.store(store).put(key, value)
        },
      }))
      created.stream(input, strings).process(processor)
      return { app: created, run }
    }

    const first = keeper()
    await first.app.start()
    await kafka.produce(input, words(['a', 'b', 'c', 'a']))
    await kafka.consumeBytes(changelog, 4)
    await first.app.stop()
    expect(first.run.writes).toBe(4)
    expect(readCheckpoint(taskDirectory(stateDir, applicationId, 0))).toEqual(
      new Map([['latest', 4n]]),
    )

    // Written to the Changelog after the Checkpoint, e.g. by a Worker that took the Task over.
    await kafka.produce(changelog, [
      { key: 'b', value: 'from-changelog', timestamp: 1, partition: 0 },
    ])

    const second = keeper()
    await second.app.start()
    await kafka.produce(input, [{ key: 'd', value: 'new', timestamp: 2 }])
    await kafka.consumeBytes(changelog, 6)
    await second.app.stop()

    // One record replayed (not the four before the Checkpoint, and no wipe), one processed.
    expect(second.run.writes).toBe(2)
    expect(storedEntries(stateDir, applicationId, 0, 'latest', serdes.string)).toEqual([
      ['a', '3'],
      ['b', 'from-changelog'],
      ['c', '2'],
      ['d', 'new'],
    ])
    expect(readCheckpoint(taskDirectory(stateDir, applicationId, 0))).toEqual(
      new Map([['latest', 6n]]),
    )
  })

  it('restores window stores by re-encoding the windowed Changelog keys', async () => {
    const input = await kafka.createTopic('input', 1)
    const output = await kafka.createTopic('output', 1)
    const applicationId = uniqueName('restore-window')
    const counter = () => {
      const created = app({ applicationId }, ['windowed'])
      created
        .stream(input, strings)
        .groupByKey()
        .window(tumbling('1m'))
        .count({ store: 'windowed', storeSupplier: stores.inMemory() })
        .toStream()
        .to(output, { value: serdes.number })
      return created
    }
    const start = 1_700_000_040_000 // a window start (a whole number of minutes)

    const first = counter()
    await first.start()
    await kafka.produce(input, words(['a', 'b', 'a'], start))
    await kafka.consume(output, 3)
    await first.stop()

    const second = counter()
    await second.start()
    await kafka.produce(input, words(['a', 'b'], start + 10))
    const counts = await kafka.consumeBytes(output, 5)
    expect(counts.slice(3).map(({ value }) => value?.readDoubleBE())).toEqual([3, 2])
  })
})

/** `store`, counting its puts and deletes in `run.writes`. */
function countingWrites(store: StateStore, run: { writes: number }): StateStore {
  return {
    persistent: store.persistent ?? false,
    get: (key) => store.get(key),
    put(key, value) {
      run.writes++
      store.put(key, value)
    },
    delete(key) {
      run.writes++
      store.delete(key)
    },
    clear: () => store.clear(),
    all: () => store.all(),
    range: (from, to) => store.range(from, to),
    begin: () => store.begin(),
    commit: () => store.commit(),
    rollback: () => store.rollback(),
    flush: () => store.flush?.(),
    close: () => store.close(),
  }
}

describeWithKafka('The restore consumer', () => {
  let kafka: ReturnType<typeof createKafka>

  beforeEach(() => {
    kafka = createKafka()
  })

  afterEach(async () => {
    await kafka.close()
  })

  it('skips an aborted transaction whose abort marker comes in a later fetch', async () => {
    const topic = await kafka.createTopic('changelog', 1)
    const producer = new Producer({
      clientId: uniqueName('test-txn-producer'),
      bootstrapBrokers: brokers,
      idempotent: true,
      transactionalId: uniqueName('test-txn'),
      producerId: -1n,
      producerEpoch: -1,
    })
    // 2 KB values, one record batch per send: with a 1 KB fetch limit, each fetch returns one
    // batch, so the aborted transaction spans several fetches and its marker comes last.
    const value = (text: string) => Buffer.from(text.padEnd(2_048, '.'))
    const transaction = async (records: [key: string, value: string][], commit: boolean) => {
      const tx = await producer.beginTransaction()
      for (const [key, text] of records) {
        await tx.send({ messages: [{ topic, key: Buffer.from(key), value: value(text) }] })
      }
      await (commit ? tx.commit() : tx.abort())
    }
    try {
      await transaction([['a', 'committed-1']], true)
      await transaction(
        [
          ['a', 'aborted-1'],
          ['b', 'aborted-2'],
          ['c', 'aborted-3'],
        ],
        false,
      )
      await transaction([['b', 'committed-2']], true)
      // A plain (non-transactional) write after them all.
      await kafka.produce(topic, [{ key: 'c', value: 'plain', timestamp: 1 }])
    } finally {
      await producer.close()
    }

    const reader = createChangelogReader({
      brokers,
      clientId: uniqueName('test-restore'),
      partitionMaxBytes: 1_024,
    })
    const read: ChangelogRecord[] = []
    let chunks = 0
    try {
      await reader.read(topic, 0, (records) => {
        chunks++
        read.push(...records)
      })
    } finally {
      await reader.close()
    }

    expect(chunks).toBeGreaterThan(1)
    expect(
      read.map(({ key, value }) => [
        Buffer.from(key as Uint8Array).toString(),
        Buffer.from(value as Uint8Array)
          .toString()
          .replace(/\.+$/, ''),
      ]),
    ).toEqual([
      ['a', 'committed-1'],
      ['b', 'committed-2'],
      ['c', 'plain'],
    ])
  })

  it('reads an empty Changelog partition without waiting', async () => {
    const topic = await kafka.createTopic('changelog', 2)
    const reader = createChangelogReader({ brokers, clientId: uniqueName('test-restore') })
    const read: ChangelogRecord[] = []
    try {
      await reader.read(topic, 1, (records) => read.push(...records))
    } finally {
      await reader.close()
    }
    expect(read).toEqual([])
  })
})

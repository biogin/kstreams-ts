import { describe, expect, it } from 'vitest'
import { type Serdes, type StateStore, serdes, stores, tumbling } from '../src/index.js'
import type { StoreNode } from '../src/processor-graph.js'
import { type ChangelogReader, type ChangelogRecord, restoreStores } from '../src/restore.js'
import { keyValueStore } from '../src/state-store.js'
import { type Windowed, windowedSerde, windowKeyEncoding } from '../src/windows.js'

const topic = 'app-counts-changelog'
const context = { topic }
const strings: Serdes<string, string> = { key: serdes.string, value: serdes.string }

function node(overrides: Partial<StoreNode> = {}): StoreNode {
  return {
    name: 'counts',
    serdes: strings,
    supplier: stores.inMemory(),
    changelogTopic: topic,
    ...overrides,
  }
}

function openStore(): StateStore {
  return stores.inMemory()({ name: 'counts', applicationId: 'app', task: 0, directory: undefined })
}

/** A Changelog held in memory, by `topic:partition`, read in chunks of `chunk` records. */
function fakeReader(
  changelogs: Record<string, ChangelogRecord[]>,
  chunk = 2,
): ChangelogReader & { reads: string[] } {
  const reads: string[] = []
  return {
    reads,
    async read(topic, partition, apply, from = 0n) {
      reads.push(`${topic}:${partition}${from === 0n ? '' : `@${from}`}`)
      const records = changelogs[`${topic}:${partition}`] ?? []
      if (from > BigInt(records.length)) return undefined
      for (let at = Number(from); at < records.length; at += chunk) {
        apply(records.slice(at, at + chunk))
      }
      return BigInt(records.length)
    },
  }
}

const bytes = (text: string) => serdes.string.serialize(text, context)

describe('restoreStores', () => {
  it('rebuilds a store from its Changelog partition, applying Tombstones as deletes', async () => {
    const store = openStore()
    const reader = fakeReader({
      [`${topic}:3`]: [
        { key: bytes('a'), value: bytes('1') },
        { key: bytes('b'), value: bytes('2') },
        { key: bytes('a'), value: null },
        { key: bytes('c'), value: bytes('3') },
        { key: bytes('b'), value: bytes('4') },
      ],
    })

    await restoreStores([{ node: node(), store }], 3, reader)

    expect(reader.reads).toEqual([`${topic}:3`])
    expect(keyValueStore(store, strings, context).all()).toEqual([
      ['b', '4'],
      ['c', '3'],
    ])
  })

  it('wipes what the local store had before restoring', async () => {
    const store = openStore()
    const view = keyValueStore(store, strings, context)
    view.put('stale', 'x')
    view.put('a', 'old')

    await restoreStores(
      [{ node: node(), store }],
      0,
      fakeReader({ [`${topic}:0`]: [{ key: bytes('a'), value: bytes('new') }] }),
    )

    expect(view.all()).toEqual([['a', 'new']])
  })

  it('re-encodes keys with the store key encoding, e.g. windowed keys', async () => {
    const windows = tumbling('1m')
    const keySerde = windowedSerde(serdes.string, windows)
    const windowed: Serdes<Windowed<string>, number> = { key: keySerde, value: serdes.number }
    const encoding = windowKeyEncoding(serdes.string, windows)
    const store = openStore()
    const key = (k: string, start: number) =>
      keySerde.serialize({ key: k, window: { start, end: start + 60_000 } }, context)
    const count = (n: number) => serdes.number.serialize(n, context)

    await restoreStores(
      [{ node: node({ serdes: windowed, keyEncoding: encoding }), store }],
      1,
      fakeReader({
        [`${topic}:1`]: [
          { key: key('b', 60_000), value: count(2) },
          { key: key('a', 120_000), value: count(1) },
          { key: key('a', 60_000), value: count(5) },
        ],
      }),
    )

    // Window store keys are time-first: ordered by window start, then key.
    expect(
      keyValueStore(store, windowed, context, encoding)
        .all()
        .map(([k, v]) => [k.key, k.window.start, v]),
    ).toEqual([
      ['a', 60_000, 5],
      ['b', 60_000, 2],
      ['a', 120_000, 1],
    ])
  })

  it('restores every store of the Task from its own Changelog', async () => {
    const first = openStore()
    const second = openStore()
    const other = 'app-other-changelog'

    await restoreStores(
      [
        { node: node(), store: first },
        { node: node({ name: 'other', changelogTopic: other }), store: second },
      ],
      0,
      fakeReader({
        [`${topic}:0`]: [{ key: bytes('a'), value: bytes('1') }],
        [`${other}:0`]: [{ key: bytes('z'), value: bytes('9') }],
      }),
    )

    expect(keyValueStore(first, strings, context).all()).toEqual([['a', '1']])
    expect(keyValueStore(second, strings, { topic: other }).all()).toEqual([['z', '9']])
  })

  it('resolves to the Changelog offset each store has applied up to', async () => {
    const restored = await restoreStores(
      [
        { node: node(), store: openStore() },
        {
          node: node({ name: 'empty', changelogTopic: 'app-empty-changelog' }),
          store: openStore(),
        },
      ],
      0,
      fakeReader({
        [`${topic}:0`]: [
          { key: bytes('a'), value: bytes('1') },
          { key: bytes('b'), value: null },
        ],
      }),
    )

    expect(restored).toEqual(
      new Map([
        ['counts', 2n],
        ['empty', 0n],
      ]),
    )
  })

  it('replays only the Changelog after a Checkpoint, keeping the local store', async () => {
    const store = openStore()
    const view = keyValueStore(store, strings, context)
    view.put('a', '1')
    view.put('b', '2')
    const reader = fakeReader({
      [`${topic}:0`]: [
        { key: bytes('a'), value: bytes('1') },
        { key: bytes('b'), value: bytes('2') },
        { key: bytes('a'), value: bytes('3') },
      ],
    })

    const restored = await restoreStores([{ node: node(), store, checkpoint: 2n }], 0, reader)

    expect(reader.reads).toEqual([`${topic}:0@2`])
    expect(restored.get('counts')).toBe(3n)
    expect(view.all()).toEqual([
      ['a', '3'],
      ['b', '2'],
    ])
  })

  it('wipes and fully restores a store whose Checkpoint is past the end of the Changelog', async () => {
    const store = openStore()
    const view = keyValueStore(store, strings, context)
    view.put('stale', 'x')
    const reader = fakeReader({ [`${topic}:0`]: [{ key: bytes('a'), value: bytes('1') }] })

    await restoreStores([{ node: node(), store, checkpoint: 5n }], 0, reader)

    expect(reader.reads).toEqual([`${topic}:0@5`, `${topic}:0`])
    expect(view.all()).toEqual([['a', '1']])
  })

  it('rejects a Changelog record without a key', async () => {
    await expect(
      restoreStores(
        [{ node: node(), store: openStore() }],
        0,
        fakeReader({ [`${topic}:0`]: [{ key: null, value: bytes('1') }] }),
      ),
    ).rejects.toThrow(`Changelog "${topic}" partition 0 has a record without a key`)
  })
})

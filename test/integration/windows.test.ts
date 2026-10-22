import { afterEach, beforeEach, expect, it } from 'vitest'
import {
  createStreamsApp,
  type StateStore,
  type StreamsApp,
  serdes,
  stores,
  tumbling,
  type Windowed,
} from '../../src/index.js'
import { keyValueStore, windowKeyEncoding } from '../support/internals.js'
import {
  type BytesRecord,
  brokers,
  createKafka,
  describeWithKafka,
  keptOpen,
  uniqueName,
} from './kafka.js'

const MINUTE = 60_000
const START = 1_700_000_040_000 // a window start (a whole number of minutes)
const windows = tumbling('1m', { grace: '10s' })
const windowedKey = serdes.windowed(serdes.string, windows)

/** The one-minute window of `key` that starts `minutes` after START. */
const at = (key: string, minutes: number): Windowed<string> => ({
  key,
  window: { start: START + minutes * MINUTE, end: START + (minutes + 1) * MINUTE },
})

describeWithKafka('Windowed count() on Kafka (at-least-once, workers: 0)', () => {
  let kafka: ReturnType<typeof createKafka>
  const apps: StreamsApp[] = []

  beforeEach(() => {
    kafka = createKafka()
  })

  afterEach(async () => {
    await Promise.allSettled(apps.splice(0).map((app) => app.stop()))
    await kafka.close()
  })

  it('counts per window across restarts, and drops and tombstones expired windows', async () => {
    const input = await kafka.createTopic('clicks', 1)
    const output = await kafka.createTopic('counts', 1)
    const applicationId = uniqueName('windowed-count')
    const changelog = `${applicationId}-counts-changelog`
    kafka.deleteOnClose(changelog)

    /** A counting app with an in-memory store, fully restored on each start; kept readable. */
    const counter = () => {
      let opened: StateStore | undefined
      const app = createStreamsApp({
        brokers,
        applicationId,
        sessionTimeoutMs: 6_000,
        changelogReplicationFactor: 1,
      })
      app
        .stream(input, { key: serdes.string, value: serdes.string })
        .groupByKey()
        .window(windows)
        .count({
          store: 'counts',
          storeSupplier: (context) => {
            opened = stores.inMemory()(context)
            return keptOpen(opened)
          },
        })
        .toStream()
        .to(output, { value: serdes.number })
      apps.push(app)
      const contents = () => {
        if (opened === undefined) throw new Error('The store was not opened')
        const view = keyValueStore(
          opened,
          { key: windowedKey, value: serdes.number },
          { topic: changelog },
          windowKeyEncoding(serdes.string, windows),
        )
        return view.all()
      }
      return { app, contents }
    }
    const click = (key: string, offsetMs: number) => ({
      key,
      value: 'click',
      timestamp: START + offsetMs,
    })
    /** The windowed counts of the output topic, decoded with the exported windowed key Serde. */
    const counts = async (total: number) =>
      (await kafka.consumeBytes(output, total)).map(({ key, value }) => [
        windowedKey.deserialize(key as Buffer, { topic: output }),
        value?.readDoubleBE(),
      ])

    const first = counter()
    await first.app.start()
    await kafka.produce(input, [click('a', 0), click('b', 1), click('a', 2)])
    expect(await counts(3)).toEqual([
      [at('a', 0), 1],
      [at('b', 0), 1],
      [at('a', 0), 2],
    ])
    await first.app.stop()

    // Restored from the Changelog: the count of [0, 1m) goes on from 2.
    const second = counter()
    await second.app.start()
    await kafka.produce(input, [click('a', 3)])
    expect((await counts(4)).slice(3)).toEqual([[at('a', 0), 3]])
    // Stream Time passes 1m + 10s, the end of [0, 1m) plus the Grace Period: it expires.
    await kafka.produce(input, [click('c', 2 * MINUTE + 1)])
    expect((await counts(5)).slice(4)).toEqual([[at('c', 2), 1]])
    await second.app.stop()
    expect(second.contents()).toEqual([[at('c', 2), 1]])

    // Every write of the store is in the Changelog. The expired windows are deleted once the
    // record that made them expire has been counted, so their Tombstones come after it.
    const logged = await kafka.consumeBytes(changelog, 7)
    expect(logged.map(decodeChangelog)).toEqual([
      [at('a', 0), 1],
      [at('b', 0), 1],
      [at('a', 0), 2],
      [at('a', 0), 3],
      [at('c', 2), 1],
      [at('a', 0), null],
      [at('b', 0), null],
    ])

    // Restored again, Tombstones included: only the window still kept comes back.
    const third = counter()
    await third.app.start()
    await kafka.produce(input, [click('c', 2 * MINUTE + 2)])
    expect((await counts(6)).slice(5)).toEqual([[at('c', 2), 2]])
    await third.app.stop()
    expect(third.contents()).toEqual([[at('c', 2), 2]])
    // Restore applied the Tombstones: nothing was left to expire, so no Tombstone was written again.
    const relogged = await kafka.consumeBytes(changelog, 8)
    expect(relogged.slice(7).map(decodeChangelog)).toEqual([[at('c', 2), 2]])
    expect(relogged).toHaveLength(8)

    function decodeChangelog({ key, value }: BytesRecord) {
      return [
        windowedKey.deserialize(key as Buffer, { topic: changelog }),
        value === null ? null : value.readDoubleBE(),
      ]
    }
  })
})

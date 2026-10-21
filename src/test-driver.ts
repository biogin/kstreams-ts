import type { SerializedRecord } from './processor-graph.js'
import type { StreamRecord } from './record.js'
import { deserializeNullable, type Serdes, serializeNullable } from './serde.js'
import type { KeyValueStore, NamedStore } from './state-store.js'
import { processorGraphOf, type StreamsApp } from './streams-app.js'
import { createTask } from './task.js'

/**
 * Runs a Streams App in memory, without Kafka. Records go through the app's real Serdes on the way
 * in and on the way out.
 *
 * Like a real Streams App with no error handlers, the Test Driver stops at the first error a
 * record raises (e.g. a `DeserializationError`): `pipe` rejects with it, and every later `pipe`
 * rejects.
 */
export interface TestDriver {
  /**
   * An input topic the Streams App reads. Throws if the app doesn't read `topic`. Records are
   * written with the topic's Serdes; pass `serdes` to write with your own instead (e.g. to feed
   * the app malformed bytes).
   */
  input<K = unknown, V = unknown>(
    topic: string,
    serdes?: Partial<Serdes<K, V>>,
  ): TestInputTopic<K, V>
  /**
   * An output topic the Streams App writes. Throws if the app doesn't write `topic`. Records are
   * read with the topic's Serdes; pass `serdes` to read with your own instead.
   */
  output<K = unknown, V = unknown>(
    topic: string,
    serdes?: Partial<Serdes<K, V>>,
  ): TestOutputTopic<K, V>
  /**
   * Moves the Test Driver's wall-clock time (which starts at 0) forward by `ms` and fires the
   * wall-clock timers that are then due. Records already piped are processed first. Wall-clock
   * time never affects Stream Time: only record timestamps do. Rejects if `ms` is negative, or
   * with the error a timer raised, which stops the Streams App like a failed record.
   */
  advanceWallClockTime(ms: number): Promise<void>
  /**
   * Reads a State Store of the Streams App: pass the store `app.store` returned, or its name (e.g.
   * the `store` of a `count`). Throws if the app has no such store.
   */
  store<K, V>(store: NamedStore<K, V>): TestStore<K, V>
  store<K = unknown, V = unknown>(name: string): TestStore<K, V>
}

/** Read access to a State Store in the Test Driver, decoded with the store's Serdes. */
export type TestStore<K, V> = Pick<KeyValueStore<K, V>, 'get' | 'all'>

export interface PipeOptions {
  /** Event Time of the record. Defaults to the Test Driver's wall-clock time, which starts at 0. */
  timestamp?: number
}

export interface TestInputTopic<K, V> {
  /**
   * Serializes the record with the topic's Serdes and processes it. A null value is a tombstone.
   * When every Processor is synchronous, the record is fully processed before `pipe` returns.
   * Await the returned Promise when the Streams App has async Processors: it resolves once the
   * record is fully processed and rejects if processing failed. Records piped without awaiting are
   * queued and processed one after another, never overlapping. After a failure, every later `pipe`
   * rejects too.
   */
  pipe(key: K | null, value: V | null, options?: PipeOptions): Promise<void>
}

export interface TestOutputTopic<K, V> {
  /** Removes and returns the oldest unread record. Throws if there is none. */
  read(): StreamRecord<K | null, V | null>
  /** Removes and returns every unread record, oldest first. */
  readAll(): StreamRecord<K | null, V | null>[]
}

export function createTestDriver(app: StreamsApp): TestDriver {
  const graph = processorGraphOf(app)
  let wallClockTime = 0
  const written = new Map<string, SerializedRecord[]>()
  for (const topic of graph.sinks.keys()) written.set(topic, [])
  /** The error that stopped the Streams App, if any. */
  let stoppedBy: { error: unknown } | undefined

  const task = createTask(
    graph,
    (topic, record) => {
      written.get(topic)?.push(record)
    },
    () => wallClockTime,
  )

  /** Runs work on the Task, stopping the Streams App if it fails. */
  const runOnTask = async (work: () => Promise<void>): Promise<void> => {
    if (stoppedBy !== undefined) {
      throw new Error(`Streams App "${app.applicationId}" stopped after an earlier error`, {
        cause: stoppedBy.error,
      })
    }
    try {
      await work()
    } catch (error) {
      stoppedBy ??= { error }
      throw error
    }
  }

  const store = (ref: string | NamedStore<unknown, unknown>): TestStore<unknown, unknown> => {
    const name = typeof ref === 'string' ? ref : ref.name
    if (!graph.stores.has(name)) {
      throw new Error(`Streams App "${app.applicationId}" has no State Store "${name}"`)
    }
    const { get, all } = task.store(name)
    return { get, all }
  }

  return {
    store: store as TestDriver['store'],

    input<K, V>(topic: string, serdes: Partial<Serdes<K, V>> = {}): TestInputTopic<K, V> {
      const source = graph.sources.get(topic)
      if (source === undefined) {
        throw new Error(`Streams App "${app.applicationId}" does not read topic "${topic}"`)
      }
      const key = serdes.key ?? source.serdes.key
      const value = serdes.value ?? source.serdes.value
      const context = { topic }
      return {
        pipe(k, v, options = {}) {
          return runOnTask(() =>
            task.process(topic, {
              key: serializeNullable(key, k, context),
              value: serializeNullable(value, v, context),
              timestamp: options.timestamp ?? wallClockTime,
            }),
          )
        },
      }
    },

    async advanceWallClockTime(ms) {
      if (!(ms >= 0)) throw new Error(`Wall-clock time cannot move backwards (got ${ms} ms)`)
      wallClockTime += ms
      await runOnTask(() => task.fireWallClockTimers())
    },

    output<K, V>(topic: string, serdes: Partial<Serdes<K, V>> = {}): TestOutputTopic<K, V> {
      const records = written.get(topic)
      const sink = graph.sinks.get(topic)?.[0]
      if (records === undefined || sink === undefined) {
        throw new Error(`Streams App "${app.applicationId}" does not write topic "${topic}"`)
      }
      const key = serdes.key ?? sink.serdes.key
      const value = serdes.value ?? sink.serdes.value
      const context = { topic }
      const deserialize = (record: SerializedRecord): StreamRecord<K | null, V | null> => ({
        key: deserializeNullable(key, record.key, context),
        value: deserializeNullable(value, record.value, context),
        timestamp: record.timestamp,
      })
      return {
        read() {
          const record = records.shift()
          if (record === undefined) throw new Error(`Output topic "${topic}" has no records left`)
          return deserialize(record)
        },
        readAll() {
          return records.splice(0).map(deserialize)
        },
      }
    },
  }
}

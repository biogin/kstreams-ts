import { deadLetterTopicsOf } from './error-handlers.js'
import type { OutputRecord } from './processor-graph.js'
import type { StreamRecord } from './record.js'
import {
  serdes as builtInSerdes,
  deserializeNullable,
  type Serde,
  type Serdes,
  serializeNullable,
} from './serde.js'
import type { KeyValueStore, NamedStore } from './state-store.js'
import { processorGraphOf, type StreamsApp } from './streams-app.js'
import { createTask } from './task.js'

/**
 * Runs a Streams App in memory, without Kafka. Records go through the app's real Serdes on the way
 * in and on the way out.
 *
 * Records that fail go to the Streams App's error handlers, as in a real Streams App. Without one,
 * the Test Driver stops at the first error a record raises (e.g. a `DeserializationError`): `pipe`
 * rejects with it, and every later `pipe` rejects.
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
   * An output topic the Streams App writes, or a topic its error handlers wrote to or will write
   * to (a `deadLetter` topic). Throws if the app doesn't write `topic`. Records are read with the
   * topic's Serdes; pass `serdes` to read with your own instead. A topic only error handlers write
   * has no Serdes: its records are read as bytes unless you pass some.
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
  /** The Streams App's counters so far, e.g. how many late records windows have dropped. */
  metrics(): TestDriverMetrics
}

export interface TestDriverMetrics {
  /**
   * Records dropped by windowed aggregations because they arrived after their window's Grace
   * Period. Each windowed aggregation that drops a record counts it.
   */
  lateRecordsDropped: number
}

/** Read access to a State Store in the Test Driver, decoded with the store's Serdes. */
export type TestStore<K, V> = Pick<KeyValueStore<K, V>, 'get' | 'all' | 'range'>

export interface TestDriverOptions {
  /**
   * A state directory for the Test Driver's Task (Task 0), e.g. a temporary directory. State
   * Stores without a supplier then are SQLite databases under `<stateDir>/<applicationId>/0/`,
   * as in a real Streams App. By default the Test Driver keeps them in memory, ignoring the
   * Streams App's `stateDir`, so that each Test Driver starts empty.
   */
  stateDir?: string
}

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

/** A record read from an output topic. `headers` are there only if the record has any. */
export type TestOutputRecord<K, V> = StreamRecord<K | null, V | null> & {
  headers?: Readonly<Record<string, Uint8Array>>
}

export interface TestOutputTopic<K, V> {
  /** Removes and returns the oldest unread record. Throws if there is none. */
  read(): TestOutputRecord<K, V>
  /** Removes and returns every unread record, oldest first. */
  readAll(): TestOutputRecord<K, V>[]
}

export function createTestDriver(app: StreamsApp, options: TestDriverOptions = {}): TestDriver {
  const graph = processorGraphOf(app)
  let wallClockTime = 0
  const written = new Map<string, OutputRecord[]>()
  for (const topic of graph.sinks.keys()) written.set(topic, [])
  for (const topic of deadLetterTopicsOf(graph.errorHandlers)) written.set(topic, [])
  /** The offset of the next record piped, by input topic. */
  const offsets = new Map<string, bigint>()
  /** The error that stopped the Streams App, if any. */
  let stoppedBy: { error: unknown } | undefined

  const task = createTask(
    graph,
    (topic, record) => {
      let records = written.get(topic)
      if (records === undefined) {
        // A topic only a custom error handler writes to.
        records = []
        written.set(topic, records)
      }
      records.push(record)
    },
    () => wallClockTime,
    { stateDir: options.stateDir },
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
    const { get, all, range } = task.store(name)
    return { get, all, range }
  }

  return {
    store: store as TestDriver['store'],

    metrics() {
      return task.metrics()
    },

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
          return runOnTask(() => {
            const offset = offsets.get(topic) ?? 0n
            offsets.set(topic, offset + 1n)
            return task.process(
              topic,
              {
                key: serializeNullable(key, k, context),
                value: serializeNullable(value, v, context),
                timestamp: options.timestamp ?? wallClockTime,
              },
              offset,
            )
          })
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
      if (records === undefined) {
        throw new Error(`Streams App "${app.applicationId}" does not write topic "${topic}"`)
      }
      // Without a sink, only error handlers write the topic: bytes, unless told otherwise.
      const topicSerdes = (graph.sinks.get(topic)?.[0]?.serdes ?? {
        key: builtInSerdes.bytes,
        value: builtInSerdes.bytes,
      }) as Serdes<unknown, unknown>
      const key = (serdes.key ?? topicSerdes.key) as Serde<K>
      const value = (serdes.value ?? topicSerdes.value) as Serde<V>
      const context = { topic }
      const deserialize = ({ headers, ...record }: OutputRecord): TestOutputRecord<K, V> => ({
        key: deserializeNullable(key, record.key, context),
        value: deserializeNullable(value, record.value, context),
        timestamp: record.timestamp,
        ...(headers === undefined ? {} : { headers }),
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

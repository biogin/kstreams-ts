import type { SerializedRecord } from './processor-graph.js'
import type { StreamRecord } from './record.js'
import { deserializeNullable, type Serdes, serializeNullable } from './serde.js'
import { processorGraphOf, type StreamsApp } from './streams-app.js'
import { createTask } from './task.js'

/**
 * Runs a Streams App in memory, without Kafka. Records go through the app's real Serdes on the way
 * in and on the way out.
 *
 * Like a real Streams App with no error handlers, the Test Driver stops at the first error a
 * record raises (e.g. a `DeserializationError`): `pipe` rethrows it, and every later `pipe`
 * throws.
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
}

export interface PipeOptions {
  /** Event Time of the record. Defaults to the Test Driver's wall-clock time, which starts at 0. */
  timestamp?: number
}

export interface TestInputTopic<K, V> {
  /**
   * Serializes the record with the topic's Serdes and processes it to completion. A null value is
   * a tombstone.
   */
  pipe(key: K | null, value: V | null, options?: PipeOptions): void
}

export interface TestOutputTopic<K, V> {
  /** Removes and returns the oldest unread record. Throws if there is none. */
  read(): StreamRecord<K | null, V | null>
  /** Removes and returns every unread record, oldest first. */
  readAll(): StreamRecord<K | null, V | null>[]
}

export function createTestDriver(app: StreamsApp): TestDriver {
  const graph = processorGraphOf(app)
  const wallClockTime = 0
  const written = new Map<string, SerializedRecord[]>()
  for (const topic of graph.sinks.keys()) written.set(topic, [])
  /** The error that stopped the Streams App, if any. */
  let stoppedBy: { error: unknown } | undefined

  const task = createTask(graph, (topic, record) => {
    written.get(topic)?.push(record)
  })

  return {
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
          if (stoppedBy !== undefined) {
            throw new Error(`Streams App "${app.applicationId}" stopped after an earlier error`, {
              cause: stoppedBy.error,
            })
          }
          const record = {
            key: serializeNullable(key, k, context),
            value: serializeNullable(value, v, context),
            timestamp: options.timestamp ?? wallClockTime,
          }
          try {
            task.process(topic, record)
          } catch (error) {
            stoppedBy = { error }
            throw error
          }
        },
      }
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

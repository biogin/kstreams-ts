import type { SerializedRecord } from './processor-graph.js'
import type { StreamRecord } from './record.js'
import { processorGraphOf, type StreamsApp } from './streams-app.js'
import { createTask } from './task.js'

/**
 * Runs a Streams App in memory, without Kafka. Records go through the app's real Serdes on the way
 * in and on the way out.
 */
export interface TestDriver {
  /** An input topic the Streams App reads. Throws if the app doesn't read `topic`. */
  input<K = unknown, V = unknown>(topic: string): TestInputTopic<K, V>
  /** An output topic the Streams App writes. Throws if the app doesn't write `topic`. */
  output<K = unknown, V = unknown>(topic: string): TestOutputTopic<K, V>
}

export interface PipeOptions {
  /** Event Time of the record. Defaults to the Test Driver's wall-clock time, which starts at 0. */
  timestamp?: number
}

export interface TestInputTopic<K, V> {
  /** Serializes the record with the topic's Serdes and processes it to completion. */
  pipe(key: K, value: V, options?: PipeOptions): void
}

export interface TestOutputTopic<K, V> {
  /** Removes and returns the oldest unread record. Throws if there is none. */
  read(): StreamRecord<K, V>
  /** Removes and returns every unread record, oldest first. */
  readAll(): StreamRecord<K, V>[]
}

export function createTestDriver(app: StreamsApp): TestDriver {
  const graph = processorGraphOf(app)
  const wallClockTime = 0
  const written = new Map<string, SerializedRecord[]>()
  for (const topic of graph.sinks.keys()) written.set(topic, [])

  const task = createTask(graph, (topic, record) => {
    written.get(topic)?.push(record)
  })

  return {
    input<K, V>(topic: string): TestInputTopic<K, V> {
      const source = graph.sources.get(topic)
      if (source === undefined) {
        throw new Error(`Streams App "${app.applicationId}" does not read topic "${topic}"`)
      }
      return {
        pipe(key, value, options = {}) {
          task.process(topic, {
            key: source.serdes.key.serialize(key),
            value: source.serdes.value.serialize(value),
            timestamp: options.timestamp ?? wallClockTime,
          })
        },
      }
    },

    output<K, V>(topic: string): TestOutputTopic<K, V> {
      const records = written.get(topic)
      const sink = graph.sinks.get(topic)?.[0]
      if (records === undefined || sink === undefined) {
        throw new Error(`Streams App "${app.applicationId}" does not write topic "${topic}"`)
      }
      const deserialize = (record: SerializedRecord): StreamRecord<K, V> => ({
        key: sink.serdes.key.deserialize(record.key),
        value: sink.serdes.value.deserialize(record.value),
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

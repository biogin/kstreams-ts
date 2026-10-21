// Internal: runs a Processor graph for one Task. Shared by the Test Driver and the Kafka runtime.

import type { ProcessorGraph, SerializedRecord } from './processor-graph.js'
import type { StreamRecord } from './record.js'

/** Receives every record a sink writes, already serialized with the sink's Serdes. */
export type RecordCollector = (topic: string, record: SerializedRecord) => void

/** Delivers a deserialized record to one graph node. */
type Deliver = (record: StreamRecord<unknown, unknown>) => void

export interface Task {
  /** Processes one record read from an input topic, synchronously, to completion. */
  process(topic: string, record: SerializedRecord): void
}

export function createTask(graph: ProcessorGraph, collect: RecordCollector): Task {
  const deliverTo = new Map<string, Deliver>()
  const forwardTo = (children: readonly string[]): Deliver => {
    return (record) => {
      for (const child of children) deliverTo.get(child)?.(record)
    }
  }

  for (const node of graph.nodes.values()) {
    switch (node.kind) {
      case 'source':
        deliverTo.set(node.name, forwardTo(node.children))
        break
      case 'processor': {
        const processor = node.supplier({ forward: forwardTo(node.children) })
        deliverTo.set(node.name, (record) => processor.process(record))
        break
      }
      case 'sink':
        deliverTo.set(node.name, (record) =>
          collect(node.topic, {
            key: node.serdes.key.serialize(record.key),
            value: node.serdes.value.serialize(record.value),
            timestamp: record.timestamp,
          }),
        )
        break
    }
  }

  return {
    process(topic, record) {
      const source = graph.sources.get(topic)
      if (source === undefined) throw new Error(`Topic "${topic}" is not an input of this Task`)
      deliverTo.get(source.name)?.({
        key: source.serdes.key.deserialize(record.key),
        value: source.serdes.value.deserialize(record.value),
        timestamp: record.timestamp,
      })
    },
  }
}

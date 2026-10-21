// Internal: runs a Processor graph for one Task. Shared by the Test Driver and the Kafka runtime.

import type { ProcessorContext, ProcessorGraph, SerializedRecord } from './processor-graph.js'
import type { StreamRecord } from './record.js'

/** Receives every record a sink writes, already serialized with the sink's Serdes. */
export type RecordCollector = (topic: string, record: SerializedRecord) => void

/** Delivers a deserialized record to one graph node. */
type Deliver = (record: StreamRecord<unknown, unknown>) => void

export interface Task {
  /**
   * Processes one record read from an input topic. Records are processed one after another: a
   * record only starts once every Processor (async ones included) has finished with the one before.
   * Work that is all synchronous completes before `process` returns. The Promise resolves once the
   * record is fully processed, and rejects if processing failed. A failed Task rejects every later
   * record with the same error (fail-fast).
   */
  process(topic: string, record: SerializedRecord): Promise<void>
}

export function createTask(graph: ProcessorGraph, collect: RecordCollector): Task {
  // Async Processor calls of the current record that have not settled yet.
  const inFlight = new Set<Promise<void>>()
  const track = (work: Promise<void>): void => {
    inFlight.add(work)
    const settled = () => inFlight.delete(work)
    work.then(settled, settled)
  }

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
        const processor = node.supplier(createContext(forwardTo(node.children)))
        // While this Processor is busy with an async call, its next records queue behind it.
        let busy: Promise<void> | undefined
        const queue = (work: Promise<void>): void => {
          const done = work.then(() => {
            if (busy === done) busy = undefined
          })
          busy = done
          track(done)
        }
        deliverTo.set(node.name, (record) => {
          if (busy !== undefined) return queue(busy.then(() => processor.process(record)))
          const result = processor.process(record)
          if (isThenable(result)) queue(Promise.resolve(result))
        })
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

  /** Runs one record. Returns a Promise only if async work is still going on. */
  const run = (topic: string, record: SerializedRecord): Promise<void> | undefined => {
    const source = graph.sources.get(topic)
    if (source === undefined) throw new Error(`Topic "${topic}" is not an input of this Task`)
    deliverTo.get(source.name)?.({
      key: source.serdes.key.deserialize(record.key),
      value: source.serdes.value.deserialize(record.value),
      timestamp: record.timestamp,
    })
    return inFlight.size === 0 ? undefined : drain()
  }

  const drain = async (): Promise<void> => {
    // Async Processors may forward to more async Processors, so wait until nothing is left.
    while (inFlight.size > 0) await Promise.all(inFlight)
  }

  let failure: { error: unknown } | undefined
  // The last queued record that is still being processed.
  let pending: Promise<void> | undefined

  const settle = (work: Promise<void>): Promise<void> => {
    const done = work.then(
      () => {
        if (pending === done) pending = undefined
      },
      (error: unknown) => {
        failure ??= { error }
        if (pending === done) pending = undefined
        throw error
      },
    )
    pending = done
    return done
  }

  return {
    process(topic, record) {
      if (failure !== undefined) return Promise.reject(failure.error)
      if (pending !== undefined) return settle(pending.then(() => run(topic, record)))
      let work: Promise<void> | undefined
      try {
        work = run(topic, record)
      } catch (error) {
        failure = { error }
        return Promise.reject(error)
      }
      return work === undefined ? Promise.resolve() : settle(work)
    },
  }
}

function createContext(forward: Deliver): ProcessorContext<unknown, unknown> {
  return {
    forward,
    store() {
      throw new Error('ctx.store is not implemented yet')
    },
    schedule() {
      throw new Error('ctx.schedule is not implemented yet')
    },
  }
}

function isThenable(value: unknown): value is PromiseLike<unknown> {
  return typeof (value as PromiseLike<unknown> | undefined)?.then === 'function'
}

// Internal: runs a Processor graph for one Task. Shared by the Test Driver and the Kafka runtime.

import type {
  ProcessorContext,
  ProcessorGraph,
  SerializedRecord,
  Timer,
  TimerCallback,
  TimerType,
} from './processor-graph.js'
import type { StreamRecord } from './record.js'
import {
  DeserializationError,
  deserializeNullable,
  type Serde,
  serializeNullable,
} from './serde.js'
import { type KeyValueStore, keyValueStore, type NamedStore } from './state-store.js'

/** Receives every record a sink writes, already serialized with the sink's Serdes. */
export type RecordCollector = (topic: string, record: SerializedRecord) => void

/** Delivers a deserialized record to one graph node. */
type Deliver = (record: StreamRecord<unknown, unknown>) => void

export interface Task {
  /**
   * Processes one record read from an input topic. Records are processed one after another: a
   * record only starts once every Processor (async ones included) has finished with the one before.
   * Work that is all synchronous completes before `process` returns. The Promise resolves once the
   * record is fully processed, and rejects if processing failed (e.g. with a
   * {@link DeserializationError} if the topic's Serdes can't read it). A failed Task rejects every
   * later record with the same error (fail-fast).
   *
   * The record's Event Time advances the Task's Stream Time (which never moves back). Once the
   * record is fully processed, the Stream Time timers that are due fire.
   */
  process(topic: string, record: SerializedRecord): Promise<void>
  /**
   * Fires the wall-clock timers that are due at the current wall-clock time. The runtime calls
   * this from its loop; the Test Driver calls it when wall-clock time is advanced. It is queued
   * behind the records already given to `process` and runs like one, with the same fail-fast
   * behavior.
   */
  fireWallClockTimers(): Promise<void>
  /** This Task's State Store named `name`. Throws if the graph has no such store. */
  store(name: string): KeyValueStore<unknown, unknown>
}

/** A timer scheduled by a Processor. */
interface ScheduledTimer {
  type: TimerType
  intervalMs: number
  callback: TimerCallback
  /** When it fires next. Unknown for a Stream Time timer until the Task has seen a record. */
  due: number | undefined
}

/**
 * @param wallClockTime the current wall-clock time in milliseconds since the epoch, read when a
 * wall-clock timer is scheduled and when wall-clock timers are fired.
 */
export function createTask(
  graph: ProcessorGraph,
  collect: RecordCollector,
  wallClockTime: () => number = Date.now,
): Task {
  // Async Processor calls of the current record that have not settled yet.
  const inFlight = new Set<Promise<void>>()
  const track = (work: Promise<void>): void => {
    inFlight.add(work)
    const settled = () => inFlight.delete(work)
    work.then(settled, settled)
  }

  const settleInFlight = (): Promise<void> | undefined =>
    inFlight.size === 0 ? undefined : drain()

  /** The highest Event Time seen so far. Unknown until the first record. */
  let streamTime: number | undefined
  const timers: ScheduledTimer[] = []

  const schedule = (intervalMs: number, type: TimerType, callback: TimerCallback): Timer => {
    if (!(intervalMs > 0 && Number.isFinite(intervalMs))) {
      throw new Error(`Timer interval must be a positive number of milliseconds, got ${intervalMs}`)
    }
    if (type !== 'stream-time' && type !== 'wall-clock-time') {
      throw new Error(`Unknown timer type ${JSON.stringify(type)}`)
    }
    const now = type === 'stream-time' ? streamTime : wallClockTime()
    const timer: ScheduledTimer = {
      type,
      intervalMs,
      callback,
      due: now === undefined ? undefined : now + intervalMs,
    }
    timers.push(timer)
    return {
      cancel() {
        const index = timers.indexOf(timer)
        if (index !== -1) timers.splice(index, 1)
      },
    }
  }

  const advanceStreamTime = (timestamp: number): void => {
    if (streamTime === undefined) {
      for (const timer of timers) {
        if (timer.type === 'stream-time') timer.due ??= timestamp + timer.intervalMs
      }
    }
    if (streamTime === undefined || timestamp > streamTime) streamTime = timestamp
  }

  /**
   * Fires the timers of `type` that are due at `now`, earliest first, one after another (async
   * callbacks and the work they forward included). Each fires once, however many intervals have
   * passed. Returns a Promise only if async work is still going on.
   */
  const fireDue = (type: TimerType, now: number): Promise<void> | undefined => {
    const due = timers
      .filter((timer) => timer.type === type && timer.due !== undefined && timer.due <= now)
      .sort((a, b) => (a.due ?? now) - (b.due ?? now))
    const fireFrom = (first: number): Promise<void> | undefined => {
      for (let i = first; i < due.length; i++) {
        const timer = due[i] as ScheduledTimer
        if (!timers.includes(timer)) continue // cancelled by a timer that fired before it
        const missed = Math.floor((now - (timer.due ?? now)) / timer.intervalMs)
        timer.due = (timer.due ?? now) + (missed + 1) * timer.intervalMs
        const result = timer.callback(now)
        if (isThenable(result)) track(Promise.resolve(result))
        const work = settleInFlight()
        if (work !== undefined) return work.then(() => fireFrom(i + 1))
      }
      return undefined
    }
    return fireFrom(0)
  }

  const stores = new Map<string, KeyValueStore<unknown, unknown>>()
  for (const { name, serdes, supplier, changelogTopic } of graph.stores.values()) {
    stores.set(name, keyValueStore(supplier({ name }), serdes, { topic: changelogTopic }))
  }
  const store = (ref: string | NamedStore<unknown, unknown>): KeyValueStore<unknown, unknown> => {
    const name = typeof ref === 'string' ? ref : ref.name
    const found = stores.get(name)
    if (found === undefined) throw new Error(`Unknown State Store "${name}"`)
    return found
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
        const processor = node.supplier(createContext(forwardTo(node.children), store, schedule))
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
      case 'sink': {
        const context = { topic: node.topic }
        deliverTo.set(node.name, (record) =>
          collect(node.topic, {
            key: serializeNullable(node.serdes.key, record.key, context),
            value: serializeNullable(node.serdes.value, record.value, context),
            timestamp: record.timestamp,
          }),
        )
        break
      }
    }
  }

  /** Runs one record. Returns a Promise only if async work is still going on. */
  const run = (topic: string, record: SerializedRecord): Promise<void> | undefined => {
    const source = graph.sources.get(topic)
    if (source === undefined) throw new Error(`Topic "${topic}" is not an input of this Task`)
    const read = <T>(serde: Serde<T>, part: 'key' | 'value') => {
      try {
        return deserializeNullable(serde, record[part], { topic })
      } catch (cause) {
        throw new DeserializationError(topic, part, record.timestamp, { cause })
      }
    }
    const key = read(source.serdes.key, 'key')
    const value = read(source.serdes.value, 'value')
    advanceStreamTime(record.timestamp)
    deliverTo.get(source.name)?.({ key, value, timestamp: record.timestamp })
    return andThen(settleInFlight(), () => fireDue('stream-time', streamTime ?? record.timestamp))
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

  /** Runs `job` once everything queued before it is done, so records and timers never overlap. */
  const enqueue = (job: () => Promise<void> | undefined): Promise<void> => {
    if (failure !== undefined) return Promise.reject(failure.error)
    if (pending !== undefined) return settle(pending.then(job))
    let work: Promise<void> | undefined
    try {
      work = job()
    } catch (error) {
      failure = { error }
      return Promise.reject(error)
    }
    return work === undefined ? Promise.resolve() : settle(work)
  }

  return {
    process(topic, record) {
      return enqueue(() => run(topic, record))
    },
    fireWallClockTimers() {
      return enqueue(() => fireDue('wall-clock-time', wallClockTime()))
    },
    store,
  }
}

function createContext(
  forward: Deliver,
  store: (ref: string | NamedStore<unknown, unknown>) => KeyValueStore<unknown, unknown>,
  schedule: ProcessorContext<unknown, unknown>['schedule'],
): ProcessorContext<unknown, unknown> {
  return {
    forward,
    store: store as ProcessorContext<unknown, unknown>['store'],
    schedule,
  }
}

/** Runs `next` after `work`, staying synchronous when there is no `work`. */
function andThen(
  work: Promise<void> | undefined,
  next: () => Promise<void> | undefined,
): Promise<void> | undefined {
  return work === undefined ? next() : work.then(next)
}

function isThenable(value: unknown): value is PromiseLike<unknown> {
  return typeof (value as PromiseLike<unknown> | undefined)?.then === 'function'
}

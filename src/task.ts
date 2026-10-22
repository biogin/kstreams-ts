// Internal: runs a Processor graph for one Task. Shared by the Test Driver and the Kafka runtime.

import { TaskCheckpoint } from './checkpoint.js'
import { type FailedRecord, handleError } from './error-handlers.js'
import { noTaskCounters, type TaskCounters } from './observability.js'
import type {
  OutputRecord,
  ProcessorContext,
  ProcessorGraph,
  SerializedRecord,
  Timer,
  TimerCallback,
  TimerType,
} from './processor-graph.js'
import type { StreamRecord } from './record.js'
import type { RestoringStore } from './restore.js'
import {
  DeserializationError,
  deserializeNullable,
  type Serde,
  serializeNullable,
} from './serde.js'
import {
  type KeyValueStore,
  keyValueStore,
  type NamedStore,
  type StateStore,
  taskDirectory,
} from './state-store.js'

/**
 * Receives every record a sink writes, already serialized with the sink's Serdes, and the records
 * error handlers write (e.g. to a dead-letter topic).
 */
export type RecordCollector = (topic: string, record: OutputRecord) => void

/** Delivers a deserialized record to one graph node. */
type Deliver = (record: StreamRecord<unknown, unknown>) => void

export interface TaskOptions {
  /** The Task's id: its partition number. Defaults to 0. */
  task?: number
  /** The configured state directory. Without one, State Stores get no directory. */
  stateDir?: string | undefined
  /**
   * Receives every State Store write as a record for the store's Changelog topic (the Task's
   * partition of it), timestamped with the wall-clock time. Without one, writes aren't logged.
   */
  changelog?: RecordCollector | undefined
  /**
   * Restores the Task's State Stores, as opened, before it processes anything: records and timers
   * given to the Task meanwhile wait for it. It resolves to the Changelog offset each store has
   * applied up to, by store name. A failed Restore fails the Task. Without one, stores start as
   * their suppliers open them.
   *
   * With a state directory, the Task also keeps a Checkpoint of its persistent stores there, and
   * gives Restore each one's Checkpoint (see checkpoint.ts).
   */
  restore?: ((stores: RestoringStore[]) => Promise<ReadonlyMap<string, bigint>>) | undefined
  /**
   * Under exactly-once: the Task's State Store writes go into store transactions (ADR-0002). One
   * begins on every store before the Task's first record or timer after Restore or the last
   * commit, and stays open until `commit` (once the Kafka transaction with the writes' Changelog
   * records has committed) or `rollback` (when it aborted). Committing a store transaction then
   * doesn't make the Checkpoint unclean: the Changelog already has every write it adds.
   */
  storeTransactions?: boolean | undefined
}

/** Changelog offsets by Changelog topic, for one Task's partition of each. */
export type ChangelogOffsets = ReadonlyMap<string, bigint>

export interface Task {
  /**
   * Processes one record read from an input topic. Records are processed one after another: a
   * record only starts once every Processor (async ones included) has finished with the one before.
   * Work that is all synchronous completes before `process` returns. The Promise resolves once the
   * record is fully processed, and rejects if processing failed (e.g. with a
   * {@link DeserializationError} if the topic's Serdes can't read it) and the graph's error
   * handler for it didn't skip the record. A failed Task rejects every later record with the same
   * error (fail-fast). `offset` is the record's offset in its partition, for error handlers.
   *
   * The record's Event Time advances the Task's Stream Time (which never moves back). Once the
   * record is fully processed, the Stream Time timers that are due fire.
   */
  process(topic: string, record: SerializedRecord, offset: bigint): Promise<void>
  /**
   * Fires the wall-clock timers that are due at the current wall-clock time. The runtime calls
   * this from its loop; the Test Driver calls it when wall-clock time is advanced. It is queued
   * behind the records already given to `process` and runs like one, with the same fail-fast
   * behavior.
   */
  fireWallClockTimers(): Promise<void>
  /** This Task's State Store named `name`. Throws if the graph has no such store. */
  store(name: string): KeyValueStore<unknown, unknown>
  /** The Task's counters so far. */
  metrics(): TaskMetrics
  /**
   * Resolves once the Task's State Stores are restored (right away without Restore). Rejects if
   * Restore failed.
   */
  restored(): Promise<void>
  /**
   * Commits the open store transactions (see {@link TaskOptions.storeTransactions}), if any.
   * Call it only once the Changelog records of their writes are committed, and when no record is
   * being processed.
   */
  commit(): void
  /**
   * Rolls back the open store transactions, if any: every store write since the last commit is
   * undone. The Task's other state (Stream Time, timers) is not; a Task whose batch was aborted
   * should be closed.
   */
  rollback(): void
  /**
   * Writes the Checkpoint of the Task's persistent State Stores, if they were written since the
   * last one. Call it only once the Changelog records of every store write so far are
   * acknowledged, with `committed` holding the offset after the last of them in each Changelog
   * written to, and when no record is being processed nor any store transaction open.
   */
  checkpoint(committed: ChangelogOffsets): void
  /** Closes the Task's State Stores. Without a Checkpoint first, they are unclean. */
  close(): void
}

/** The Task's counters (see observability.ts). */
export type TaskMetrics = TaskCounters

/** Internal: what the DSL's own Processors get from their Task besides the public context. */
export interface TaskInternals {
  /** The Task's Stream Time, including the record being processed. Unknown before any record. */
  streamTime(): number | undefined
  /** Counts a record dropped because it arrived after its window's Grace Period. */
  recordLateDropped(): void
}

const internals = new WeakMap<object, TaskInternals>()

/** Internal: the Task behind a Processor context the Task created. */
export function taskInternals(context: ProcessorContext<unknown, unknown>): TaskInternals {
  const found = internals.get(context)
  if (found === undefined) throw new TypeError('Not a Processor context created by a Task')
  return found
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
  { task = 0, stateDir, changelog, restore, storeTransactions }: TaskOptions = {},
): Task {
  // Async Processor calls of the current record that have not settled yet.
  const inFlight = new Set<Promise<void>>()
  /** The first error an async call of the current record raised. */
  let asyncFailure: { error: unknown } | undefined
  const track = (work: Promise<void>): void => {
    inFlight.add(work)
    work.then(
      () => inFlight.delete(work),
      (error: unknown) => {
        inFlight.delete(work)
        asyncFailure ??= { error }
      },
    )
  }

  /**
   * Waits until no async call is left, then throws the first error one raised. Returns a Promise
   * only if there is anything to wait for.
   */
  const settleInFlight = (): Promise<void> | undefined =>
    inFlight.size === 0 && asyncFailure === undefined ? undefined : drain()

  /** Waits until no async call is left, ignoring their errors: a record already failed. */
  const quiesce = (): Promise<void> | undefined => {
    const work = settleInFlight()
    if (work === undefined) return undefined
    return work.catch(() => {})
  }

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
  const opened: RestoringStore[] = []
  const directory =
    stateDir === undefined ? undefined : taskDirectory(stateDir, graph.applicationId, task)
  /** The Checkpoint of the persistent stores, which only a Task that is restored keeps. */
  let checkpoint: TaskCheckpoint | undefined
  const transactional = storeTransactions === true
  for (const node of graph.stores.values()) {
    const { name, serdes, supplier, changelogTopic, keyEncoding } = node
    let store = supplier({ name, applicationId: graph.applicationId, task, directory })
    let restoreFrom: bigint | undefined
    if (directory !== undefined && restore !== undefined && store.persistent === true) {
      checkpoint ??= new TaskCheckpoint(directory, { changelogFirst: transactional })
      restoreFrom = checkpoint.offset(name)
      store = checkpoint.track(name, store)
    }
    opened.push({ node, store, checkpoint: restoreFrom })
    const log =
      changelog &&
      ((key: Uint8Array, value: Uint8Array | null) =>
        changelog(changelogTopic, { key, value, timestamp: wallClockTime() }))
    stores.set(name, keyValueStore(store, serdes, { topic: changelogTopic }, keyEncoding, log))
  }
  const store = (ref: string | NamedStore<unknown, unknown>): KeyValueStore<unknown, unknown> => {
    const name = typeof ref === 'string' ? ref : ref.name
    const found = stores.get(name)
    if (found === undefined) throw new Error(`Unknown State Store "${name}"`)
    return found
  }

  const metrics: TaskMetrics = noTaskCounters()
  const taskSide: TaskInternals = {
    streamTime: () => streamTime,
    recordLateDropped() {
      metrics.lateRecordsDropped++
    },
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
        const context = createContext(forwardTo(node.children), store, schedule)
        internals.set(context, taskSide)
        const processor = node.supplier(context)
        // While this Processor is busy with an async call, its next records queue behind it.
        let busy: Promise<void> | undefined
        const queue = (work: Promise<void>): void => {
          const idle = () => {
            if (busy === done) busy = undefined
          }
          const done = work.then(idle, (error: unknown) => {
            idle()
            throw error
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

  /**
   * Runs one record. Returns a Promise only if async work is still going on. A record that fails
   * goes to the graph's error handler for its kind, which skips it or fails the record.
   */
  const run = (
    topic: string,
    record: SerializedRecord,
    offset: bigint,
  ): Promise<void> | undefined => {
    const source = graph.sources.get(topic)
    if (source === undefined) throw new Error(`Topic "${topic}" is not an input of this Task`)
    const failed: FailedRecord = { topic, partition: task, offset, ...record }
    const read = <T>(serde: Serde<T>, part: 'key' | 'value') => {
      try {
        return deserializeNullable(serde, record[part], { topic })
      } catch (cause) {
        throw new DeserializationError(topic, part, record.timestamp, { cause })
      }
    }
    let key: unknown
    let value: unknown
    try {
      key = read(source.serdes.key, 'key')
      value = read(source.serdes.value, 'value')
    } catch (error) {
      // A skipped record doesn't advance Stream Time.
      handleError(graph.errorHandlers, 'deserialization', error, failed, collect)
      metrics.recordsDropped.deserialization++
      return undefined
    }
    advanceStreamTime(record.timestamp)
    const onError = (error: unknown) =>
      // The record's async work still going on finishes before the next record starts.
      andThen(quiesce(), () => {
        handleError(graph.errorHandlers, 'processing', error, failed, collect)
        metrics.recordsDropped.processing++
        return undefined
      })
    let processed: Promise<void> | undefined
    try {
      deliverTo.get(source.name)?.({ key, value, timestamp: record.timestamp })
      processed = settleInFlight()?.catch(onError)
    } catch (error) {
      processed = onError(error)
    }
    // Timers are not a record's processing: their errors always fail the Task.
    return andThen(processed, () => fireDue('stream-time', streamTime ?? record.timestamp))
  }

  const drain = async (): Promise<void> => {
    // Async Processors may forward to more async Processors, so wait until nothing is left.
    while (inFlight.size > 0) await Promise.allSettled(inFlight)
    const failed = asyncFailure
    asyncFailure = undefined
    if (failed !== undefined) throw failed.error
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

  // The first job: everything else queues behind it. Its failure fails the Task's later work.
  let restored: Promise<void> = Promise.resolve()
  if (restore !== undefined && opened.length > 0) {
    restored = enqueue(async () => {
      const applied = await restore(opened)
      for (const [name, offset] of applied) checkpoint?.advance(name, offset)
      // Restore's own writes are in the Changelog already.
      checkpoint?.write()
    })
    restored.catch(() => {})
  }

  /** Whether the stores have an open store transaction. */
  let inTransaction = false
  /** Runs `job` in the open store transaction, beginning one first if needed. */
  const transacted = (job: () => Promise<void> | undefined) => (): Promise<void> | undefined => {
    if (transactional && !inTransaction && opened.length > 0) {
      for (const { store } of opened) store.begin()
      inTransaction = true
    }
    return job()
  }
  /** Ends the open store transaction of every store, with `end`. */
  const endTransaction = (end: (store: StateStore) => void): void => {
    if (!inTransaction) return
    inTransaction = false
    let first: { error: unknown } | undefined
    for (const { store } of opened) {
      try {
        end(store)
      } catch (error) {
        first ??= { error }
      }
    }
    if (first !== undefined) throw first.error
  }

  return {
    process(topic, record, offset) {
      return enqueue(
        transacted(() =>
          andThen(run(topic, record, offset), () => {
            metrics.recordsProcessed++
            return undefined
          }),
        ),
      )
    },
    fireWallClockTimers() {
      return enqueue(transacted(() => fireDue('wall-clock-time', wallClockTime())))
    },
    store,
    metrics: () => ({ ...metrics, recordsDropped: { ...metrics.recordsDropped } }),
    restored: () => restored,
    commit() {
      if (pending !== undefined) {
        throw new Error('Cannot commit store transactions while records are processed')
      }
      endTransaction((store) => store.commit())
    },
    rollback() {
      endTransaction((store) => store.rollback())
    },
    checkpoint(committed) {
      if (checkpoint === undefined || failure !== undefined) return
      if (pending !== undefined)
        throw new Error('Cannot write a Checkpoint while records are processed')
      if (inTransaction) {
        throw new Error('Cannot write a Checkpoint while a store transaction is open')
      }
      for (const { node } of opened) {
        const offset = committed.get(node.changelogTopic)
        if (offset !== undefined) checkpoint.advance(node.name, offset)
      }
      checkpoint.write()
    },
    close() {
      for (const { store } of opened) store.close()
    },
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

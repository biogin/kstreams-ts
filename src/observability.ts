// Observability: the Streams App's typed events and the metrics it gives a metrics sink. Workers,
// including those in worker threads, report their state, events and metrics to the Streams App
// in the main thread (see `WorkerReport`), which sums the states up into the Application State and
// passes events on to listeners and metrics to the sink.

import type { ErrorHandlerKind } from './error-handlers.js'

/**
 * The lifecycle phase a Streams App reports: created → rebalancing ⇄ restoring ⇄ running →
 * stopping → stopped, or error from any state.
 *
 * - `rebalancing`: some Worker is joining the consumer group or taking part in a rebalance (from
 *   `start()` until every Worker has joined, and whenever the group rebalances).
 * - `restoring`: no Worker is rebalancing, and some Worker has a Task with State Stores that is
 *   starting: waiting for its previous owner to let it go, or restoring its stores from their
 *   Changelogs. A Task starts when its first record arrives. The Worker's other Tasks keep
 *   processing meanwhile.
 * - `running`: every Worker has joined the group and none is rebalancing or restoring.
 * - `stopping`, `stopped`: after `stop()`. `error`: a Worker failed, and the app stopped the others.
 */
export type ApplicationState =
  | 'created'
  | 'rebalancing'
  | 'restoring'
  | 'running'
  | 'stopping'
  | 'stopped'
  | 'error'

/** A partition of a topic. */
export interface TopicPartition {
  topic: string
  partition: number
}

/** The Application State changed. */
export interface StateChangeEvent {
  state: ApplicationState
  previous: ApplicationState
}

/**
 * The error that stopped the Streams App: the first one a Worker reported (the root cause), not
 * the errors its stopping caused. An error from a worker thread keeps its class for the library's
 * errors and JavaScript's built-in ones, and its own properties. Emitted once, as the app goes to
 * `error`; `stop()` rejects with the same error.
 */
export interface StreamsAppErrorEvent {
  error: unknown
}

/**
 * A Worker took part in a completed rebalance round. A cooperative rebalance that moves Tasks has
 * two rounds: the Worker that gives Tasks up sees them revoked in the first, the one that takes
 * them sees them assigned in the second.
 */
export interface RebalanceEvent {
  /** The Worker's number: 0 to the number of worker threads − 1 (0 with `workers: 0`). */
  worker: number
  /** The partitions the Worker owns since this round. */
  assigned: TopicPartition[]
  /**
   * The partitions the Worker no longer owns, or lost and got back: their Tasks are closed, and
   * started again (with a Restore) if they come back.
   */
  revoked: TopicPartition[]
}

/** A Task began restoring one of its State Stores from its Changelog. */
export interface RestoreStartEvent {
  worker: number
  /** The Task: its partition number. */
  task: number
  /** The State Store's name. */
  store: string
  /** True when the store was wiped and its whole Changelog is replayed (no usable Checkpoint). */
  full: boolean
  /**
   * The Changelog offsets left to replay. Transaction markers and aborted records take offsets
   * too, so fewer records may be left.
   */
  remaining: number
}

/** A Restore applied another chunk of its Changelog. */
export interface RestoreProgressEvent {
  worker: number
  task: number
  store: string
  /** The records applied to the store so far. */
  restored: number
  /** The Changelog offsets left to replay (see {@link RestoreStartEvent.remaining}). */
  remaining: number
}

/** A Restore finished, or was cancelled. */
export interface RestoreEndEvent {
  worker: number
  task: number
  store: string
  /** The records applied to the store. */
  restored: number
  /** How long the Restore took, in milliseconds. */
  durationMs: number
  /**
   * True when the Restore stopped early because the Task moved to another Worker or the app is
   * stopping. The store is then incomplete; the Task's next owner restores it again.
   */
  cancelled: boolean
}

/** The events a Streams App emits, by name, with their payloads. */
export interface StreamsAppEvents {
  state: StateChangeEvent
  error: StreamsAppErrorEvent
  rebalance: RebalanceEvent
  'restore-start': RestoreStartEvent
  'restore-progress': RestoreProgressEvent
  'restore-end': RestoreEndEvent
}

/** The name of a Streams App event. */
export type StreamsAppEventName = keyof StreamsAppEvents

/** Why a Kafka transaction was aborted (exactly-once only). */
export type AbortReason =
  /** The Worker lost partitions whose records were in the batch (ADR-0006). */
  | 'rebalance'
  /** Processing or committing failed; the app stops. */
  | 'error'
  /** `stop()` ran out of `shutdownTimeoutMs` and gave the batch up. */
  | 'shutdown-timeout'

/** What a starting Task waited for (see the `wait-duration` metric). */
export type WaitKind =
  /** Its Task directory lock, held by its previous owner until that one closes the Task. */
  | 'task-lock'
  /** Exactly-once: its committed offsets to be stable (its previous owner's transaction ended). */
  | 'stable-offsets'
  /** Restore: the transactions open on a Changelog partition to end, before reading it. */
  | 'changelog-transactions'

/** `T` as one object type, so that editors show a metric's labels in full. */
type Flat<T> = { [K in keyof T]: T[K] }

interface WorkerLabels {
  /** The Worker's number: 0 to the number of worker threads − 1 (0 with `workers: 0`). */
  worker: number
}
interface TaskLabels extends WorkerLabels {
  /** The Task: its partition number. */
  task: number
}

/**
 * A measurement, given to the metrics sink as it happens. `type` says how to aggregate it:
 * - `counter`: `value` is an increment (rates such as records per second derive from it);
 * - `gauge`: `value` is the current value, replacing the previous one with the same labels;
 * - `duration`: `value` is one observation in milliseconds, for a histogram or a summary.
 *
 * Labels are strings and numbers, so a generic sink can pass `Object.entries(metric.labels)` on.
 */
export type Metric =
  /**
   * Input records a Task finished with: processed, or skipped by an error handler. A Worker counts
   * them once their batch has committed; under exactly-once a batch that aborts is processed again
   * and counted again.
   */
  | { name: 'records-processed'; type: 'counter'; value: number; labels: TaskLabels }
  /**
   * Input records an error handler skipped (it returned `'continue'`). A skipped record doesn't
   * advance Stream Time; what it forwarded before it failed is kept.
   */
  | {
      name: 'records-dropped'
      type: 'counter'
      value: number
      labels: Flat<TaskLabels & { kind: ErrorHandlerKind }>
    }
  /** Records windowed aggregations dropped because they arrived after the Grace Period. */
  | { name: 'late-records-dropped'; type: 'counter'; value: number; labels: TaskLabels }
  /** Kafka transactions aborted (exactly-once only). */
  | {
      name: 'aborts'
      type: 'counter'
      value: number
      labels: Flat<WorkerLabels & { reason: AbortReason }>
    }
  /**
   * How long committing a batch took: producing (at-least-once) or flushing (exactly-once) its
   * outputs, committing its offsets (and transaction), its store transactions and Checkpoints.
   * Not reported for a batch with nothing to commit.
   */
  | { name: 'commit-duration'; type: 'duration'; value: number; labels: WorkerLabels }
  /**
   * The records of an owned input partition after its committed offset, up to its end (its last
   * stable offset, as `read_committed` readers see it). Measured every few seconds.
   */
  | {
      name: 'consumer-lag'
      type: 'gauge'
      value: number
      labels: Flat<WorkerLabels & { topic: string; partition: number }>
    }
  /** Changelog records a Restore applied to a State Store. */
  | {
      name: 'records-restored'
      type: 'counter'
      value: number
      labels: Flat<TaskLabels & { store: string }>
    }
  /** The Changelog offsets a State Store's Restore has left; 0 once it is done. */
  | {
      name: 'restore-remaining'
      type: 'gauge'
      value: number
      labels: Flat<TaskLabels & { store: string }>
    }
  /**
   * How long a starting Task waited for something its previous owner held, in milliseconds (see
   * {@link WaitKind}). The Worker's other Tasks keep processing meanwhile.
   */
  | {
      name: 'wait-duration'
      type: 'duration'
      value: number
      labels: Flat<TaskLabels & { wait: WaitKind }>
    }

/** The name of a metric. */
export type MetricName = Metric['name']

/**
 * Receives the Streams App's metrics, e.g. to feed a monitoring system. With worker threads, every
 * Worker's metrics reach the sink in the main thread. The Test Driver gives it the counters of its
 * Task (as Worker 0, Task 0).
 *
 * `record` is called synchronously from the engine, so it should be quick (e.g. update an
 * in-memory registry). An exception it throws is ignored.
 */
export interface MetricsSink {
  record(metric: Metric): void
}

/** Internal: a Worker's own phase, which the Streams App sums up into the Application State. */
export type WorkerState = 'rebalancing' | 'restoring' | 'running'

/** Internal: the events a Worker emits, as it reports them. */
export type WorkerEvent = {
  [E in 'rebalance' | 'restore-start' | 'restore-progress' | 'restore-end']: {
    name: E
    payload: StreamsAppEvents[E]
  }
}['rebalance' | 'restore-start' | 'restore-progress' | 'restore-end']

/**
 * Internal: what a Worker reports to the Streams App. Plain data, so a worker thread can post it
 * to the main thread.
 */
export type WorkerReport =
  | { type: 'state'; state: WorkerState }
  | { type: 'event'; event: WorkerEvent }
  | { type: 'metric'; metric: Metric }

/** Internal: the Streams App's listeners, by event. Calls each with exceptions ignored. */
export class Listeners {
  readonly #listeners = new Map<StreamsAppEventName, Set<(payload: never) => void>>()

  on<E extends StreamsAppEventName>(event: E, listener: (payload: StreamsAppEvents[E]) => void) {
    if (typeof listener !== 'function') throw new TypeError('A listener must be a function')
    let set = this.#listeners.get(event)
    if (set === undefined) {
      set = new Set()
      this.#listeners.set(event, set)
    }
    set.add(listener)
  }

  off<E extends StreamsAppEventName>(event: E, listener: (payload: StreamsAppEvents[E]) => void) {
    this.#listeners.get(event)?.delete(listener)
  }

  emit<E extends StreamsAppEventName>(event: E, payload: StreamsAppEvents[E]): void {
    for (const listener of [...(this.#listeners.get(event) ?? [])]) {
      try {
        ;(listener as (payload: StreamsAppEvents[E]) => void)(payload)
      } catch {
        // A listener's bug must not break the app (see `StreamsApp.on`).
      }
    }
  }
}

/** Internal: a Task's counters, from which its counter metrics are made. */
export interface TaskCounters {
  /** Input records the Task finished with: processed, or skipped by an error handler. */
  recordsProcessed: number
  /** Input records error handlers skipped, by the kind of error. */
  recordsDropped: Record<ErrorHandlerKind, number>
  /** Records dropped by windowed aggregations because they arrived after the Grace Period. */
  lateRecordsDropped: number
}

/** Internal: no counts yet. */
export function noTaskCounters(): TaskCounters {
  return {
    recordsProcessed: 0,
    recordsDropped: { deserialization: 0, processing: 0 },
    lateRecordsDropped: 0,
  }
}

/**
 * Internal: the counter metrics of what Task `task` of Worker `worker` counted since `before`
 * (`now` minus `before`), leaving out those that didn't change.
 */
export function taskCounterMetrics(
  worker: number,
  task: number,
  now: TaskCounters,
  before: TaskCounters,
): Metric[] {
  const labels = { worker, task }
  const metrics: Metric[] = []
  const processed = now.recordsProcessed - before.recordsProcessed
  if (processed > 0) {
    metrics.push({ name: 'records-processed', type: 'counter', value: processed, labels })
  }
  for (const kind of ['processing', 'deserialization'] as const) {
    const dropped = now.recordsDropped[kind] - before.recordsDropped[kind]
    if (dropped > 0) {
      metrics.push({
        name: 'records-dropped',
        type: 'counter',
        value: dropped,
        labels: { ...labels, kind },
      })
    }
  }
  const late = now.lateRecordsDropped - before.lateRecordsDropped
  if (late > 0) metrics.push({ name: 'late-records-dropped', type: 'counter', value: late, labels })
  return metrics
}

/** Internal: gives `metric` to `sink`, ignoring its exceptions. */
export function recordMetric(sink: MetricsSink | undefined, metric: Metric): void {
  if (sink === undefined) return
  try {
    sink.record(metric)
  } catch {
    // A sink's bug must not break the app (see `MetricsSink`).
  }
}

/** Internal: throws unless `sink` is a metrics sink (or undefined). */
export function checkMetricsSink(sink: unknown): void {
  if (sink === undefined) return
  if (typeof (sink as Partial<MetricsSink> | null)?.record !== 'function') {
    throw new TypeError('metrics must be a metrics sink with a record(metric) function')
  }
}

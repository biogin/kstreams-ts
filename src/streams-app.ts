import { checkErrorHandlers, type ErrorHandlers } from './error-handlers.js'
import { type ProcessorContext, ProcessorGraph, type ProcessorSupplier } from './processor-graph.js'
import type { StreamRecord } from './record.js'
import { serdes as builtInSerdes, type Serde, type Serdes } from './serde.js'
import {
  claimProcessIdentity,
  type ProcessIdentity,
  taskLockHolder,
  unlockTasksOf,
} from './state-directory.js'
import {
  defaultStoreSupplier,
  type KeyEncoding,
  type KeyValueStore,
  type NamedStore,
  rangeByStoreKey,
  type StateStoreSupplier,
} from './state-store.js'
import { encodeKey } from './store-key.js'
import { type Sync, type SyncKeyValue, synchronous } from './sync-dsl.js'
import { taskInternals } from './task.js'
import {
  firstKeptWindowStart,
  type TumblingWindows,
  type Windowed,
  windowedSerde,
  windowKeyEncoding,
  windowOf,
} from './windows.js'
import { startWorker, type Worker } from './worker.js'
import { startWorkerThread } from './worker-thread.js'

export interface StreamsAppConfig {
  /**
   * The single name the application is identified by. Its consumer group, Internal Topic names,
   * transactional IDs and state directory all derive from it. Allowed characters: ASCII letters,
   * digits, `.`, `_` and `-`.
   */
  applicationId: string
  /**
   * The directory local State Stores are kept in, under `<stateDir>/<applicationId>/<task>/`.
   * With one, State Stores that aren't given a supplier are SQLite databases there; without one,
   * they are kept in memory. Persistent stores keep a Checkpoint there (the file `.checkpoint`),
   * so a restart restores only the part of their Changelogs after it; after a crash mid-batch the
   * stores are wiped and fully restored. Without a persistent state directory (e.g. a disposable
   * disk) every start is a full Restore. The Test Driver ignores it (see `createTestDriver`).
   */
  stateDir?: string
  /**
   * Bootstrap brokers (`host:port`) to connect to. Needed to `start` the app; the Test Driver
   * doesn't use them.
   */
  brokers?: string[]
  /**
   * The Delivery Guarantee. Defaults to `'at-least-once'`. Under `'exactly-once'` each batch's
   * outputs and consumed offsets commit together in one Kafka transaction, so `read_committed`
   * readers never see the outputs of a failed batch. State Store writes stay in a store
   * transaction until that Kafka transaction has committed, and are rolled back if it aborts.
   */
  guarantee?: DeliveryGuarantee
  /**
   * The Workers this process runs, each its own consumer-group member with its own consumer,
   * producer and State Stores. `0` (the default) runs one Worker in the main thread.
   *
   * More Workers run in worker threads, which can't be handed the app's functions, so each one
   * rebuilds the app from its App Module: the module that exports this Streams App as its default
   * export, without side effects (it must not call `start`). Name it in the module itself:
   * `workers: { count: 4, module: import.meta.url }`, and start the app from another file.
   */
  workers?: number | WorkersConfig
  /**
   * How long a Worker collects records into a batch before it produces the outputs and commits
   * the batch, in milliseconds. Defaults to 100.
   */
  commitIntervalMs?: number
  /**
   * A batch is committed early once it has this many input records. Defaults to 1000. It also
   * bounds how many records a Worker holds in memory for its Tasks whose State Stores are
   * restoring; records past it are fetched again once the Restore is done.
   */
  commitMaxRecords?: number
  /**
   * How long the consumer group waits for a member that stopped sending heartbeats before moving
   * its Tasks to other members, in milliseconds. Defaults to 45000.
   */
  sessionTimeoutMs?: number
  /**
   * How long `stop()` may take to finish and commit the current batch, in milliseconds. When it
   * runs out, the batch is given up instead (under exactly-once, its transaction is aborted, so
   * it is processed again after a restart) and the app still closes its State Stores and leaves
   * the consumer group. Defaults to 30000.
   */
  shutdownTimeoutMs?: number
  /**
   * The replication factor of the Changelog topics the app creates, one per State Store, named
   * `<applicationId>-<store>-changelog`. Defaults to 3; use 1 with a single-broker cluster. Only
   * used when `autoCreateInternalTopics` creates the topics.
   */
  changelogReplicationFactor?: number
  /**
   * Whether the app creates its missing Internal Topics (the Changelogs) on start. When false, it
   * only checks them: `start()` fails with one error listing every missing Internal Topic, with
   * its required partition count and `cleanup.policy=compact`. Either way, an existing Internal
   * Topic with the wrong partition count is a fatal error. Defaults to true.
   */
  autoCreateInternalTopics?: boolean
  /**
   * Where records whose deserialization or processing fails go (ADR-0010). Without a handler, the
   * first such error stops the app (fail-fast), which is the default. `deadLetter(topic)` writes
   * failed records to a dead-letter topic instead, in the same transaction under exactly-once.
   * Errors writing to Kafka (produce errors) and `AsyncNotAllowedError` always stop the app.
   * With worker threads, handlers run in each worker thread.
   */
  errorHandlers?: ErrorHandlers
}

/** Worker threads, and the App Module each of them rebuilds the Streams App from. */
export interface WorkersConfig {
  /** The number of Workers (worker threads); `0` runs one Worker in the main thread instead. */
  count: number
  /**
   * The URL of the App Module, usually `import.meta.url` in the module that defines the app. Its
   * default export must be this Streams App.
   */
  module: string | URL
}

/** The processing contract an application runs under. */
export type DeliveryGuarantee = 'at-least-once' | 'exactly-once'

/**
 * The lifecycle phase a Streams App reports: created → rebalancing ⇄ restoring ⇄ running →
 * stopping → stopped, or error from any state.
 */
export type ApplicationState =
  | 'created'
  | 'rebalancing'
  | 'restoring'
  | 'running'
  | 'stopping'
  | 'stopped'
  | 'error'

/** The in-process object that defines and runs an application. */
export interface StreamsApp {
  readonly applicationId: string
  /** The configured state directory, if any. */
  readonly stateDir: string | undefined
  /** The Application State. */
  readonly state: ApplicationState
  /**
   * Connects to Kafka and starts processing: starts the Workers, each of which joins the consumer
   * group (named after the Application ID) and resumes from its committed offsets, or reads input
   * topics from the beginning the first time. Resolves once every Worker is running. Rejects if the
   * app has no brokers or reads no topic, was started before, or a Worker can't start (the app
   * then goes to `error` and stops the other Workers).
   *
   * Define the whole app before starting it.
   */
  start(): Promise<void>
  /**
   * Stops every Worker gracefully: each finishes and commits its current batch, closes its State
   * Stores, then leaves the consumer group, so its Tasks move to other members right away. If that
   * takes longer than `shutdownTimeoutMs`, the batch is given up instead (see there) and the
   * Worker still leaves the group. Resolves once the app is `stopped`. If the app stopped on its
   * own because of an error (state `error`: a Worker failed, and the others were stopped), rejects
   * with that error.
   */
  stop(): Promise<void>
  /**
   * Declares a Stream read from `topic`, decoded with the given key and value Serdes. A record with
   * a null value (tombstone) arrives as `null`, so the Stream's values are typed `V | null`; drop
   * tombstones with a type guard, e.g. `.filter((key, value): value is V => value !== null)`.
   */
  stream<K, V>(topic: string, serdes: Serdes<K, V>): Stream<K, V | null>
  /**
   * Registers a Processor under a unique `name`. The supplier is called once per Task with that
   * Task's context. Connect it to a Stream with `stream.process(...)`.
   */
  processor<KIn, VIn, KOut, VOut>(
    name: string,
    supplier: ProcessorSupplier<KIn, VIn, KOut, VOut>,
  ): NamedProcessor<KIn, VIn, KOut, VOut>
  /**
   * Registers a key-value State Store under a unique `name`, for Processors to use with
   * `ctx.store`. Each Task gets its own store. The name is part of the store's Changelog topic,
   * so it allows the same characters as the Application ID.
   */
  store<K, V>(name: string, serdes: Serdes<K, V>, options?: StoreOptions): NamedStore<K, V>
}

export interface StoreOptions {
  /**
   * Opens the State Store. Defaults to SQLite in the Task's state directory, or to the in-memory
   * store when there is no state directory.
   */
  supplier?: StateStoreSupplier
}

declare const recordTypes: unique symbol

/** A Processor registered with `app.processor`, ready to be connected to Streams. */
export interface NamedProcessor<KIn, VIn, KOut, VOut> {
  readonly name: string
  /** Type-only: the records it reads and forwards, so Streams are checked against them. */
  readonly [recordTypes]?: {
    reads: (record: StreamRecord<KIn, VIn>) => void
    forwards: StreamRecord<KOut, VOut>
  }
}

/**
 * An unbounded sequence of keyed records where each record is an independent event.
 *
 * Functions passed to its operators must be synchronous (ADR-0012): an async function fails to
 * compile and is rejected when the app is built, and one that returns a Promise throws
 * `AsyncNotAllowedError` on the first record. Async work belongs in a Processor.
 *
 * Operators and Processors get every record, tombstones included: where a Stream can carry them
 * (read from a topic, or the updates of a Table) its value type includes `null`.
 */
export interface Stream<K, V> {
  /** Keeps only the records for which `predicate` returns true. */
  filter<VOut extends V>(predicate: (key: K, value: V) => value is VOut): Stream<K, VOut>
  filter<R extends boolean | PromiseLike<unknown>>(
    predicate: (key: K, value: V) => Sync<R>,
  ): Stream<K, V>
  /**
   * Turns each record into a new key and value, which may have other types. The record keeps its
   * timestamp. The resulting Stream has no Serdes of its own, so `.to()` must be given both.
   */
  map<R>(mapper: (key: K, value: V) => SyncKeyValue<R>): Stream<KeyOf<R>, ValueOf<R>>
  /**
   * Calls `action` with each record (for logging, metrics, ...) and passes the record on unchanged.
   * Whatever `action` returns is ignored.
   */
  peek<R>(action: (key: K, value: V) => Sync<R>): Stream<K, V>
  /**
   * Writes every record to `topic`. Serdes not given here default to the ones this Stream was
   * read with.
   */
  to(topic: string, serdes?: Partial<Serdes<K, NonNullable<V>>>): void
  /**
   * Sends every record to a Processor and returns the Stream of records it forwards. Connecting
   * several Streams to one Processor merges them; each call returns that same output Stream. The
   * output Stream has no Serdes, so `.to()` after it needs both.
   */
  process<KOut, VOut>(processor: NamedProcessor<K, V, KOut, VOut>): Stream<KOut, VOut>
  /**
   * Groups the records by their current key, to aggregate them per key. Needs a key Serde for the
   * aggregation's State Store: pass one if this Stream has none (e.g. after `map`). The value Serde
   * is used by `reduce`. Aggregations skip tombstones, so the grouped values are never null.
   */
  groupByKey(serdes?: Partial<Serdes<K, NonNullable<V>>>): GroupedStream<K, NonNullable<V>>
}

type KeyOf<R> = R extends { key: infer KOut } ? KOut : never
type ValueOf<R> = R extends { value: infer VOut } ? VOut : never

/**
 * A Stream grouped by key, ready to be aggregated per key into a Table. Like Kafka Streams, every
 * aggregation skips records with a null key or a null value (tombstone). Each update of the Table
 * is forwarded with the Event Time of the record that caused it.
 */
export interface GroupedStream<K, V> {
  /** Counts the records of each key into a Table. */
  count(options?: CountOptions): Table<K, number>
  /**
   * Folds the records of each key into a Table: `aggregator` gets each record and the key's
   * current aggregate (from `initializer` when the key has none) and returns the new aggregate.
   * Returning null deletes the key from the Table and forwards a tombstone; the key's next record
   * starts again from `initializer`. `options.serde` stores the aggregates.
   */
  aggregate<
    VA,
    RI extends VA | PromiseLike<unknown> = VA,
    RA extends VA | null | PromiseLike<unknown> = VA | null,
  >(
    initializer: () => Sync<RI>,
    aggregator: (key: K, value: V, aggregate: VA) => Sync<RA>,
    options: AggregateOptions<VA>,
  ): Table<K, VA>
  /**
   * Combines the values of each key into a Table: the first value of a key is stored as is, and
   * each later one is combined with the stored value by `reducer`. Returning null deletes the key
   * and forwards a tombstone. Stores with `options.serde`, else the value Serde of the grouped
   * Stream; throws if there is neither.
   */
  reduce<R extends V | null | PromiseLike<unknown> = V | null>(
    reducer: (aggregate: V, value: V, key: K) => Sync<R>,
    options?: ReduceOptions<V>,
  ): Table<K, V>
  /**
   * Aggregates the records of each key per window instead of over all time: each record goes into
   * the window its Event Time falls into. Pass windows made with `tumbling`.
   */
  window(windows: TumblingWindows): WindowedStream<K, V>
  /** Type-only: the key and value types of the grouped records. */
  readonly [recordTypes]?: { key: K; value: V }
}

/**
 * A grouped Stream split into time windows, ready to be aggregated per key and window into a
 * Table keyed by {@link Windowed} keys. Aggregations skip null keys and tombstones as on a
 * `GroupedStream`.
 *
 * Windows are driven by Stream Time: a window closes once Stream Time reaches its end plus the
 * Grace Period. A record for a closed window is dropped and counted as late (the Test Driver's
 * `metrics().lateRecordsDropped`). Each update of the Table is forwarded with the window end as
 * its timestamp. In the Table's Stream, windowed keys are written as the key Serde's bytes followed
 * by the window start as an 8-byte big-endian integer.
 */
export interface WindowedStream<K, V> {
  /** Counts the records of each key per window. */
  count(options?: CountOptions): Table<Windowed<K>, number>
  /**
   * Folds the records of each key and window into a Table, starting each window from
   * `initializer`. Returning null deletes the window's entry and forwards a tombstone.
   * `options.serde` stores the aggregates.
   */
  aggregate<
    VA,
    RI extends VA | PromiseLike<unknown> = VA,
    RA extends VA | null | PromiseLike<unknown> = VA | null,
  >(
    initializer: () => Sync<RI>,
    aggregator: (key: K, value: V, aggregate: VA) => Sync<RA>,
    options: AggregateOptions<VA>,
  ): Table<Windowed<K>, VA>
  /**
   * Combines the values of each key and window: the first value of a window is stored as is, and
   * each later one is combined with it by `reducer`. Returning null deletes the window's entry and
   * forwards a tombstone. Stores with `options.serde`, else the value Serde of the grouped Stream.
   */
  reduce<R extends V | null | PromiseLike<unknown> = V | null>(
    reducer: (aggregate: V, value: V, key: K) => Sync<R>,
    options?: ReduceOptions<V>,
  ): Table<Windowed<K>, V>
  /** Type-only: the key and value types of the grouped records. */
  readonly [recordTypes]?: { key: K; value: V }
}

export interface CountOptions {
  /**
   * The name of the State Store holding the counts, which is part of its Changelog topic. Name it
   * so that the name stays stable as the app changes; without a name one is generated.
   */
  store?: string
  /**
   * Opens the State Store. Defaults to SQLite in the Task's state directory, or to the in-memory
   * store when there is no state directory.
   */
  storeSupplier?: StateStoreSupplier
}

export interface AggregateOptions<VA> extends CountOptions {
  /** Serializes the aggregates in the State Store, and in the Table's Stream by default. */
  serde: Serde<VA>
}

export interface ReduceOptions<V> extends CountOptions {
  /**
   * Serializes the values in the State Store, and in the Table's Stream by default. Defaults to
   * the value Serde of the grouped Stream.
   */
  serde?: Serde<V>
}

/** A keyed view where each record is an upsert of the latest value for its key. */
export interface Table<K, V> {
  /** The name of the State Store the Table is kept in. */
  readonly store: string
  /**
   * The Stream of the Table's updates: one record per change, with the key's new value, or a
   * tombstone (null) when the key is deleted. It has the Table's Serdes, so `.to()` needs none.
   */
  toStream(): Stream<K, V | null>
  /** Type-only: the key and value types of the Table. */
  readonly [tableTypes]?: { key: K; value: V }
}

declare const tableTypes: unique symbol

const NAME = /^[A-Za-z0-9._-]+$/

export function createStreamsApp(config: StreamsAppConfig): StreamsApp {
  const { applicationId, stateDir } = config
  if (typeof applicationId !== 'string' || !NAME.test(applicationId)) {
    throw new Error(
      `Invalid Application ID ${JSON.stringify(applicationId)}: use one or more ASCII letters, digits, ".", "_" or "-"`,
    )
  }
  const settings = runtimeSettings(config)
  checkErrorHandlers(config.errorHandlers)
  const graph = new ProcessorGraph(applicationId, config.errorHandlers)
  const runtime = new Runtime(graph, settings)
  const app: StreamsApp = {
    applicationId,
    stateDir,
    get state() {
      return runtime.state
    },
    start: () => runtime.start(),
    stop: () => runtime.stop(),
    stream(topic, serdes) {
      return new StreamNode(graph, graph.addSource(topic, serdes), withTombstones(serdes))
    },
    processor(name, supplier) {
      if (typeof name !== 'string' || name === '') {
        throw new Error('A Processor needs a non-empty name')
      }
      const processor = { name: graph.addProcessor(supplier, [], name) }
      registered.set(processor, { graph, output: new StreamNode(graph, name, {}) })
      return processor
    },
    store(name, serdes, options = {}) {
      addStore(graph, name, serdes, options.supplier)
      return { name }
    },
  }
  graphs.set(app, graph)
  runtimes.set(app, runtime)
  return app
}

/**
 * Internal, for worker threads: starts Worker `workerNumber` of `app`, the default export of the
 * App Module `appModule`, which must be the Streams App `applicationId`.
 */
export async function startWorkerOf(
  app: unknown,
  { appModule, applicationId, processId, workerNumber, onError }: WorkerOfOptions,
): Promise<Worker> {
  const runtime = runtimes.get(app as StreamsApp)
  if (runtime === undefined) {
    throw new Error(
      `App Module ${appModule} must export the Streams App as its default export, without side effects`,
    )
  }
  if ((app as StreamsApp).applicationId !== applicationId) {
    throw new Error(
      `App Module ${appModule} exports Streams App "${(app as StreamsApp).applicationId}", not "${applicationId}"`,
    )
  }
  return runtime.startWorker(processId, workerNumber, onError)
}

export interface WorkerOfOptions {
  appModule: string
  applicationId: string
  /** The App Process ID of the process the thread runs in (see state-directory.ts). */
  processId: string
  workerNumber: number
  onError(error: unknown): void
}

interface RuntimeSettings {
  applicationId: string
  guarantee: DeliveryGuarantee
  brokers: string[] | undefined
  commitIntervalMs: number
  commitMaxRecords: number
  sessionTimeoutMs: number
  shutdownTimeoutMs: number
  changelogReplicationFactor: number
  autoCreateInternalTopics: boolean
  stateDir: string | undefined
  /** The number of worker threads; 0 runs one Worker in the main thread. */
  workers: number
  /** The App Module worker threads rebuild the app from; set when `workers > 0`. */
  appModule: URL | undefined
}

function runtimeSettings(config: StreamsAppConfig): RuntimeSettings {
  const { guarantee = 'at-least-once', brokers } = config
  if (guarantee !== 'at-least-once' && guarantee !== 'exactly-once') {
    throw new Error(`Unknown Delivery Guarantee ${JSON.stringify(guarantee)}`)
  }
  const { workers, appModule } = workersSettings(config.workers ?? 0)
  if (brokers !== undefined && brokers.length === 0) {
    throw new Error('brokers must list at least one broker (host:port)')
  }
  return {
    applicationId: config.applicationId,
    guarantee,
    brokers,
    commitIntervalMs: positive('commitIntervalMs', config.commitIntervalMs ?? 100),
    commitMaxRecords: positive('commitMaxRecords', config.commitMaxRecords ?? 1000, true),
    sessionTimeoutMs: positive('sessionTimeoutMs', config.sessionTimeoutMs ?? 45_000),
    shutdownTimeoutMs: positive('shutdownTimeoutMs', config.shutdownTimeoutMs ?? 30_000),
    changelogReplicationFactor: positive(
      'changelogReplicationFactor',
      config.changelogReplicationFactor ?? 3,
      true,
    ),
    autoCreateInternalTopics: config.autoCreateInternalTopics ?? true,
    stateDir: config.stateDir,
    workers,
    appModule,
  }
}

function workersSettings(config: number | WorkersConfig): {
  workers: number
  appModule: URL | undefined
} {
  const { count, module } =
    typeof config === 'number' ? { count: config, module: undefined } : config
  if (!(Number.isInteger(count) && count >= 0)) {
    throw new Error(`workers must be a whole number (0 or more), got ${count}`)
  }
  if (count === 0) return { workers: 0, appModule: undefined }
  if (module === undefined) {
    throw new Error(
      `workers > 0 needs an App Module: use workers: { count: ${count}, module: import.meta.url } in the module that exports the Streams App`,
    )
  }
  if (!URL.canParse(String(module))) {
    throw new Error(
      `The App Module must be a URL such as import.meta.url, got ${JSON.stringify(String(module))}`,
    )
  }
  return { workers: count, appModule: new URL(String(module)) }
}

function positive(option: string, value: number, whole = false): number {
  if (!(value > 0 && Number.isFinite(value) && (!whole || Number.isInteger(value)))) {
    throw new Error(`${option} must be a positive ${whole ? 'whole ' : ''}number, got ${value}`)
  }
  return value
}

/**
 * Runs a Streams App on Kafka and tracks its Application State, which sums up its Workers': it is
 * running once every Worker is, and goes to `error` when one fails, which stops the others.
 */
class Runtime {
  readonly #graph: ProcessorGraph
  readonly #settings: RuntimeSettings
  #state: ApplicationState = 'created'
  /** One per Worker, settled once it is running (undefined if it failed to start). */
  #workers: Promise<Worker | undefined>[] | undefined
  #stopped: Promise<void> | undefined
  #workersStopped: Promise<void> | undefined
  /** The error that put the app in the `error` state. */
  #failure: { error: unknown } | undefined
  /** The App Process ID, while the app runs. */
  #identity: ProcessIdentity | undefined

  constructor(graph: ProcessorGraph, settings: RuntimeSettings) {
    this.#graph = graph
    this.#settings = settings
  }

  get state(): ApplicationState {
    return this.#state
  }

  async start(): Promise<void> {
    const { applicationId, workers, appModule, shutdownTimeoutMs } = this.#settings
    if (this.#state !== 'created' || this.#workers !== undefined) {
      throw new Error(`Streams App "${applicationId}" cannot start: it is ${this.#state}`)
    }
    this.#checkStartable()
    const identity = claimProcessIdentity(this.#settings.stateDir, applicationId)
    this.#identity = identity
    const onError = (error: unknown) => this.#fail(error)
    const starting =
      appModule === undefined
        ? [this.startWorker(identity.id, 0, onError)]
        : Array.from({ length: workers }, (_, workerNumber) =>
            startWorkerThread({
              appModule,
              applicationId,
              processId: identity.id,
              workerNumber,
              shutdownTimeoutMs,
              onError,
            }),
          )
    this.#workers = starting.map((worker) => worker.catch(() => undefined))
    const started = await Promise.allSettled(starting)
    const failed = started.find((result) => result.status === 'rejected')
    if (failed !== undefined) {
      this.#fail(failed.reason)
      throw failed.reason
    }
    if (this.#state === 'created') this.#state = 'running'
  }

  /** Starts Worker `workerNumber` of App Process `processId` in this thread. */
  startWorker(
    processId: string,
    workerNumber: number,
    onError: (error: unknown) => void,
  ): Promise<Worker> {
    const brokers = this.#checkStartable()
    const { applicationId, guarantee } = this.#settings
    const workerId = `${applicationId}-${processId}-worker-${workerNumber}`
    return startWorker(this.#graph, {
      ...this.#settings,
      brokers,
      clientId: workerId,
      // Unique among the live Workers of the application, and the same when the process restarts
      // with the same state directory, so a restarted Worker fences its predecessor's producer.
      transactionalId: guarantee === 'exactly-once' ? workerId : undefined,
      lockHolder: taskLockHolder(processId, workerNumber),
      onError,
    })
  }

  /** Throws if the app can't start; returns its brokers. */
  #checkStartable(): string[] {
    const { applicationId, brokers } = this.#settings
    if (brokers === undefined) {
      throw new Error(`Streams App "${applicationId}" has no brokers to connect to`)
    }
    if (this.#graph.sources.size === 0) {
      throw new Error(`Streams App "${applicationId}" reads no topic: declare a Stream first`)
    }
    return brokers
  }

  stop(): Promise<void> {
    this.#stopped ??= this.#stop()
    return this.#stopped
  }

  async #stop(): Promise<void> {
    if (this.#state === 'created' && this.#workers === undefined) {
      this.#state = 'stopped'
      return
    }
    if (this.#state !== 'error') this.#state = 'stopping'
    await this.#stopWorkers()
    if (this.#failure !== undefined) {
      throw this.#failure.error
    }
    this.#state = 'stopped'
  }

  /** Stops every Worker (each once it has started), all at the same time. */
  #stopWorkers(): Promise<void> {
    this.#workersStopped ??= Promise.all(
      (this.#workers ?? []).map(async (starting) => {
        const worker = await starting
        try {
          await worker?.stop()
        } catch (error) {
          this.#fail(error)
        }
      }),
    ).then(() => this.#releaseIdentity())
    return this.#workersStopped
  }

  /**
   * Once every Worker has stopped: removes the Task locks they still hold (a terminated worker
   * thread can't), and gives the App Process ID up.
   */
  #releaseIdentity(): void {
    const identity = this.#identity
    if (identity === undefined) return
    this.#identity = undefined
    const { stateDir, applicationId } = this.#settings
    try {
      if (stateDir !== undefined) unlockTasksOf(stateDir, applicationId, identity.id)
      identity.release()
    } catch {
      // A lock left behind is stale once this process is gone.
    }
  }

  #fail(error: unknown): void {
    this.#failure ??= { error }
    this.#state = 'error'
    // One Worker failed: stop the others, so the app doesn't keep running with part of its Tasks.
    void this.#stopWorkers()
  }
}

function addStore(
  graph: ProcessorGraph,
  name: string,
  serdes: Serdes<unknown, unknown>,
  supplier: StateStoreSupplier = defaultStoreSupplier,
  keyEncoding?: KeyEncoding<unknown>,
): void {
  if (typeof name !== 'string' || !NAME.test(name)) {
    throw new Error(
      `Invalid State Store name ${JSON.stringify(name)}: use one or more ASCII letters, digits, ".", "_" or "-"`,
    )
  }
  graph.addStore({
    name,
    serdes,
    supplier,
    changelogTopic: `${graph.applicationId}-${name}-changelog`,
    ...(keyEncoding === undefined ? {} : { keyEncoding }),
  })
}

const graphs = new WeakMap<StreamsApp, ProcessorGraph>()
const runtimes = new WeakMap<StreamsApp, Runtime>()

// Record types are erased once a Processor is registered; `Stream.process` restores them.
// biome-ignore lint/suspicious/noExplicitAny: type erasure, see above
type AnyNamedProcessor = NamedProcessor<any, any, any, any>
// biome-ignore lint/suspicious/noExplicitAny: type erasure, see above
type AnyStream = Stream<any, any>
const registered = new WeakMap<AnyNamedProcessor, { graph: ProcessorGraph; output: AnyStream }>()

/** Internal: the Processor graph a Streams App's DSL has compiled to so far. */
export function processorGraphOf(app: StreamsApp): ProcessorGraph {
  const graph = graphs.get(app)
  if (graph === undefined) throw new TypeError('Not a Streams App created by createStreamsApp')
  return graph
}

class StreamNode<K, V> implements Stream<K, V> {
  readonly #graph: ProcessorGraph
  readonly #node: string
  /** Serdes known to fit this Stream's records, inherited from its input topic or Table. */
  readonly #serdes: Partial<Serdes<K, NonNullable<V>>>

  constructor(graph: ProcessorGraph, node: string, serdes: Partial<Serdes<K, NonNullable<V>>>) {
    this.#graph = graph
    this.#node = node
    this.#serdes = serdes
  }

  filter(predicate: (key: K, value: V) => boolean): Stream<K, V> {
    const test = synchronous('filter', predicate)
    return this.#then<K, V>(
      ({ forward }) => ({
        process(record) {
          if (test(record.key, record.value)) forward(record)
        },
      }),
      this.#serdes,
    )
  }

  map<R>(mapper: (key: K, value: V) => SyncKeyValue<R>): Stream<KeyOf<R>, ValueOf<R>> {
    // Once Promises are ruled out (at runtime by `synchronous`), the mapper returns a key and a value.
    const toKeyValue = synchronous('map', mapper) as (
      key: K,
      value: V,
    ) => { key: KeyOf<R>; value: ValueOf<R> }
    // The new key and value types may differ, so the inherited Serdes no longer apply.
    return this.#then<KeyOf<R>, ValueOf<R>>(
      ({ forward }) => ({
        process(record) {
          const { key, value } = toKeyValue(record.key, record.value)
          forward({ key, value, timestamp: record.timestamp })
        },
      }),
      {},
    )
  }

  peek(action: (key: K, value: V) => unknown): Stream<K, V> {
    const see = synchronous('peek', action)
    return this.#then<K, V>(
      ({ forward }) => ({
        process(record) {
          see(record.key, record.value)
          forward(record)
        },
      }),
      this.#serdes,
    )
  }

  /** Adds a Processor downstream of this Stream and returns the Stream of what it forwards. */
  #then<KOut, VOut>(
    supplier: ProcessorSupplier<K, V, KOut, VOut>,
    serdes: Partial<Serdes<KOut, NonNullable<VOut>>>,
  ): Stream<KOut, VOut> {
    return new StreamNode(this.#graph, this.#graph.addProcessor(supplier, [this.#node]), serdes)
  }

  to(topic: string, serdes: Partial<Serdes<K, NonNullable<V>>> = {}): void {
    const key = serdes.key ?? this.#serdes.key
    const value = serdes.value ?? this.#serdes.value
    this.#graph.addSink(
      topic,
      { key: required(key, 'key', topic), value: required(value, 'value', topic) },
      [this.#node],
    )
  }

  groupByKey(serdes: Partial<Serdes<K, NonNullable<V>>> = {}): GroupedStream<K, NonNullable<V>> {
    const key = serdes.key ?? this.#serdes.key
    if (key === undefined) {
      throw new Error('No key Serde for groupByKey: pass one to .groupByKey({ key })')
    }
    const value = serdes.value ?? this.#serdes.value
    return new GroupedStreamNode(this.#graph, this.#node, { key, value })
  }

  process<KOut, VOut>(processor: NamedProcessor<K, V, KOut, VOut>): Stream<KOut, VOut> {
    const entry = registered.get(processor)
    if (entry?.graph !== this.#graph) {
      throw new Error(`Processor "${processor.name}" was not registered with this Streams App`)
    }
    this.#graph.connect(this.#node, processor.name)
    return entry.output
  }
}

/**
 * How an aggregation keys its Table: by the grouped key, or by the grouped key and a window.
 * `KT` is the Table's key type.
 */
interface Keying<K, KT> {
  /** The Serde of the Table's keys, for its State Store and its Stream. */
  serde: Serde<KT>
  /** How the State Store encodes the Table's keys. Undefined for the default single part. */
  encoding: KeyEncoding<KT> | undefined
  /** Called once per Task, with the Task's view of the Table's State Store. */
  forTask(
    ctx: ProcessorContext<unknown, unknown>,
    store: KeyValueStore<KT, unknown>,
  ): TaskKeying<K, KT>
}

interface TaskKeying<K, KT> {
  /**
   * Returns the Table key a record updates and the timestamp to forward the update with, or
   * undefined to drop the record.
   */
  place(key: K, timestamp: number): { key: KT; timestamp: number } | undefined
  /** Called before each record: deletes the entries Stream Time has made expire, if any. */
  expire(): void
}

/** Keys the Table by the grouped key; updates keep the record's Event Time. */
function byKey<K>(serde: Serde<K>): Keying<K, K> {
  return {
    serde,
    encoding: undefined,
    forTask: () => ({ place: (key, timestamp) => ({ key, timestamp }), expire() {} }),
  }
}

/**
 * Keys the Table by the grouped key and the window of the record. Updates are timestamped with
 * the window end; records for windows that Stream Time has closed are dropped and counted as late.
 *
 * Retention: a window is kept for its size plus the Grace Period, until Stream Time passes its end
 * plus the Grace Period. Then its entries are deleted, which writes their Tombstones to the
 * Changelog; nothing is forwarded. Expired windows are looked for before each record, but only
 * when Stream Time has made another window expire since the last look: then one range scan over
 * the store keys before the first window still kept (they are time-first, ADR-0015) finds them
 * all, however many windows Stream Time jumped over.
 */
function byWindow<K>(serde: Serde<K>, windows: TumblingWindows): Keying<K, Windowed<K>> {
  return {
    serde: windowedSerde(serde, windows),
    encoding: windowKeyEncoding(serde, windows),
    forTask(ctx, store) {
      const task = taskInternals(ctx)
      /** The start of the first window that was kept at the last look for expired windows. */
      let keptFrom: number | undefined
      return {
        place(key, timestamp) {
          const window = windowOf(windows, timestamp)
          // The Task advances Stream Time before processing a record, so it is known here.
          const streamTime = task.streamTime() ?? timestamp
          if (window.end + windows.graceMs <= streamTime) {
            task.recordLateDropped()
            return undefined
          }
          return { key: { key, window }, timestamp: window.end }
        },
        expire() {
          const streamTime = task.streamTime()
          if (streamTime === undefined) return
          const from = firstKeptWindowStart(windows, streamTime)
          if (keptFrom !== undefined && from <= keptFrom) return
          keptFrom = from
          for (const [key] of rangeByStoreKey(store, undefined, encodeKey([from]))) {
            store.delete(key)
          }
        },
      }
    },
  }
}

/** The aggregations of a grouped Stream, into a Table keyed as its {@link Keying} says. */
class AggregationNode<K, V, KT> {
  protected readonly graph: ProcessorGraph
  protected readonly node: string
  protected readonly serdes: { key: Serde<K>; value: Serde<V> | undefined }
  readonly #keying: Keying<K, KT>

  constructor(
    graph: ProcessorGraph,
    node: string,
    serdes: { key: Serde<K>; value: Serde<V> | undefined },
    keying: Keying<K, KT>,
  ) {
    this.graph = graph
    this.node = node
    this.serdes = serdes
    this.#keying = keying
  }

  count(options: CountOptions = {}): Table<KT, number> {
    return this.#aggregation('count', builtInSerdes.number, options, (_key, _value, count) => {
      return (count ?? 0) + 1
    })
  }

  aggregate<VA>(
    initializer: () => unknown,
    aggregator: (key: K, value: V, aggregate: VA) => unknown,
    options: AggregateOptions<VA>,
  ): Table<KT, VA> {
    // Once Promises are ruled out (at runtime by `synchronous`), these return aggregates.
    const initial = synchronous('aggregate', initializer) as () => VA
    const next = synchronous('aggregate', aggregator) as (key: K, value: V, aggregate: VA) => VA
    return this.#aggregation('aggregate', options.serde, options, (key, value, aggregate) =>
      next(key, value, aggregate === undefined ? initial() : aggregate),
    )
  }

  reduce(
    reducer: (aggregate: V, value: V, key: K) => unknown,
    options: ReduceOptions<V> = {},
  ): Table<KT, V> {
    const combine = synchronous('reduce', reducer) as (aggregate: V, value: V, key: K) => V
    const serde = options.serde ?? this.serdes.value
    if (serde === undefined) {
      throw new Error(
        'No value Serde for reduce: pass one to .reduce(reducer, { serde }) or to .groupByKey({ value })',
      )
    }
    return this.#aggregation('reduce', serde, options, (key, value, aggregate) =>
      aggregate === undefined ? value : combine(aggregate, value, key),
    )
  }

  /**
   * Adds a State Store and a Processor that keeps, per Table key, the result of `update` on the
   * current value (undefined if none). A null result deletes the key and forwards a tombstone.
   */
  #aggregation<VA>(
    operator: string,
    serde: Serde<VA>,
    options: CountOptions,
    update: (key: K, value: V, aggregate: VA | undefined) => VA | null,
  ): Table<KT, VA> {
    const store = options.store ?? this.graph.storeName(operator)
    const keying = this.#keying
    const serdes = { key: keying.serde, value: serde }
    addStore(
      this.graph,
      store,
      serdes,
      options.storeSupplier,
      keying.encoding as KeyEncoding<unknown> | undefined,
    )
    // Like Kafka Streams, aggregations skip records without a key or with a tombstone value.
    const supplier: ProcessorSupplier<K | null, V | null, KT, VA | null> = (ctx) => {
      const aggregates = ctx.store<KT, VA>(store)
      const { place, expire } = keying.forTask(
        ctx as ProcessorContext<unknown, unknown>,
        aggregates as KeyValueStore<KT, unknown>,
      )
      return {
        process({ key, value, timestamp }) {
          expire()
          if (key === null || value === null) return
          const target = place(key, timestamp)
          if (target === undefined) return
          const aggregate = update(key, value, aggregates.get(target.key))
          if (aggregate === null) aggregates.delete(target.key)
          else aggregates.put(target.key, aggregate)
          ctx.forward({ key: target.key, value: aggregate, timestamp: target.timestamp })
        },
      }
    }
    const node = this.graph.addProcessor(supplier, [this.node])
    return new TableNode(this.graph, node, store, serdes)
  }
}

class GroupedStreamNode<K, V> extends AggregationNode<K, V, K> implements GroupedStream<K, V> {
  constructor(
    graph: ProcessorGraph,
    node: string,
    serdes: { key: Serde<K>; value: Serde<V> | undefined },
  ) {
    super(graph, node, serdes, byKey(serdes.key))
  }

  window(windows: TumblingWindows): WindowedStream<K, V> {
    const keying = byWindow(this.serdes.key, windows)
    return new AggregationNode(this.graph, this.node, this.serdes, keying)
  }
}

class TableNode<K, V> implements Table<K, V> {
  readonly store: string
  readonly #graph: ProcessorGraph
  /** The Processor that keeps the Table and forwards its updates. */
  readonly #node: string
  readonly #serdes: Serdes<K, V>

  constructor(graph: ProcessorGraph, node: string, store: string, serdes: Serdes<K, V>) {
    this.#graph = graph
    this.#node = node
    this.store = store
    this.#serdes = serdes
  }

  toStream(): Stream<K, V | null> {
    return new StreamNode(this.#graph, this.#node, withTombstones(this.#serdes))
  }
}

function required<T>(serde: Serde<T> | undefined, part: 'key' | 'value', topic: string): Serde<T> {
  if (serde === undefined) {
    throw new Error(`No ${part} Serde for output topic "${topic}": pass one to .to()`)
  }
  return serde
}

/** A Serde never sees null, so the Serdes of `V` are those of a Stream of `V | null` too. */
function withTombstones<K, V>(serdes: Serdes<K, V>): Serdes<K, NonNullable<V | null>> {
  return serdes as Serdes<K, NonNullable<V | null>>
}

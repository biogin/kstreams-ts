import { ProcessorGraph, type ProcessorSupplier } from './processor-graph.js'
import type { StreamRecord } from './record.js'
import { serdes as builtInSerdes, type Serde, type Serdes } from './serde.js'
import { defaultStoreSupplier, type NamedStore, type StateStoreSupplier } from './state-store.js'
import { type Sync, type SyncKeyValue, synchronous } from './sync-dsl.js'

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
   * they are kept in memory. The Test Driver ignores it (see `createTestDriver`).
   */
  stateDir?: string
}

/** The in-process object that defines and runs an application. */
export interface StreamsApp {
  readonly applicationId: string
  /** The configured state directory, if any. */
  readonly stateDir: string | undefined
  /** Declares a Stream read from `topic`, decoded with the given key and value Serdes. */
  stream<K, V>(topic: string, serdes: Serdes<K, V>): Stream<K, V>
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
  to(topic: string, serdes?: Partial<Serdes<K, V>>): void
  /**
   * Sends every record to a Processor and returns the Stream of records it forwards. Connecting
   * several Streams to one Processor merges them; each call returns that same output Stream. The
   * output Stream has no Serdes, so `.to()` after it needs both.
   */
  process<KOut, VOut>(processor: NamedProcessor<K, V, KOut, VOut>): Stream<KOut, VOut>
  /**
   * Groups the records by their current key, to aggregate them per key. Needs a key Serde for the
   * aggregation's State Store: pass one if this Stream has none (e.g. after `map`).
   */
  groupByKey(serdes?: Partial<Serdes<K, V>>): GroupedStream<K, V>
}

type KeyOf<R> = R extends { key: infer KOut } ? KOut : never
type ValueOf<R> = R extends { value: infer VOut } ? VOut : never

/** A Stream grouped by key, ready to be aggregated per key. */
export interface GroupedStream<K, V> {
  /**
   * Counts the records of each key into a Table. Records with a null key or a null value
   * (tombstone) are skipped and not counted.
   */
  count(options?: CountOptions): Table<K, number>
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

/** A keyed view where each record is an upsert of the latest value for its key. */
export interface Table<K, V> {
  /** The name of the State Store the Table is kept in. */
  readonly store: string
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
  const graph = new ProcessorGraph(applicationId)
  const app: StreamsApp = {
    applicationId,
    stateDir,
    stream(topic, serdes) {
      return new StreamNode(graph, graph.addSource(topic, serdes), serdes)
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
  return app
}

function addStore(
  graph: ProcessorGraph,
  name: string,
  serdes: Serdes<unknown, unknown>,
  supplier: StateStoreSupplier = defaultStoreSupplier,
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
  })
}

const graphs = new WeakMap<StreamsApp, ProcessorGraph>()

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
  /** Serdes known to fit this Stream's records, inherited from its input topic. */
  readonly #serdes: Partial<Serdes<K, V>>

  constructor(graph: ProcessorGraph, node: string, serdes: Partial<Serdes<K, V>>) {
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
    serdes: Partial<Serdes<KOut, VOut>>,
  ): Stream<KOut, VOut> {
    return new StreamNode(this.#graph, this.#graph.addProcessor(supplier, [this.#node]), serdes)
  }

  to(topic: string, serdes: Partial<Serdes<K, V>> = {}): void {
    const key = serdes.key ?? this.#serdes.key
    const value = serdes.value ?? this.#serdes.value
    this.#graph.addSink(
      topic,
      { key: required(key, 'key', topic), value: required(value, 'value', topic) },
      [this.#node],
    )
  }

  groupByKey(serdes: Partial<Serdes<K, V>> = {}): GroupedStream<K, V> {
    const key = serdes.key ?? this.#serdes.key
    if (key === undefined) {
      throw new Error('No key Serde for groupByKey: pass one to .groupByKey({ key })')
    }
    return new GroupedStreamNode(this.#graph, this.#node, key)
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

class GroupedStreamNode<K, V> implements GroupedStream<K, V> {
  readonly #graph: ProcessorGraph
  readonly #node: string
  readonly #key: Serde<K>

  constructor(graph: ProcessorGraph, node: string, key: Serde<K>) {
    this.#graph = graph
    this.#node = node
    this.#key = key
  }

  count(options: CountOptions = {}): Table<K, number> {
    const store = options.store ?? this.#graph.storeName('count')
    addStore(
      this.#graph,
      store,
      { key: this.#key, value: builtInSerdes.number },
      options.storeSupplier,
    )
    // Like Kafka Streams, aggregations skip records without a key or with a tombstone value.
    const supplier: ProcessorSupplier<K | null, V | null, K, number> = (ctx) => {
      const counts = ctx.store<K, number>(store)
      return {
        process({ key, value, timestamp }) {
          if (key === null || value === null) return
          const count = (counts.get(key) ?? 0) + 1
          counts.put(key, count)
          ctx.forward({ key, value: count, timestamp })
        },
      }
    }
    this.#graph.addProcessor(supplier, [this.#node])
    return { store }
  }
}

function required<T>(serde: Serde<T> | undefined, part: 'key' | 'value', topic: string): Serde<T> {
  if (serde === undefined) {
    throw new Error(`No ${part} Serde for output topic "${topic}": pass one to .to()`)
  }
  return serde
}

import { ProcessorGraph, type ProcessorSupplier } from './processor-graph.js'
import type { Serde, Serdes } from './serde.js'
import { type Sync, type SyncKeyValue, synchronous } from './sync-dsl.js'

export interface StreamsAppConfig {
  /**
   * The single name the application is identified by. Its consumer group, Internal Topic names,
   * transactional IDs and state directory all derive from it. Allowed characters: ASCII letters,
   * digits, `.`, `_` and `-`.
   */
  applicationId: string
}

/** The in-process object that defines and runs an application. */
export interface StreamsApp {
  readonly applicationId: string
  /** Declares a Stream read from `topic`, decoded with the given key and value Serdes. */
  stream<K, V>(topic: string, serdes: Serdes<K, V>): Stream<K, V>
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
}

type KeyOf<R> = R extends { key: infer KOut } ? KOut : never
type ValueOf<R> = R extends { value: infer VOut } ? VOut : never

const APPLICATION_ID = /^[A-Za-z0-9._-]+$/

export function createStreamsApp(config: StreamsAppConfig): StreamsApp {
  const { applicationId } = config
  if (typeof applicationId !== 'string' || !APPLICATION_ID.test(applicationId)) {
    throw new Error(
      `Invalid Application ID ${JSON.stringify(applicationId)}: use one or more ASCII letters, digits, ".", "_" or "-"`,
    )
  }
  const graph = new ProcessorGraph()
  const app: StreamsApp = {
    applicationId,
    stream(topic, serdes) {
      return new StreamNode(graph, graph.addSource(topic, serdes), serdes)
    },
  }
  graphs.set(app, graph)
  return app
}

const graphs = new WeakMap<StreamsApp, ProcessorGraph>()

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
}

function required<T>(serde: Serde<T> | undefined, part: 'key' | 'value', topic: string): Serde<T> {
  if (serde === undefined) {
    throw new Error(`No ${part} Serde for output topic "${topic}": pass one to .to()`)
  }
  return serde
}

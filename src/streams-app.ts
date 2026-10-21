import { ProcessorGraph } from './processor-graph.js'
import type { Serde, Serdes } from './serde.js'

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

/** An unbounded sequence of keyed records where each record is an independent event. */
export interface Stream<K, V> {
  /**
   * Writes every record to `topic`. Serdes not given here default to the ones this Stream was
   * read with.
   */
  to(topic: string, serdes?: Partial<Serdes<K, V>>): void
}

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

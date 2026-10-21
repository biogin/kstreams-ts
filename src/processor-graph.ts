// The Processor graph the DSL compiles to, and the public Processor API types it is built on.
// The graph itself is internal; only the types above `SerializedRecord` are exported.

import type { StreamRecord } from './record.js'
import type { Serdes } from './serde.js'
import type { KeyValueStore, NamedStore, StateStoreSupplier } from './state-store.js'

/**
 * Per-record logic. Receives records from its parents and sends results on with
 * `ProcessorContext.forward`.
 *
 * `process` may be async (for example to call an HTTP service). A Task awaits it before handing
 * the Processor its next record, so records within one Task never overlap.
 */
export interface Processor<KIn, VIn> {
  process(record: StreamRecord<KIn, VIn>): void | Promise<void>
}

/** When a scheduled timer fires: as Stream Time advances, or as wall-clock time passes. */
export type TimerType = 'stream-time' | 'wall-clock-time'

/**
 * Called when a timer fires, with the time it fired at: the Task's Stream Time for a Stream Time
 * timer, the wall-clock time for a wall-clock timer. May be async; the Task awaits it like a
 * Processor's `process`.
 */
export type TimerCallback = (timestamp: number) => void | Promise<void>

/** A timer returned by `ProcessorContext.schedule`. */
export interface Timer {
  /** Stops the timer. It never fires again. */
  cancel(): void
}

/** What a Processor can do besides receiving records. One context per Processor per Task. */
export interface ProcessorContext<KOut, VOut> {
  /**
   * Sends a record to every child of this Processor. Async children are queued and awaited by the
   * Task, so there is no need to await `forward`.
   */
  forward(record: StreamRecord<KOut, VOut>): void
  /**
   * A State Store of the Streams App, for this Task. Pass the store `app.store` returned for typed
   * access, or its name. Throws if the Streams App has no such store.
   */
  store<K, V>(store: NamedStore<K, V>): KeyValueStore<K, V>
  store<K = unknown, V = unknown>(name: string): KeyValueStore<K, V>
  /**
   * Schedules a recurring timer that fires every `intervalMs` (a positive number) after it is
   * scheduled, measured in Stream Time or in wall-clock time. If the time jumps over several
   * intervals at once, the timer fires once, with the current time. A Stream Time timer scheduled
   * before the Task has seen any record starts counting from the first record's Event Time.
   *
   * Timers fire between records, never while one is being processed, and records forwarded from
   * the callback go through the Processor's children as usual. They are not given a timestamp:
   * set one, usually the `timestamp` the callback receives.
   */
  schedule(intervalMs: number, type: TimerType, callback: TimerCallback): Timer
}

/** Creates one Processor instance per Task. */
export type ProcessorSupplier<KIn, VIn, KOut, VOut> = (
  context: ProcessorContext<KOut, VOut>,
) => Processor<KIn, VIn>

/** A record as it is on a topic: serialized key and value. A null value is a tombstone. */
export type SerializedRecord = StreamRecord<Uint8Array | null, Uint8Array | null>

// Inside the graph, record types are erased; the DSL keeps them typed at its surface.
// biome-ignore lint/suspicious/noExplicitAny: type erasure at the graph boundary
type AnySerdes = Serdes<any, any>
// biome-ignore lint/suspicious/noExplicitAny: type erasure at the graph boundary
type AnySupplier = ProcessorSupplier<any, any, any, any>

export interface SourceNode {
  kind: 'source'
  name: string
  topic: string
  serdes: AnySerdes
  children: string[]
}

export interface ProcessorNode {
  kind: 'processor'
  name: string
  supplier: AnySupplier
  children: string[]
}

export interface SinkNode {
  kind: 'sink'
  name: string
  topic: string
  serdes: AnySerdes
}

export type GraphNode = SourceNode | ProcessorNode | SinkNode

/** A State Store definition. Each Task opens its own store from it. */
export interface StoreNode {
  name: string
  serdes: AnySerdes
  supplier: StateStoreSupplier
  /** The topic the store's Serdes are told about: its Changelog. */
  changelogTopic: string
}

/** A directed graph of source, Processor and sink nodes. Built by the DSL, run by a Task. */
export class ProcessorGraph {
  readonly #nodes = new Map<string, GraphNode>()
  readonly #sourcesByTopic = new Map<string, SourceNode>()
  readonly #sinksByTopic = new Map<string, SinkNode[]>()
  readonly #stores = new Map<string, StoreNode>()
  #nextId = 0

  /** The Application ID of the Streams App the graph belongs to. */
  readonly applicationId: string

  constructor(applicationId: string) {
    this.applicationId = applicationId
  }

  get nodes(): ReadonlyMap<string, GraphNode> {
    return this.#nodes
  }

  /** Input topics, each with the Serdes its records are read with. */
  get sources(): ReadonlyMap<string, SourceNode> {
    return this.#sourcesByTopic
  }

  /** Output topics, each with the sinks that write to it. */
  get sinks(): ReadonlyMap<string, readonly SinkNode[]> {
    return this.#sinksByTopic
  }

  /** State Store definitions by name. */
  get stores(): ReadonlyMap<string, StoreNode> {
    return this.#stores
  }

  addStore(store: StoreNode): void {
    if (this.#stores.has(store.name)) {
      throw new Error(`A State Store named "${store.name}" already exists in this Streams App`)
    }
    this.#stores.set(store.name, store)
  }

  /** A State Store name not used yet. */
  storeName(prefix: string): string {
    let name: string
    do name = `${prefix}-store-${String(this.#nextId++).padStart(4, '0')}`
    while (this.#stores.has(name))
    return name
  }

  addSource(topic: string, serdes: AnySerdes): string {
    if (this.#sourcesByTopic.has(topic)) {
      throw new Error(`Input topic "${topic}" is already read by this Streams App`)
    }
    const node: SourceNode = {
      kind: 'source',
      name: this.#name('source'),
      topic,
      serdes,
      children: [],
    }
    this.#nodes.set(node.name, node)
    this.#sourcesByTopic.set(topic, node)
    return node.name
  }

  /** Adds a Processor node. Without a `name`, one is generated. */
  addProcessor(supplier: AnySupplier, parents: readonly string[], name?: string): string {
    if (name !== undefined && this.#nodes.has(name)) {
      throw new Error(`A Processor named "${name}" already exists in this Streams App`)
    }
    const node: ProcessorNode = {
      kind: 'processor',
      name: name ?? this.#name('processor'),
      supplier,
      children: [],
    }
    this.#attach(node, parents)
    return node.name
  }

  addSink(topic: string, serdes: AnySerdes, parents: readonly string[]): string {
    const node: SinkNode = { kind: 'sink', name: this.#name('sink'), topic, serdes }
    this.#attach(node, parents)
    const sinks = this.#sinksByTopic.get(topic) ?? []
    sinks.push(node)
    this.#sinksByTopic.set(topic, sinks)
    return node.name
  }

  /** Makes `child` (an existing node) receive the records `parent` forwards. */
  connect(parent: string, child: string): void {
    if (!this.#nodes.has(child)) throw new Error(`Unknown node "${child}"`)
    this.#parent(parent).children.push(child)
  }

  #attach(node: GraphNode, parents: readonly string[]): void {
    for (const parentName of parents) this.#parent(parentName).children.push(node.name)
    this.#nodes.set(node.name, node)
  }

  #parent(name: string): SourceNode | ProcessorNode {
    const parent = this.#nodes.get(name)
    if (parent === undefined) throw new Error(`Unknown parent node "${name}"`)
    if (parent.kind === 'sink') throw new Error(`Sink node "${name}" cannot have children`)
    return parent
  }

  #name(prefix: string): string {
    let name: string
    do name = `${prefix}-${String(this.#nextId++).padStart(4, '0')}`
    while (this.#nodes.has(name))
    return name
  }
}

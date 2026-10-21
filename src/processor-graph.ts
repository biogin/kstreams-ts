// Internal: the Processor graph the DSL compiles to. Not exported from the package entry point
// (the public Processor API arrives in ticket 06 and will be built on these types).

import type { StreamRecord } from './record.js'
import type { Serdes } from './serde.js'

/** Per-record logic. Receives records from its parents. */
export interface Processor<KIn, VIn> {
  process(record: StreamRecord<KIn, VIn>): void
}

/** What a Processor can do besides receiving records. */
export interface ProcessorContext<KOut, VOut> {
  /** Sends a record to every child of this Processor. */
  forward(record: StreamRecord<KOut, VOut>): void
}

/** Creates one Processor instance per Task. */
export type ProcessorSupplier<KIn, VIn, KOut, VOut> = (
  context: ProcessorContext<KOut, VOut>,
) => Processor<KIn, VIn>

/** A record as it is on a topic: serialized key and value. */
export type SerializedRecord = StreamRecord<Uint8Array, Uint8Array>

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

/** A directed graph of source, Processor and sink nodes. Built by the DSL, run by a Task. */
export class ProcessorGraph {
  readonly #nodes = new Map<string, GraphNode>()
  readonly #sourcesByTopic = new Map<string, SourceNode>()
  readonly #sinksByTopic = new Map<string, SinkNode[]>()
  #nextId = 0

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

  addProcessor(supplier: AnySupplier, parents: readonly string[]): string {
    const node: ProcessorNode = {
      kind: 'processor',
      name: this.#name('processor'),
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

  #attach(node: GraphNode, parents: readonly string[]): void {
    for (const parentName of parents) {
      const parent = this.#nodes.get(parentName)
      if (parent === undefined) throw new Error(`Unknown parent node "${parentName}"`)
      if (parent.kind === 'sink') throw new Error(`Sink node "${parentName}" cannot have children`)
      parent.children.push(node.name)
    }
    this.#nodes.set(node.name, node)
  }

  #name(prefix: string): string {
    return `${prefix}-${String(this.#nextId++).padStart(4, '0')}`
  }
}

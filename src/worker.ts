// Internal: a Worker, one consumer-group member that owns its Tasks, its producer and its
// consumer (ADR-0007). With `workers: 0` the Streams App runs one Worker in the main thread.
//
// A Task is created when its first record arrives. Its State Stores are restored from their
// Changelogs first (from their Checkpoints, or in full; see restore.ts and checkpoint.ts), between
// batches: a record for a new Task ends the batch it arrives in, and the next batch starts with
// that Task's Restore, before anything else is processed or produced. So no Kafka transaction or
// store transaction is open during a Restore.
//
// The commit cycle: read records into a batch until the commit interval runs out or the batch is
// full, and process each record in its Task.
// - At-least-once: then produce the batch's outputs and Changelog records, wait for the broker to
//   acknowledge them, and only then commit the consumed offsets. Then each Task writes the
//   Checkpoint of its persistent State Stores, with the Changelog offsets the broker acknowledged.
// - Exactly-once (ADR-0002): each Task's store writes go into a store transaction (for SQLite, an
//   open SQLite transaction) that lasts as long as the Kafka transaction. Outputs and Changelog
//   records go into the Worker's open Kafka transaction as they are produced; at the end of the
//   batch the consumed offsets (the last record per partition) join it and it commits. Only then
//   do the Tasks commit their store transactions and write their Checkpoints, with the Changelog
//   offsets of the transaction's sends. A failure aborts the Kafka transaction and rolls back the
//   store transactions, so neither `read_committed` readers nor the stores keep the batch.

import {
  Consumer,
  compatibilityPartitioner,
  FetchIsolationLevels,
  type Message,
  type MessagesStream,
  type MessageToProduce,
  ProduceAcks,
  Producer,
} from '@platformatic/kafka'
import { checkCoPartitioned } from './co-partitioning.js'
import { ensureChangelogs } from './internal-topics.js'
import type { ProcessorGraph, SerializedRecord } from './processor-graph.js'
import { createChangelogReader, type KafkaChangelogReader, restoreStores } from './restore.js'
import { type ChangelogOffsets, createTask, type Task } from './task.js'

export interface WorkerOptions {
  applicationId: string
  brokers: string[]
  /** Identifies this Worker's clients to the broker. */
  clientId: string
  /**
   * The Worker's transactional ID under exactly-once; at-least-once without one. Must stay the
   * same across restarts, so the new producer fences the old one.
   */
  transactionalId: string | undefined
  commitIntervalMs: number
  commitMaxRecords: number
  sessionTimeoutMs: number
  /** The replication factor of the Changelog topics the Worker creates. */
  changelogReplicationFactor: number
  /** The state directory State Stores are kept under, if any. */
  stateDir: string | undefined
  /** Called once if the Worker stops because of an error; it has closed its clients by then. */
  onError(error: unknown): void
}

export interface Worker {
  /**
   * Finishes the current batch (processing, producing and committing it), then leaves the
   * consumer group and closes the Worker's clients. Rejects if the Worker stopped with an error.
   */
  stop(): Promise<void>
}

/** How much longer than the commit interval a transaction may stay open. */
const TRANSACTION_TIMEOUT_MARGIN_MS = 10_000

/** Bytes, as the Kafka client wants them. */
type Bytes = Buffer
type ConsumedMessage = Message<Bytes, Bytes, Bytes, Bytes>
type Output = MessageToProduce<Bytes, Bytes, Bytes, Bytes>
/** The client doesn't export its transaction type. */
type Transaction = Awaited<ReturnType<Producer<Bytes, Bytes, Bytes, Bytes>['beginTransaction']>>

/**
 * Joins the consumer group (the Application ID) and starts the commit cycle. Resolves once the
 * Worker has joined; rejects (after closing its clients) if it could not.
 */
export async function startWorker(graph: ProcessorGraph, options: WorkerOptions): Promise<Worker> {
  const { applicationId, brokers, clientId, transactionalId } = options
  const consumer = new Consumer<Bytes, Bytes, Bytes, Bytes>({
    clientId,
    groupId: applicationId,
    bootstrapBrokers: brokers,
    // The commit cycle commits offsets itself, after the batch's outputs are acknowledged.
    autocommit: false,
    isolationLevel: FetchIsolationLevels.READ_COMMITTED,
    sessionTimeout: options.sessionTimeoutMs,
    heartbeatInterval: Math.max(100, Math.min(3_000, Math.floor(options.sessionTimeoutMs / 3))),
    maxWaitTime: 500,
  })
  let roundRobin = 0
  const producer = new Producer<Bytes, Bytes, Bytes, Bytes>({
    clientId,
    bootstrapBrokers: brokers,
    idempotent: true,
    acks: ProduceAcks.ALL,
    ...(transactionalId === undefined
      ? {}
      : {
          transactionalId,
          // The client also sends this as the transaction timeout, after which the broker aborts
          // an open transaction. It must outlast a batch (the client default is 5 s).
          timeout: options.commitIntervalMs + TRANSACTION_TIMEOUT_MARGIN_MS,
          // Without these the client asks the broker for producer ID 0 / epoch 0, which the broker
          // rejects as fenced once the transactional ID was used before (any restart).
          producerId: -1n,
          producerEpoch: -1,
        }),
    // Keyed records go where Java clients would put them (murmur2), so Co-partitioned topics
    // written by other producers line up; records without a key are spread round-robin.
    partitioner: (message, key) =>
      key === undefined ? roundRobin++ : compatibilityPartitioner(message, key),
  })

  // A restore consumer outside the group (ADR-0009), only for apps with State Stores.
  const restorer =
    graph.stores.size === 0
      ? undefined
      : createChangelogReader({ brokers, clientId: `${clientId}-restore` })

  const topics = [...graph.sources.keys()]
  let stream: MessagesStream<Bytes, Bytes, Bytes, Bytes>
  try {
    // Checked before joining the group, so a misconfigured app never takes any Task.
    await checkCoPartitioned(consumer, applicationId, topics)
    await ensureChangelogs(graph, {
      brokers,
      clientId,
      replicationFactor: options.changelogReplicationFactor,
    })
    // Fences the Worker's previous producer before the group's offsets are read and any
    // Changelog is restored: the broker aborts (or completes) a transaction it left open, e.g.
    // after a crash, so neither waits for that transaction's timeout.
    if (transactionalId !== undefined) await (await producer.beginTransaction()).cancel()
    stream = await consumer.consume({
      topics,
      // Resume from the committed offsets; a new application starts at the beginning.
      mode: 'committed',
      fallbackMode: 'earliest',
      autocommit: false,
    })
  } catch (error) {
    await Promise.allSettled([closeQuietly(consumer, producer), restorer?.close()])
    throw error
  }

  const cycle = new CommitCycle(
    graph,
    options,
    (fail) =>
      transactionalId === undefined
        ? new AtLeastOnceCommitter(consumer, producer)
        : new TransactionalCommitter(consumer, producer, fail),
    stream,
    restorer,
  )
  const done = cycle.run().then(
    () => close(),
    async (error: unknown) => {
      await close(true).catch(() => {})
      options.onError(error)
      throw error
    },
  )
  // `stop()` reports the error; `onError` already did if nobody calls it.
  done.catch(() => {})

  async function close(force = false): Promise<void> {
    try {
      await stream.close()
    } finally {
      // Closing the consumer leaves the group, so its Tasks move to other members right away.
      await Promise.allSettled([closeQuietly(consumer, producer, force), restorer?.close()])
    }
  }

  return {
    stop() {
      cycle.requestStop()
      return done
    },
  }
}

const TIME_UP = Symbol('time up')
const INTERRUPTED = Symbol('interrupted')

class CommitCycle {
  readonly #graph: ProcessorGraph
  readonly #options: WorkerOptions
  readonly #committer: BatchCommitter
  readonly #messages: AsyncIterator<ConsumedMessage>
  /** Reads the Changelogs of a new Task's State Stores; undefined without State Stores. */
  readonly #restorer: KafkaChangelogReader | undefined
  /** One Task per partition number, shared by all the Co-partitioned input topics. */
  readonly #tasks = new Map<number, Task>()

  #stopRequested = false
  /** Settles when a stop is requested or the batch fails, ending the batch early. */
  readonly #interrupted: Promise<typeof INTERRUPTED>
  #interrupt: () => void = () => {}

  /** The first error a Task or a send raised. Stops the Worker (fail-fast). */
  #failure: { error: unknown } | undefined
  // The current batch.
  #work: Promise<void>[] = []
  /** The last record read from each partition, by `topic:partition`. */
  #lastRead = new Map<string, ConsumedMessage>()
  /** A record for a new Task, which ended the batch it arrived in; the next batch starts with it. */
  #held: ConsumedMessage | undefined

  constructor(
    graph: ProcessorGraph,
    options: WorkerOptions,
    createCommitter: (fail: (error: unknown) => void) => BatchCommitter,
    stream: MessagesStream<Bytes, Bytes, Bytes, Bytes>,
    restorer: KafkaChangelogReader | undefined,
  ) {
    this.#graph = graph
    this.#restorer = restorer
    this.#options = options
    this.#committer = createCommitter((error) => this.#fail(error))
    this.#messages = stream[Symbol.asyncIterator]()
    this.#interrupted = new Promise((resolve) => {
      this.#interrupt = () => resolve(INTERRUPTED)
    })
  }

  requestStop(): void {
    this.#stopRequested = true
    this.#interrupt()
  }

  /** Remembers the first failure and ends the batch, which then fails (fail-fast). */
  #fail(error: unknown): void {
    this.#failure ??= { error }
    this.#interrupt()
  }

  async run(): Promise<void> {
    let next = this.#messages.next()
    // The read still pending when the cycle ends settles once the stream closes.
    try {
      while (!this.#stopRequested) {
        next = (await this.#readBatch(next)).next
        await this.#commit()
      }
    } catch (error) {
      // The error is what the Worker reports, not a failure to abort. A Kafka abort rolls back
      // the store transactions too; their writes' Changelog records are aborted with it.
      await this.#committer.abort().catch(() => {})
      this.#rollbackTasks()
      throw error
    } finally {
      next.catch(() => {})
      this.#closeTasks()
    }
  }

  #rollbackTasks(): void {
    for (const task of this.#tasks.values()) {
      try {
        task.rollback()
      } catch {
        // Rolling back the others matters more; closing an uncommitted store rolls it back too.
      }
    }
  }

  /**
   * Closes every Task's State Stores. After a failure they are left without a new Checkpoint
   * (under at-least-once, without any).
   */
  #closeTasks(): void {
    for (const task of this.#tasks.values()) {
      try {
        task.close()
      } catch {
        // Closing the others matters more; an unclosed store is unclean anyway.
      }
    }
    this.#tasks.clear()
  }

  /**
   * Reads and processes records until the commit interval runs out, the batch is full, a Task
   * fails or a stop is requested. Returns the read that is still pending.
   */
  async #readBatch(
    pending: Promise<IteratorResult<ConsumedMessage>>,
  ): Promise<{ next: Promise<IteratorResult<ConsumedMessage>> }> {
    const { commitIntervalMs, commitMaxRecords } = this.#options
    let count = 0
    const held = this.#held
    this.#held = undefined
    if (held !== undefined) {
      await this.#restored(held.partition)
      this.#process(held)
      count++
    }
    let timer: NodeJS.Timeout | undefined
    const timeUp = new Promise<typeof TIME_UP>((resolve) => {
      timer = setTimeout(() => resolve(TIME_UP), commitIntervalMs)
    })
    let next = pending
    try {
      for (; count < commitMaxRecords && this.#failure === undefined; count++) {
        const result = await Promise.race([next, timeUp, this.#interrupted])
        if (result === TIME_UP || result === INTERRUPTED) break
        if (result.done) throw new Error('The consumer stopped delivering records')
        next = this.#messages.next()
        const message = result.value
        if (this.#needsRestore(message.partition)) {
          // Restore only between batches: nothing of this batch may be produced or written yet.
          if (count > 0) {
            this.#held = message
            break
          }
          await this.#restored(message.partition)
        }
        this.#process(message)
      }
    } finally {
      clearTimeout(timer)
    }
    return { next }
  }

  /** Whether a record for `partition` creates a Task whose State Stores must be restored. */
  #needsRestore(partition: number): boolean {
    return this.#restorer !== undefined && !this.#tasks.has(partition)
  }

  /** Creates the Task of `partition` if needed, and waits for its Restore. */
  async #restored(partition: number): Promise<void> {
    await this.#task(partition).restored()
  }

  #process(message: ConsumedMessage): void {
    const record: SerializedRecord = {
      key: message.key ?? null,
      value: message.value ?? null,
      timestamp: Number(message.timestamp),
    }
    this.#track(this.#task(message.partition).process(message.topic, record))
    this.#lastRead.set(`${message.topic}:${message.partition}`, message)
  }

  /** Keeps a Task's work for the commit, remembering the first failure. */
  #track(work: Promise<void>): void {
    this.#work.push(work.catch((error: unknown) => this.#fail(error)))
  }

  #task(partition: number): Task {
    let task = this.#tasks.get(partition)
    if (task === undefined) {
      task = createTask(
        this.#graph,
        (topic, record) => this.#committer.produce(toMessage(topic, record)),
        Date.now,
        {
          task: partition,
          stateDir: this.#options.stateDir,
          // Exactly-once: store transactions as long as the Kafka transaction.
          storeTransactions: this.#options.transactionalId !== undefined,
          // A Task's State Stores are logged to the Changelog partition of the same number.
          changelog: (topic, record) =>
            this.#committer.produce({ ...toMessage(topic, record), partition }),
          // Every Task starts with a Restore, which the cycle waits for between batches.
          restore: (stores) => {
            const restorer = this.#restorer
            if (restorer === undefined) throw new Error('No restore consumer for State Stores')
            return restoreStores(stores, partition, restorer)
          },
        },
      )
      this.#tasks.set(partition, task)
    }
    return task
  }

  /**
   * Finishes the batch: waits for its processing, then commits its outputs, Changelog records and
   * offsets, then the Tasks' store transactions, then their Checkpoints.
   */
  async #commit(): Promise<void> {
    if (this.#failure === undefined) {
      for (const task of this.#tasks.values()) this.#track(task.fireWallClockTimers())
    }
    await Promise.all(this.#work)
    this.#work = []
    if (this.#failure !== undefined) throw this.#failure.error

    const lastRead = [...this.#lastRead.values()]
    this.#lastRead = new Map()
    const produced = await this.#committer.commit(lastRead)
    // Only once the batch is committed: a store must not get ahead of its Changelog (ADR-0002),
    // nor a Checkpoint ahead of its store.
    for (const task of this.#tasks.values()) task.commit()
    for (const [partition, task] of this.#tasks) {
      task.checkpoint(produced.get(partition) ?? new Map())
    }
  }
}

/** The offset after the last record a batch produced, by partition, then by topic. */
type ProducedOffsets = ReadonlyMap<number, ChangelogOffsets>

/** Commits a batch under one Delivery Guarantee. */
interface BatchCommitter {
  /** Takes an output of the current batch. */
  produce(output: Output): void
  /**
   * Commits the current batch: its outputs, and the offsets after `lastRead` (the last record
   * read from each partition). Rejects if any part of it failed. Resolves to the offset after the
   * last output in each partition that outputs given an explicit partition (the Changelog records)
   * went to. An offset may be lower than that, never higher: replaying a Changelog from too early
   * is harmless.
   */
  commit(lastRead: ConsumedMessage[]): Promise<ProducedOffsets>
  /** Gives up on the current batch after a failure. */
  abort(): Promise<void>
}

/** Produces the outputs, waits for the broker to acknowledge them, then commits the offsets. */
class AtLeastOnceCommitter implements BatchCommitter {
  readonly #consumer: Consumer<Bytes, Bytes, Bytes, Bytes>
  readonly #producer: Producer<Bytes, Bytes, Bytes, Bytes>
  #outputs: Output[] = []

  constructor(
    consumer: Consumer<Bytes, Bytes, Bytes, Bytes>,
    producer: Producer<Bytes, Bytes, Bytes, Bytes>,
  ) {
    this.#consumer = consumer
    this.#producer = producer
  }

  produce(output: Output): void {
    this.#outputs.push(output)
  }

  async commit(lastRead: ConsumedMessage[]): Promise<ProducedOffsets> {
    const outputs = this.#outputs
    this.#outputs = []
    let produced: ProducedOffsets = new Map()
    if (outputs.length > 0) {
      const result = await this.#producer.send({ messages: outputs })
      produced = producedOffsets(outputs, result.offsets ?? [])
    }
    if (lastRead.length > 0) {
      await this.#consumer.commit({
        offsets: lastRead.map(({ topic, partition, offset, leaderEpoch }) => ({
          topic,
          partition,
          offset: offset + 1n,
          leaderEpoch,
        })),
      })
    }
    return produced
  }

  async abort(): Promise<void> {
    this.#outputs = []
  }
}

/**
 * Sends the outputs into a Kafka transaction as they come, then adds the consumed offsets to it
 * and commits it (ADR-0002, ADR-0008). A transaction is only begun once there is something to
 * send or commit: the broker rejects ending an empty one.
 */
class TransactionalCommitter implements BatchCommitter {
  readonly #consumer: Consumer<Bytes, Bytes, Bytes, Bytes>
  readonly #producer: Producer<Bytes, Bytes, Bytes, Bytes>
  #transaction: Transaction | undefined
  /** Outputs waiting for the next send. */
  #pending: Output[] = []
  /** The chain of sends: one at a time, so outputs keep their order. */
  #sending: Promise<void> = Promise.resolve()
  #sendScheduled = false
  /** The first error a send raised. Fails the batch. */
  #sendFailure: { error: unknown } | undefined
  /** Reports a send error right away, so the batch ends early. */
  readonly #fail: (error: unknown) => void
  /** The offsets after the Changelog records sent into the open transaction. */
  #produced = new Map<number, Map<string, bigint>>()

  constructor(
    consumer: Consumer<Bytes, Bytes, Bytes, Bytes>,
    producer: Producer<Bytes, Bytes, Bytes, Bytes>,
    fail: (error: unknown) => void,
  ) {
    this.#consumer = consumer
    this.#producer = producer
    this.#fail = fail
  }

  produce(output: Output): void {
    this.#pending.push(output)
    if (this.#sendScheduled) return
    // Outputs produced while a send is in flight go out together in the next one.
    this.#sendScheduled = true
    this.#sending = this.#sending.then(() => this.#send())
  }

  async #send(): Promise<void> {
    this.#sendScheduled = false
    const messages = this.#pending
    this.#pending = []
    if (messages.length === 0 || this.#sendFailure !== undefined) return
    try {
      const transaction = await this.#begin()
      const result = await transaction.send({ messages })
      mergeProduced(this.#produced, producedOffsets(messages, result.offsets ?? []))
    } catch (error) {
      this.#sendFailure = { error }
      this.#fail(error)
    }
  }

  async #begin(): Promise<Transaction> {
    this.#transaction ??= await this.#producer.beginTransaction()
    return this.#transaction
  }

  async commit(lastRead: ConsumedMessage[]): Promise<ProducedOffsets> {
    await this.#sending
    if (this.#sendFailure !== undefined) throw this.#sendFailure.error
    if (lastRead.length > 0) {
      const transaction = await this.#begin()
      await transaction.addConsumer(this.#consumer)
      // One offset per partition, and one at a time: the client merges concurrent offset commits.
      for (const message of lastRead) await transaction.addOffset(this.#withCurrentGroup(message))
    }
    const transaction = this.#transaction
    const produced = this.#produced
    this.#transaction = undefined
    this.#produced = new Map()
    await transaction?.commit()
    return produced
  }

  /**
   * `message` with the consumer group's current generation instead of the one it was fetched
   * under, which a rebalance during the batch makes stale. Only valid for a partition this Worker
   * still owns; a revoked partition fails the batch (handling revokes is ticket 28, ADR-0006).
   */
  #withCurrentGroup(message: ConsumedMessage): ConsumedMessage {
    const { topic, partition } = message
    const consumer = this.#consumer
    const owned = consumer.assignments?.some(
      (assignment) => assignment.topic === topic && assignment.partitions.includes(partition),
    )
    if (!owned) {
      throw new Error(
        `Partition ${topic}-${partition} was revoked during the batch; aborting its transaction`,
      )
    }
    return {
      ...message,
      metadata: {
        ...message.metadata,
        consumer: {
          groupId: consumer.groupId,
          memberId: consumer.memberId,
          generationId: consumer.generationId,
          coordinatorId: consumer.coordinatorId,
        },
      },
    } as ConsumedMessage
  }

  async abort(): Promise<void> {
    await this.#sending
    this.#pending = []
    this.#produced = new Map()
    const transaction = this.#transaction
    this.#transaction = undefined
    await transaction?.abort()
  }
}

/**
 * The offset after the last of `outputs` in each partition they were explicitly sent to, from the
 * base offsets the broker acknowledged. One send writes one record batch per partition, so its
 * records follow the base offset; if a partition got several base offsets, the records after
 * the last one are not known, and only that one is counted.
 */
function producedOffsets(
  outputs: readonly Output[],
  acknowledged: readonly { topic: string; partition: number; offset: bigint }[],
): ProducedOffsets {
  const counts = new Map<string, number>()
  for (const { topic, partition } of outputs) {
    if (partition === undefined) continue
    const id = `${topic}:${partition}`
    counts.set(id, (counts.get(id) ?? 0) + 1)
  }
  const bases = new Map<string, bigint[]>()
  for (const { topic, partition, offset } of acknowledged) {
    const id = `${topic}:${partition}`
    bases.set(id, [...(bases.get(id) ?? []), offset])
  }
  const produced = new Map<number, Map<string, bigint>>()
  for (const { topic, partition, offset } of acknowledged) {
    const id = `${topic}:${partition}`
    const count = counts.get(id)
    const found = bases.get(id) ?? []
    if (count === undefined || offset !== found.reduce((a, b) => (a > b ? a : b))) continue
    const next = found.length === 1 ? offset + BigInt(count) : offset + 1n
    let byTopic = produced.get(partition)
    if (byTopic === undefined) {
      byTopic = new Map()
      produced.set(partition, byTopic)
    }
    byTopic.set(topic, next)
  }
  return produced
}

/** Adds `more` to `produced`, keeping the higher offset: later sends write later offsets. */
function mergeProduced(produced: Map<number, Map<string, bigint>>, more: ProducedOffsets): void {
  for (const [partition, offsets] of more) {
    let byTopic = produced.get(partition)
    if (byTopic === undefined) {
      byTopic = new Map()
      produced.set(partition, byTopic)
    }
    for (const [topic, offset] of offsets) {
      const known = byTopic.get(topic)
      if (known === undefined || offset > known) byTopic.set(topic, offset)
    }
  }
}

function toMessage(
  topic: string,
  record: SerializedRecord,
): MessageToProduce<Bytes, Bytes, Bytes, Bytes> {
  return {
    topic,
    ...(record.key === null ? {} : { key: toBuffer(record.key) }),
    // The client writes a null value as a tombstone.
    value: (record.value === null ? null : toBuffer(record.value)) as Bytes,
    timestamp: BigInt(record.timestamp),
  }
}

function toBuffer(bytes: Uint8Array): Buffer {
  return Buffer.isBuffer(bytes) ? bytes : Buffer.from(bytes.buffer, bytes.byteOffset, bytes.length)
}

async function closeQuietly(
  consumer: Consumer<Bytes, Bytes, Bytes, Bytes>,
  producer: Producer<Bytes, Bytes, Bytes, Bytes>,
  force = false,
): Promise<void> {
  await Promise.allSettled([consumer.close(force), producer.close()])
}

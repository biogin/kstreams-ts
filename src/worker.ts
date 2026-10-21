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
//
// Rebalances (ADR-0006): the client only reports them after the fact (see ownership.ts). A
// rebalance that loses partitions ends the batch. Records still buffered for lost partitions are
// dropped, and so are records fetched during a partition's earlier ownership, and records the
// client delivers again (after a rebalance it rewinds every partition it keeps to its committed
// offset).
// - Exactly-once: a batch with records of a lost partition aborts: Kafka abort, then SQLite
//   rollback. Every Task is then closed, since a rollback doesn't undo Stream Time, timers or
//   Processor state, and is created again (restored from its Checkpoint) when its records come
//   back: the Worker reads every partition again from its committed offset. A batch without such
//   records commits, with the consumer's current group generation.
// - At-least-once: the batch commits its outputs, and the offsets of the partitions still owned;
//   the new owner of a lost partition processes its records again.
// Either way the Tasks of lost partitions are closed; one that comes back is a new Task.
//
// A graceful stop ends the batch, commits it, closes the Tasks and leaves the group. If that takes
// longer than the shutdown timeout, the batch is given up (under exactly-once, aborted) and the
// Worker closes the Tasks and leaves the group anyway.

import {
  type ConsumeOptions,
  Consumer,
  compatibilityPartitioner,
  FetchIsolationLevels,
  findErrorBy,
  type Message,
  type MessagesStream,
  type MessageToProduce,
  ProduceAcks,
  Producer,
} from '@platformatic/kafka'
import { checkCoPartitioned } from './co-partitioning.js'
import { ensureChangelogs } from './internal-topics.js'
import { Ownership } from './ownership.js'
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
  /** How long `stop()` may take to commit the current batch before it gives the batch up. */
  shutdownTimeoutMs: number
  /** The replication factor of the Changelog topics the Worker creates. */
  changelogReplicationFactor: number
  /** The state directory State Stores are kept under, if any. */
  stateDir: string | undefined
  /** Called once if the Worker stops because of an error; it has closed its clients by then. */
  onError(error: unknown): void
}

export interface Worker {
  /**
   * Finishes the current batch (processing, producing and committing it), closes the Tasks, then
   * leaves the consumer group and closes the Worker's clients. If that takes longer than the
   * shutdown timeout, gives the batch up instead (under exactly-once, aborts it), and still closes
   * the Tasks and leaves the group. Rejects if the Worker stopped with an error.
   */
  stop(): Promise<void>
}

/** How much longer than the commit interval a transaction may stay open. */
const TRANSACTION_TIMEOUT_MARGIN_MS = 10_000

/** Bytes, as the Kafka client wants them. */
type Bytes = Buffer
type ConsumedMessage = Message<Bytes, Bytes, Bytes, Bytes>
type Output = MessageToProduce<Bytes, Bytes, Bytes, Bytes>
type Stream = MessagesStream<Bytes, Bytes, Bytes, Bytes>
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
  const consumeOptions: ConsumeOptions<Bytes, Bytes, Bytes, Bytes> = {
    topics,
    // Resume from the committed offsets; a new application starts at the beginning.
    mode: 'committed',
    fallbackMode: 'earliest',
    autocommit: false,
  }
  let stream: Stream
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
    stream = await consumer.consume(consumeOptions)
  } catch (error) {
    await Promise.allSettled([closeQuietly(consumer, producer), restorer?.close()])
    throw error
  }

  const cycle = new CommitCycle(graph, options, {
    consumer,
    createCommitter: (fail, owns) =>
      transactionalId === undefined
        ? new AtLeastOnceCommitter(consumer, producer)
        : new TransactionalCommitter(consumer, producer, fail, owns),
    stream,
    // While a stream is open, opening another joins no group: it starts from the committed
    // offsets of the partitions the consumer has.
    openStream: () => consumer.consume(consumeOptions),
    restorer,
  })
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
      await cycle.closeStream()
    } finally {
      // Closing the consumer leaves the group, so its Tasks move to other members right away.
      await Promise.allSettled([closeQuietly(consumer, producer, force), restorer?.close()])
    }
  }

  return {
    stop() {
      cycle.requestStop()
      return new Promise<void>((resolve, reject) => {
        const timer = setTimeout(() => {
          // The batch didn't commit in time (e.g. a Processor or a Restore is still busy): give
          // it up, and leave the group without waiting for it.
          cycle
            .abandon()
            .then(() => close(true))
            .then(resolve, reject)
        }, options.shutdownTimeoutMs)
        done.then(resolve, reject).finally(() => clearTimeout(timer))
      })
    },
  }
}

const TIME_UP = Symbol('time up')
const INTERRUPTED = Symbol('interrupted')

/** Thrown when a batch would commit the offset of a partition the Worker no longer owns. */
class PartitionRevokedError extends Error {}

/**
 * Whether `error` means the group moved on (a rebalance) while the batch was committing, so the
 * batch can be given up and the Worker go on.
 */
function isRebalanceError(error: unknown): boolean {
  if (error instanceof PartitionRevokedError) return true
  return error instanceof Error && findErrorBy(error, 'needsRejoin', true) !== null
}

function partitionKey(topic: string, partition: number): string {
  return `${topic}:${partition}`
}

interface CommitCycleClients {
  consumer: Consumer<Bytes, Bytes, Bytes, Bytes>
  createCommitter(
    fail: (error: unknown) => void,
    owns: (topic: string, partition: number) => boolean,
  ): BatchCommitter
  stream: Stream
  /** Opens another stream of the same topics, from the committed offsets. */
  openStream(): Promise<Stream>
  /** Reads the Changelogs of a new Task's State Stores; undefined without State Stores. */
  restorer: KafkaChangelogReader | undefined
}

class CommitCycle {
  readonly #graph: ProcessorGraph
  readonly #options: WorkerOptions
  readonly #exactlyOnce: boolean
  readonly #committer: BatchCommitter
  readonly #ownership: Ownership
  readonly #openStream: () => Promise<Stream>
  #stream: Stream
  #messages: AsyncIterator<ConsumedMessage>
  /** The read pending on `#messages`. */
  #next: Promise<IteratorResult<ConsumedMessage>> = new Promise(() => {})
  /** Reads the Changelogs of a new Task's State Stores; undefined without State Stores. */
  readonly #restorer: KafkaChangelogReader | undefined
  /** One Task per partition number, shared by all the Co-partitioned input topics. */
  readonly #tasks = new Map<number, Task>()

  #stopRequested = false
  /** Set once a stop gave the batch up: nothing is committed after that. */
  #abandoned = false
  /** Settles when a stop is requested or the batch fails, ending the batch early. */
  readonly #interrupted: Promise<typeof INTERRUPTED>
  #interrupt: () => void = () => {}
  /** Ends the current batch early, when partitions are lost. */
  #endBatch: () => void = () => {}

  /** The first error a Task or a send raised. Stops the Worker (fail-fast). */
  #failure: { error: unknown } | undefined
  // The current batch.
  #work: Promise<void>[] = []
  /** The last record read from each partition, by `topic:partition`. */
  #lastRead = new Map<string, ConsumedMessage>()
  /**
   * A record that ended the batch it arrived in (for a new Task, or after partitions were lost);
   * the next batch starts with it.
   */
  #held: ConsumedMessage | undefined
  /**
   * The offset after the last record processed, by `topic:partition`. A record before it was
   * processed already: the client delivers it again after a rebalance.
   */
  #position = new Map<string, bigint>()
  /** The offset after the last record committed, by `topic:partition`. */
  #committedPosition = new Map<string, bigint>()

  constructor(graph: ProcessorGraph, options: WorkerOptions, clients: CommitCycleClients) {
    this.#graph = graph
    this.#options = options
    this.#exactlyOnce = options.transactionalId !== undefined
    this.#restorer = clients.restorer
    this.#openStream = clients.openStream
    this.#stream = clients.stream
    this.#messages = clients.stream[Symbol.asyncIterator]()
    this.#ownership = new Ownership(clients.consumer, () => this.#endBatch())
    this.#committer = clients.createCommitter(
      (error) => this.#fail(error),
      (topic, partition) => this.#ownership.owns(topic, partition),
    )
    this.#interrupted = new Promise((resolve) => {
      this.#interrupt = () => resolve(INTERRUPTED)
    })
  }

  requestStop(): void {
    this.#stopRequested = true
    this.#interrupt()
  }

  /**
   * Gives the current batch up, for a stop that ran out of time: aborts it, rolls back and closes
   * the Tasks. Whatever the cycle is still waiting for commits nothing after that.
   */
  async abandon(): Promise<void> {
    this.#abandoned = true
    this.requestStop()
    await this.#committer.abort().catch(() => {})
    this.#closeTasks()
  }

  /** Closes the stream the cycle reads. */
  async closeStream(): Promise<void> {
    await this.#stream.close()
  }

  /** Remembers the first failure and ends the batch, which then fails (fail-fast). */
  #fail(error: unknown): void {
    this.#failure ??= { error }
    this.#interrupt()
  }

  async run(): Promise<void> {
    this.#next = this.#messages.next()
    try {
      while (!this.#stopRequested) {
        await this.#readBatch()
        await this.#commit()
      }
    } catch (error) {
      // After a stop gave up, what was still running fails against closed stores and clients.
      if (this.#abandoned) return
      // The error is what the Worker reports, not a failure to abort. A Kafka abort rolls back
      // the store transactions too; their writes' Changelog records are aborted with it.
      await this.#committer.abort().catch(() => {})
      throw error
    } finally {
      // The read still pending when the cycle ends settles once the stream closes.
      this.#next.catch(() => {})
      // After a failure the stores are left without a new Checkpoint (at-least-once: any).
      this.#closeTasks()
      this.#ownership.close()
    }
  }

  /** Rolls back and closes every Task. */
  #closeTasks(): void {
    for (const task of this.#tasks.values()) closeTask(task)
    this.#tasks.clear()
  }

  /**
   * Reads and processes records until the commit interval runs out, the batch is full, a Task
   * fails, partitions are lost or a stop is requested.
   */
  async #readBatch(): Promise<void> {
    const { commitIntervalMs, commitMaxRecords } = this.#options
    const ownership = this.#ownership
    // Lost partitions are dealt with first, by committing (or aborting) the batch so far.
    if (ownership.lostPending) return
    let count = 0
    const held = this.#held
    this.#held = undefined
    if (held !== undefined && this.#accepts(held)) {
      await this.#restored(held.partition)
      if (ownership.lostPending) {
        this.#held = held
        return
      }
      this.#process(held)
      count++
    }
    let timer: NodeJS.Timeout | undefined
    const timeUp = new Promise<typeof TIME_UP>((resolve) => {
      timer = setTimeout(() => resolve(TIME_UP), commitIntervalMs)
    })
    const lost = new Promise<typeof INTERRUPTED>((resolve) => {
      this.#endBatch = () => resolve(INTERRUPTED)
    })
    try {
      while (count < commitMaxRecords && this.#failure === undefined) {
        const result = await Promise.race([this.#next, timeUp, this.#interrupted, lost])
        if (result === TIME_UP || result === INTERRUPTED) break
        if (result.done) throw new Error('The consumer stopped delivering records')
        this.#next = this.#messages.next()
        const message = result.value
        if (ownership.lostPending) {
          this.#held = message
          break
        }
        if (!this.#accepts(message)) continue
        if (this.#needsRestore(message.partition)) {
          // Restore only between batches: nothing of this batch may be produced or written yet.
          if (count > 0) {
            this.#held = message
            break
          }
          await this.#restored(message.partition)
          if (ownership.lostPending) {
            this.#held = message
            break
          }
        }
        this.#process(message)
        count++
      }
    } finally {
      clearTimeout(timer)
      this.#endBatch = () => {}
    }
  }

  /**
   * Whether to process `message`: it was fetched while its partition is owned (not left over
   * from a revoked or an earlier ownership), and wasn't processed already.
   */
  #accepts(message: ConsumedMessage): boolean {
    const position = this.#position.get(partitionKey(message.topic, message.partition))
    return (
      this.#ownership.current(message) && (position === undefined || message.offset >= position)
    )
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
    const key = partitionKey(message.topic, message.partition)
    this.#lastRead.set(key, message)
    this.#position.set(key, message.offset + 1n)
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
          storeTransactions: this.#exactlyOnce,
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
   * offsets, then the Tasks' store transactions, then their Checkpoints. Deals with the partitions
   * lost since the last commit (see the top of this file).
   */
  async #commit(): Promise<void> {
    const lost = this.#ownership.takeLost()
    if (this.#failure === undefined) {
      for (const [partition, task] of this.#tasks) {
        if (!lost.has(partition)) this.#track(task.fireWallClockTimers())
      }
    }
    await Promise.all(this.#work)
    this.#work = []
    if (this.#failure !== undefined) throw this.#failure.error

    const lastRead = [...this.#lastRead.values()]
    this.#lastRead = new Map()
    // Exactly-once: a transaction with records of a lost partition must not commit (ADR-0006).
    if (this.#exactlyOnce && lastRead.some(({ partition }) => lost.has(partition))) {
      await this.#abortBatch(lost)
      return
    }
    const committing = this.#exactlyOnce
      ? lastRead
      : lastRead.filter(({ topic, partition }) => this.#ownership.owns(topic, partition))
    let result: CommitResult
    try {
      result = await this.#committer.commit(committing)
    } catch (error) {
      // Exactly-once: partitions were lost while the batch was committing.
      if (!this.#exactlyOnce || this.#abandoned || !isRebalanceError(error)) throw error
      await this.#abortBatch(new Set([...lost, ...this.#ownership.takeLost()]))
      return
    }
    // A stop that gave up has closed (and rolled back) the stores: no Checkpoint for them.
    if (this.#abandoned) return
    // Only once the batch is committed: a store must not get ahead of its Changelog (ADR-0002),
    // nor a Checkpoint ahead of its store.
    for (const task of this.#tasks.values()) task.commit()
    for (const [partition, task] of this.#tasks) {
      task.checkpoint(result.produced.get(partition) ?? new Map())
    }
    for (const message of committing) {
      const key = partitionKey(message.topic, message.partition)
      if (result.offsetsCommitted) {
        this.#committedPosition.set(key, message.offset + 1n)
      } else if (!this.#lastRead.has(key)) {
        // At-least-once, a rebalance got in the way: the next batch commits them.
        this.#lastRead.set(key, message)
      }
    }
    this.#forget(lost)
  }

  /**
   * Exactly-once: aborts the batch, closes every Task and reads every partition again from its
   * committed offset.
   */
  async #abortBatch(lost: ReadonlySet<number>): Promise<void> {
    await this.#committer.abort()
    // Rolled back, a Task would still have the batch's Stream Time, timers and Processor state.
    this.#closeTasks()
    this.#held = undefined
    this.#position = new Map(this.#committedPosition)
    this.#forget(lost)
    if (!this.#stopRequested) await this.#rewind()
  }

  /** Closes the Tasks of the `lost` partitions and forgets how far they were read. */
  #forget(lost: ReadonlySet<number>): void {
    if (lost.size === 0) return
    for (const partition of lost) {
      const task = this.#tasks.get(partition)
      if (task !== undefined) closeTask(task)
      this.#tasks.delete(partition)
    }
    for (const positions of [this.#position, this.#committedPosition]) {
      for (const key of positions.keys()) {
        if (lost.has(Number(key.slice(key.lastIndexOf(':') + 1)))) positions.delete(key)
      }
    }
  }

  /**
   * Reads every owned partition again from its committed offset, with a new stream: the records
   * of an aborted batch must come again, and the client only rewinds on a rebalance.
   */
  async #rewind(): Promise<void> {
    const previous = this.#stream
    this.#stream = await this.#openStream()
    this.#next.catch(() => {})
    this.#messages = this.#stream[Symbol.asyncIterator]()
    this.#next = this.#messages.next()
    await previous.close().catch(() => {})
  }
}

/** Rolls back and closes `task`. Errors are ignored: closing the others matters more. */
function closeTask(task: Task): void {
  try {
    task.rollback()
  } catch {
    // Closing an uncommitted store rolls it back too.
  }
  try {
    task.close()
  } catch {
    // An unclosed store is unclean anyway.
  }
}

/** The offset after the last record a batch produced, by partition, then by topic. */
type ProducedOffsets = ReadonlyMap<number, ChangelogOffsets>

interface CommitResult {
  /**
   * The offset after the last output in each partition that outputs given an explicit partition
   * (the Changelog records) went to. An offset may be lower than that, never higher: replaying a
   * Changelog from too early is harmless.
   */
  produced: ProducedOffsets
  /** False if the outputs committed but the offsets didn't, because of a rebalance. */
  offsetsCommitted: boolean
}

/** Commits a batch under one Delivery Guarantee. */
interface BatchCommitter {
  /** Takes an output of the current batch. */
  produce(output: Output): void
  /**
   * Commits the current batch: its outputs, and the offsets after `lastRead` (the last record
   * read from each partition). Rejects if any part of it failed, except that at-least-once may
   * leave the offsets uncommitted after a rebalance.
   */
  commit(lastRead: ConsumedMessage[]): Promise<CommitResult>
  /** Gives up on the current batch. */
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

  async commit(lastRead: ConsumedMessage[]): Promise<CommitResult> {
    const outputs = this.#outputs
    this.#outputs = []
    let produced: ProducedOffsets = new Map()
    if (outputs.length > 0) {
      const result = await this.#producer.send({ messages: outputs })
      produced = producedOffsets(outputs, result.offsets ?? [])
    }
    if (lastRead.length > 0) {
      try {
        // After a rebalance error the client joins the group again and retries by itself.
        await this.#consumer.commit({
          offsets: lastRead.map(({ topic, partition, offset, leaderEpoch }) => ({
            topic,
            partition,
            offset: offset + 1n,
            leaderEpoch,
          })),
        })
      } catch (error) {
        if (!isRebalanceError(error)) throw error
        return { produced, offsetsCommitted: false }
      }
    }
    return { produced, offsetsCommitted: true }
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
  /** Whether the Worker owns `topic:partition` now. */
  readonly #owns: (topic: string, partition: number) => boolean

  constructor(
    consumer: Consumer<Bytes, Bytes, Bytes, Bytes>,
    producer: Producer<Bytes, Bytes, Bytes, Bytes>,
    fail: (error: unknown) => void,
    owns: (topic: string, partition: number) => boolean,
  ) {
    this.#consumer = consumer
    this.#producer = producer
    this.#fail = fail
    this.#owns = owns
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

  async commit(lastRead: ConsumedMessage[]): Promise<CommitResult> {
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
    return { produced, offsetsCommitted: true }
  }

  /**
   * `message` with the consumer group's current generation instead of the one it was fetched
   * under, which a rebalance during the batch makes stale (as Java's `groupMetadata()`). Only
   * valid for a partition the Worker still owns; for a lost one the batch aborts (ADR-0006).
   */
  #withCurrentGroup(message: ConsumedMessage): ConsumedMessage {
    const { topic, partition } = message
    const consumer = this.#consumer
    if (!this.#owns(topic, partition)) {
      throw new PartitionRevokedError(
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

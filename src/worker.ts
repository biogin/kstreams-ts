// Internal: a Worker, one consumer-group member that owns its Tasks, its producer and its
// consumer (ADR-0007). With `workers: 0` the Streams App runs one Worker in the main thread.
//
// A Task is created when its first record arrives. A Task that has State Stores, or any Task under
// exactly-once, is first started in the background (see "Starting a Task" below), while the
// Worker's other Tasks keep processing and committing (ADR-0009).
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
// Tasks are assigned whole and cooperative-sticky (task-assignor.ts): a rebalance moves as few
// Tasks as it can, and a Task that moves is first revoked from its old owner, then assigned to its
// new one in a second rebalance.
//
// Starting a Task (its first record, under exactly-once or with State Stores) runs in the
// background, outside the commit cycle, and waits for the Task's previous owner, which may be
// another Worker or process:
// - for its Task directory lock, when there is a state directory (state-directory.ts): the
//   previous owner closes the Task, and so unlocks it, once it notices it lost the Task. Only then
//   are the Task's State Stores opened;
// - under exactly-once, for its stable committed offsets (stable-offsets.ts): records before them
//   were processed and committed already, and are skipped;
// - for its Changelog partitions to have no transaction left open from before, then for its
//   Restore (restore.ts, from the Checkpoints or in full; see checkpoint.ts).
// The starting Task is not part of any batch: it processes nothing, so it has no store
// transaction, and nothing of it is in the Worker's Kafka transaction (which the other Tasks'
// batches keep using meanwhile); Restore's own per-chunk store transactions never nest. Since the
// waits don't hold the commit cycle, two Workers waiting for each other's Tasks never deadlock:
// each goes on, notices what it lost, and closes it.
//
// Meanwhile the starting Task's records are held in memory, in arrival order, and processed once
// it has started (the client can't pause a single partition; ADR-0009). The bound: all starting
// Tasks of a Worker together hold at most `commitMaxRecords` records, as many as one batch
// processes. A starting Task that would go over it holds nothing more (it drops every later
// record, so its held records stay without gaps) and, once started, reads its partitions again:
// the Worker rewinds its stream to the committed offsets, as after an abort, and drops what it had
// processed already. Once started, the Task joins the current batch like any Task. A start is
// cancelled when its partitions are lost, or the Worker stops; the Task's lock is only released
// once its Restore has stopped writing, and a later start of the same Task waits for that.
//
// Rebalances (ADR-0006): the client only reports them after the fact (see ownership.ts). A
// rebalance that loses partitions ends the batch. Records still buffered for lost partitions are
// dropped, and so are records fetched during a partition's earlier ownership, and records the
// client delivers again (after a rebalance it rewinds every partition it keeps to its committed
// offset, if it has one).
// - Exactly-once: a batch that anything of a Task whose ownership was interrupted had a part in
//   (records it processed, outputs, Changelog writes, timer-driven writes; see `Ownership.epoch`)
//   aborts: Kafka abort, then SQLite rollback. This is checked after the batch's processing, after
//   each wait while committing, and right before the commit. Every started Task is then closed,
//   since a rollback doesn't undo Stream Time, timers or Processor state, and is started again
//   (restored from its Checkpoint) when its records come back: the Worker reads every partition
//   again from its committed offset. Tasks still starting had no part in the batch and go on
//   starting. Any other batch commits, with the consumer's current group generation, and with
//   offsets of every Task that had a part in it (`fencingOffsets` for those with only timer-driven
//   writes: their committed offsets, known from what the Worker committed or from the stable
//   offsets the Task started from), so the group coordinator fences a Worker that is no longer a
//   member. A Task with nothing committed yet that read no record in the batch keeps its
//   wall-clock timers due until it has (see `#fenceable`).
// - At-least-once: the batch commits its outputs, and the offsets of the partitions still owned;
//   the new owner of a lost partition processes its records again.
// Either way the Tasks of lost partitions are closed, and their starts cancelled; one that comes
// back is a new Task. Then the Worker reads every partition again from its committed offset, with
// a new stream: a lost partition that comes back without a committed offset (nothing of it
// committed yet) would otherwise go on from where the client was, past the records the Worker
// dropped, and they would never be processed.
//
// A graceful stop ends the batch, commits it, closes the Tasks, cancels the starts (a Restore stops
// after its current fetch) and leaves the group. If that takes longer than the shutdown timeout,
// the batch is given up (under exactly-once, aborted) and the Worker closes the Tasks and leaves
// the group anyway.
//
// Exactly-once, a batch's transaction must end before the transaction timeout, or the broker
// aborts it and the Worker's producer epoch with it. The batch reads records for at most the
// timeout minus a margin (`transactionBudget`). Queued async work can still outlast that, so the
// Worker learns how many records a batch can process within the budget and admits no more
// (`#maxBatchRecords`). A batch that still runs too long is aborted (by the Worker if it can still
// see it coming, else by the broker) and read again, like a batch aborted by a rebalance, and the
// producer takes up its transactional ID again with its last epoch: the broker then fences it if
// another producer took the ID over since (a zombie), which stays fatal (`TransactionalCommitter`).
//
// Observability (observability.ts): the Worker reports its state, events and metrics through
// `WorkerOptions.report`. Its state is `rebalancing` from the client's `consumer:group:rebalance`
// (a rebalance began) to its `consumer:group:join` (it completed), else `restoring` while a Task
// with State Stores is starting, else `running`. Task counters are reported once their batch has
// committed; consumer lag is measured every few seconds, in the background.

import {
  COOPERATIVE_STICKY_ASSIGNOR,
  type ConsumeOptions,
  type Consumer,
  compatibilityPartitioner,
  FetchIsolationLevels,
  findErrorBy,
  ListOffsetTimestamps,
  type Message,
  type MessagesStream,
  type MessageToProduce,
  ProduceAcks,
  Producer,
} from '@platformatic/kafka'
import { checkCoPartitioned } from './co-partitioning.js'
import { ensureChangelogs } from './internal-topics.js'
import {
  type AbortReason,
  type Metric,
  noTaskCounters,
  type TaskCounters,
  taskCounterMetrics,
  type WaitKind,
  type WorkerReport,
  type WorkerState,
} from './observability.js'
import { Ownership } from './ownership.js'
import type { OutputRecord, ProcessorGraph, SerializedRecord } from './processor-graph.js'
import { ReadCommittedConsumer } from './read-committed.js'
import {
  createChangelogReader,
  type KafkaChangelogReader,
  type RestoreListener,
  restoreStores,
} from './restore.js'
import { createStableOffsetReader, type StableOffsetReader } from './stable-offsets.js'
import { lockTask, unlockTask } from './state-directory.js'
import { type ChangelogOffsets, createTask, type Task } from './task.js'
import { assignTasks } from './task-assignor.js'
import {
  fromTopicPartitionKey,
  type TopicPartitionKey,
  topicPartitionKey,
} from './topic-partition.js'

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
  /** Exactly-once: the transaction timeout, after which the broker aborts an open transaction. */
  transactionTimeoutMs: number
  /** The most bytes of records one produce request carries (see `splitBySize`). */
  maxProduceBytes: number
  sessionTimeoutMs: number
  /** How long `stop()` may take to commit the current batch before it gives the batch up. */
  shutdownTimeoutMs: number
  /** The replication factor of the Changelog topics the Worker creates. */
  changelogReplicationFactor: number
  /** Whether the Worker creates missing Internal Topics (true) or only checks them (false). */
  autoCreateInternalTopics: boolean
  /** The state directory State Stores are kept under, if any. */
  stateDir: string | undefined
  /** Names the Worker in the Task directory locks it takes (see state-directory.ts). */
  lockHolder: string
  /** The Worker's number in its App Process, which labels its events and metrics. */
  workerNumber: number
  /** Whether the Worker reports metrics (only when there is a metrics sink for them). */
  collectMetrics: boolean
  /** Receives the Worker's state, events and metrics (see observability.ts). */
  report(report: WorkerReport): void
  /**
   * Called once if the Worker stops because of an error, with the first error (the root cause), as
   * soon as it happens: before the Worker has rolled back and closed its clients, so that what
   * closing them causes (here or in other Workers) can't be reported before it.
   */
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

/** The most of the transaction timeout a batch keeps in reserve, after it stops reading. */
const TRANSACTION_TIMEOUT_MARGIN_MS = 10_000
/** The most of the transaction timeout a batch keeps in reserve for committing. */
const COMMIT_MARGIN_MS = 1_000

/**
 * Exactly-once: how long a batch may read records and process them: the transaction timeout less
 * a margin for the processing still queued and for committing.
 */
export function transactionBudget(timeoutMs: number): number {
  return timeoutMs - Math.min(TRANSACTION_TIMEOUT_MARGIN_MS, timeoutMs / 2)
}

/** Exactly-once: how long a transaction may have been open and still be committed. */
function commitDeadline(timeoutMs: number): number {
  return timeoutMs - Math.min(COMMIT_MARGIN_MS, timeoutMs / 10)
}
/** How often the consumer lag is measured, at most. */
const LAG_INTERVAL_MS = 2_000

/** Bytes, as the Kafka client wants them. */
type Bytes = Buffer
type ConsumedMessage = Message<Bytes, Bytes, Bytes, Bytes>
export type Output = MessageToProduce<Bytes, Bytes, Bytes, Bytes>
export type Stream = MessagesStream<Bytes, Bytes, Bytes, Bytes>
/** The client doesn't export its transaction type. */
type Transaction = Awaited<ReturnType<Producer<Bytes, Bytes, Bytes, Bytes>['beginTransaction']>>

/**
 * Joins the consumer group (the Application ID) and starts the commit cycle. Resolves once the
 * Worker has joined; rejects (after closing its clients) if it could not.
 */
export async function startWorker(graph: ProcessorGraph, options: WorkerOptions): Promise<Worker> {
  const { applicationId, brokers, clientId, transactionalId } = options
  const consumer = new ReadCommittedConsumer<Bytes, Bytes, Bytes, Bytes>({
    clientId,
    groupId: applicationId,
    bootstrapBrokers: brokers,
    // The commit cycle commits offsets itself, after the batch's outputs are acknowledged.
    autocommit: false,
    // read_committed, with aborted transactions filtered by read-committed.ts rather than by the
    // client (platformatic/kafka#467).
    isolationLevel: FetchIsolationLevels.READ_COMMITTED,
    sessionTimeout: options.sessionTimeoutMs,
    heartbeatInterval: Math.max(100, Math.min(3_000, Math.floor(options.sessionTimeoutMs / 3))),
    maxWaitTime: 500,
    // Whole Tasks, cooperative-sticky (task-assignor.ts). Version 3 subscriptions carry the
    // partitions each member owns, and the generation it owned them in.
    protocols: [{ name: COOPERATIVE_STICKY_ASSIGNOR, version: 3 }],
    partitionAssigner: assignTasks,
  })
  // A failed rejoin of the group in the background is emitted as an 'error' event, which would be
  // an uncaught exception without a listener. It fails the commit cycle, unless the Worker is
  // stopping or closing already: then it's a consequence of leaving the group, not an error.
  let closing = false
  let onClientError: (error: unknown) => void = () => {}
  consumer.on('error', (error: unknown) => {
    if (!closing) onClientError(error)
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
          timeout: options.transactionTimeoutMs,
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
  // Exactly-once: where a new Task starts, once its previous owner's transaction has completed.
  const stableOffsets =
    transactionalId === undefined
      ? undefined
      : createStableOffsetReader({
          brokers,
          clientId: `${clientId}-offsets`,
          groupId: applicationId,
        })

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
      applicationId,
      brokers,
      clientId,
      replicationFactor: options.changelogReplicationFactor,
      autoCreate: options.autoCreateInternalTopics,
    })
    // Fences the Worker's previous producer before the group's offsets are read and any
    // Changelog is restored: the broker aborts (or completes) a transaction it left open, e.g.
    // after a crash, so neither waits for that transaction's timeout.
    if (transactionalId !== undefined) await (await producer.beginTransaction()).cancel()
    stream = await consumer.consume(consumeOptions)
  } catch (error) {
    await Promise.allSettled([
      closeQuietly(consumer, producer),
      restorer?.close(),
      stableOffsets?.close(),
    ])
    throw error
  }

  const cycle = new CommitCycle(graph, options, {
    consumer,
    createCommitter: (fail, owns) =>
      transactionalId === undefined
        ? new AtLeastOnceCommitter(consumer, producer, options.maxProduceBytes)
        : new TransactionalCommitter(consumer, producer, {
            maxBytes: options.maxProduceBytes,
            timeoutMs: options.transactionTimeoutMs,
            fail,
            owns,
          }),
    stream,
    // While a stream is open, opening another joins no group: it starts from the committed
    // offsets of the partitions the consumer has.
    openStream: () => consumer.consume(consumeOptions),
    restorer,
    stableOffsets,
  })
  onClientError = (error) => cycle.clientFailed(error)
  const done = cycle.run().then(
    () => close(),
    async (error: unknown) => {
      await close(true).catch(() => {})
      throw error
    },
  )
  // `stop()` reports the error; the cycle's `onError` already did if nobody calls it.
  done.catch(() => {})

  async function close(force = false): Promise<void> {
    closing = true
    try {
      await cycle.closeStream()
    } finally {
      // Closing the consumer leaves the group, so its Tasks move to other members right away.
      await Promise.allSettled([
        closeQuietly(consumer, producer, force),
        restorer?.close(),
        stableOffsets?.close(),
      ])
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
const STARTED = Symbol('started')
const PROGRESS = Symbol('progress')

/** A Task starting in the background (see the top of this file). */
interface StartingTask {
  readonly partition: number
  /** Cancels the start: its waits give up, and its Restore stops. */
  readonly cancel: AbortController
  /** Its records, in arrival order, until it has started. */
  readonly held: ConsumedMessage[]
  /**
   * The offset after the last record held, by `topic:partition`: a record the stream delivers
   * again (after a rewind) isn't held twice.
   */
  readonly heldUpTo: Map<TopicPartitionKey, bigint>
  /** Whether it dropped records because the bound was reached: it then reads them again. */
  overflowed: boolean
  /** Exactly-once: its stable committed offsets, by topic. */
  stable: ReadonlyMap<string, bigint> | undefined
  /** Created once its directory is locked; restoring until `started`. */
  task: Task | undefined
  /** Set once it has started; the cycle then takes it over. */
  started: boolean
  /** Settles once the background start has ended, whether it started, failed or was cancelled. */
  done: Promise<void>
}

/** Thrown when a batch would commit the offset of a partition the Worker no longer owns. */
class PartitionRevokedError extends Error {}

/**
 * Exported for the unit tests (ADR-0016). Exactly-once: the batch's transaction outlived (or was
 * about to outlive) the transaction timeout. The batch is aborted and read again.
 */
export class TransactionTimedOutError extends Error {}

/**
 * The errors a broker answers a producer with once its epoch was bumped: by the broker aborting
 * its transaction after the timeout, or by another producer taking its transactional ID over.
 */
const EPOCH_ERRORS = ['INVALID_PRODUCER_EPOCH', 'PRODUCER_FENCED', 'TRANSACTION_ABORTABLE']

/** Whether `error` is (or contains) one of `EPOCH_ERRORS`. */
function isEpochError(error: unknown): boolean {
  return (
    error instanceof Error && EPOCH_ERRORS.some((id) => findErrorBy(error, 'apiId', id) !== null)
  )
}

/**
 * Whether `error` means the group moved on (a rebalance) while the batch was committing, so the
 * batch can be given up and the Worker go on.
 */
function isRebalanceError(error: unknown): boolean {
  if (error instanceof PartitionRevokedError) return true
  return error instanceof Error && findErrorBy(error, 'needsRejoin', true) !== null
}

/** Exported for the Worker-level unit tests (ADR-0016), like `CommitCycle`. */
export interface CommitCycleClients {
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
  /** Exactly-once: reads the stable committed offsets of a new Task's partitions. */
  stableOffsets: StableOffsetReader | undefined
}

/** Exported for the Worker-level unit tests only (ADR-0016): not part of the API. */
export class CommitCycle {
  readonly #graph: ProcessorGraph
  readonly #options: WorkerOptions
  readonly #exactlyOnce: boolean
  readonly #committer: BatchCommitter
  readonly #ownership: Ownership
  readonly #consumer: Consumer<Bytes, Bytes, Bytes, Bytes>
  readonly #openStream: () => Promise<Stream>
  #stream: Stream
  #messages: AsyncIterator<ConsumedMessage>
  /** The read pending on `#messages`. */
  #next: Promise<IteratorResult<ConsumedMessage>> = new Promise(() => {})
  /** Reads the Changelogs of a new Task's State Stores; undefined without State Stores. */
  readonly #restorer: KafkaChangelogReader | undefined
  /** Exactly-once: reads the stable committed offsets of a new Task's partitions. */
  readonly #stableOffsets: StableOffsetReader | undefined
  /**
   * One started Task per partition number, shared by all the Co-partitioned input topics. A Task
   * is in here or in `#starting`, never in both.
   */
  readonly #tasks = new Map<number, Task>()
  /** The Tasks starting in the background, by partition. */
  readonly #starting = new Map<number, StartingTask>()
  /** Cancelled starts still winding down (their Restores stopping), by partition. */
  readonly #cancelling = new Map<number, Promise<void>>()
  /** How many records the starting Tasks hold, together: at most as many as a batch reads. */
  #heldCount = 0
  /** Settles once a starting Task has started; renewed whenever the cycle takes them over. */
  #started!: Promise<typeof STARTED>
  #signalStarted: () => void = () => {}
  /** The Tasks whose directories this Worker has locked, started or starting. */
  readonly #locked = new Set<number>()

  #stopRequested = false
  /** Set once a stop gave the batch up: nothing is committed after that. */
  #abandoned = false
  /** Settles when a stop is requested or the batch fails, ending the batch early. */
  readonly #interrupted: Promise<typeof INTERRUPTED>
  #interrupt: () => void = () => {}
  /** Ends the current batch early, when partitions are lost. */
  #endBatch: () => void = () => {}

  /** When the current batch began (`performance.now()`), and how many records it processed. */
  #batchStartedAt = 0
  #batchRecords = 0
  /** How many of the current batch's records are processed, and when its first one was taken. */
  #batchSettled = 0
  #batchFirstAt: number | undefined
  /** Settles once one of the batch's records is processed; renewed each time. */
  #progress!: Promise<typeof PROGRESS>
  #signalProgress: () => void = () => {}
  /**
   * Exactly-once: the most records a batch reads, learned from how long batches took, so that
   * they stay within the transaction timeout (see `#fitBatch`). At most `commitMaxRecords`.
   */
  #maxBatchRecords: number

  /** The first error a Task or a send raised. Stops the Worker (fail-fast). */
  #failure: { error: unknown } | undefined
  // The current batch.
  #work: Promise<void>[] = []
  /** The last record read from each partition, by `topic:partition`. */
  #lastRead = new Map<TopicPartitionKey, ConsumedMessage>()
  /**
   * The Tasks that have a part in the batch, by partition number: they processed a record of it,
   * or wrote outputs or Changelog records into it (from a timer too). Each with the ownership
   * epoch it was created under (see `Ownership.epoch`), or -1 if Tasks of two epochs took part.
   */
  #contributors = new Map<number, number>()
  /** The ownership epoch each Task was created under. */
  #taskEpochs = new WeakMap<Task, number>()
  /**
   * A record that ended the batch it arrived in, after partitions were lost; the next batch starts
   * with it.
   */
  #held: ConsumedMessage | undefined
  /**
   * The offset after the last record processed, by `topic:partition`. A record before it was
   * processed already: the client delivers it again after a rebalance.
   */
  #position = new Map<TopicPartitionKey, bigint>()
  /** The offset after the last record committed, by `topic:partition`. */
  #committedPosition = new Map<TopicPartitionKey, bigint>()

  /** Between the client's `consumer:group:rebalance` and its `consumer:group:join`. */
  #rebalancing = false
  /** The state last reported. */
  #reportedState: WorkerState | undefined
  /** Each started Task's counters as last reported, by partition. */
  readonly #reportedCounters = new Map<number, TaskCounters>()
  /** When the consumer lag was last measured (`Date.now()`), and whether that is still going on. */
  #lagMeasuredAt = 0
  #measuringLag = false
  readonly #onRebalance = () => {
    this.#rebalancing = true
    this.#updateState()
  }
  readonly #onJoin = () => {
    this.#rebalancing = false
    this.#updateState()
  }

  constructor(graph: ProcessorGraph, options: WorkerOptions, clients: CommitCycleClients) {
    this.#graph = graph
    this.#options = options
    this.#exactlyOnce = options.transactionalId !== undefined
    this.#maxBatchRecords = options.commitMaxRecords
    this.#restorer = clients.restorer
    this.#stableOffsets = clients.stableOffsets
    this.#openStream = clients.openStream
    this.#stream = clients.stream
    this.#messages = clients.stream[Symbol.asyncIterator]()
    this.#consumer = clients.consumer
    // Before the listeners below, so that a rebalance's lost partitions are known first.
    this.#ownership = new Ownership(
      clients.consumer,
      () => this.#endBatch(),
      ({ assigned, revoked }) =>
        this.#event({
          name: 'rebalance',
          payload: { worker: options.workerNumber, assigned, revoked },
        }),
    )
    clients.consumer.on('consumer:group:rebalance', this.#onRebalance)
    clients.consumer.on('consumer:group:join', this.#onJoin)
    // The round the Worker joined the group in completed before the cycle could follow it.
    this.#event({
      name: 'rebalance',
      payload: { worker: options.workerNumber, assigned: this.#ownership.owned(), revoked: [] },
    })
    this.#committer = clients.createCommitter(
      (error) => this.#fail(error),
      (topic, partition) => this.#ownership.owns(topic, partition),
    )
    this.#interrupted = new Promise((resolve) => {
      this.#interrupt = () => resolve(INTERRUPTED)
    })
    this.#renewStarted()
    this.#renewProgress()
    this.#updateState()
  }

  /** Reports the Worker's state if it changed (see the top of this file). */
  #updateState(): void {
    const state: WorkerState = this.#rebalancing
      ? 'rebalancing'
      : this.#graph.stores.size > 0 && this.#starting.size > 0
        ? 'restoring'
        : 'running'
    if (state === this.#reportedState) return
    this.#reportedState = state
    this.#options.report({ type: 'state', state })
  }

  #event(event: Extract<WorkerReport, { type: 'event' }>['event']): void {
    this.#options.report({ type: 'event', event })
  }

  #metric(metric: Metric): void {
    if (this.#options.collectMetrics) this.#options.report({ type: 'metric', metric })
  }

  #countAbort(reason: AbortReason): void {
    this.#metric({
      name: 'aborts',
      type: 'counter',
      value: 1,
      labels: { worker: this.#options.workerNumber, reason },
    })
  }

  #wait(task: number, wait: WaitKind, ms: number): void {
    this.#metric({
      name: 'wait-duration',
      type: 'duration',
      value: ms,
      labels: { worker: this.#options.workerNumber, task, wait },
    })
  }

  /** Reports what the started Tasks counted since last time; called once their batch committed. */
  #reportCounters(): void {
    if (!this.#options.collectMetrics) return
    for (const [partition, task] of this.#tasks) {
      const now = task.metrics()
      const before = this.#reportedCounters.get(partition) ?? noTaskCounters()
      for (const metric of taskCounterMetrics(this.#options.workerNumber, partition, now, before)) {
        this.#metric(metric)
      }
      this.#reportedCounters.set(partition, now)
    }
  }

  /** Measures the consumer lag in the background, unless it was measured recently. */
  #measureLagSometimes(): void {
    if (!this.#options.collectMetrics || this.#measuringLag || this.#stopRequested) return
    const now = Date.now()
    if (now - this.#lagMeasuredAt < LAG_INTERVAL_MS) return
    this.#lagMeasuredAt = now
    this.#measuringLag = true
    this.#measureLag()
      .catch(() => {
        // Measured again next time.
      })
      .finally(() => {
        this.#measuringLag = false
      })
  }

  /**
   * Reports the records of each owned partition after its committed offset (or its start, before
   * any commit), up to its last stable offset.
   */
  async #measureLag(): Promise<void> {
    const byTopic = new Map<string, number[]>()
    for (const { topic, partition } of this.#ownership.owned()) {
      byTopic.set(topic, [...(byTopic.get(topic) ?? []), partition])
    }
    if (byTopic.size === 0) return
    const consumer = this.#consumer
    const topics = [...byTopic.keys()]
    const partitions = Object.fromEntries(byTopic)
    const [latest, committed] = await Promise.all([
      consumer.listOffsets({
        topics,
        partitions,
        isolationLevel: FetchIsolationLevels.READ_COMMITTED,
      }),
      consumer.listCommittedOffsets({
        topics: [...byTopic].map(([topic, partitions]) => ({ topic, partitions })),
      }),
    ])
    const uncommitted = [...byTopic].some(([topic, numbers]) =>
      numbers.some((partition) => (committed.get(topic)?.[partition] ?? -1n) < 0n),
    )
    const earliest = uncommitted
      ? await consumer.listOffsets({
          topics,
          partitions,
          timestamp: ListOffsetTimestamps.EARLIEST,
        })
      : undefined
    for (const [topic, numbers] of byTopic) {
      for (const partition of numbers) {
        const end = latest.get(topic)?.[partition]
        const known = committed.get(topic)?.[partition] ?? -1n
        const from = known >= 0n ? known : earliest?.get(topic)?.[partition]
        if (end === undefined || from === undefined) continue
        this.#metric({
          name: 'consumer-lag',
          type: 'gauge',
          value: Number(end > from ? end - from : 0n),
          labels: { worker: this.#options.workerNumber, topic, partition },
        })
      }
    }
  }

  #renewProgress(): void {
    this.#progress = new Promise((resolve) => {
      this.#signalProgress = () => {
        this.#renewProgress()
        resolve(PROGRESS)
      }
    })
  }

  #renewStarted(): void {
    this.#started = new Promise((resolve) => {
      this.#signalStarted = () => resolve(STARTED)
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
    if (await this.#committer.abort().catch(() => false)) this.#countAbort('shutdown-timeout')
    this.#closeTasks()
    this.#cancelStarts()
  }

  /** Closes the stream the cycle reads. */
  async closeStream(): Promise<void> {
    await this.#stream.close()
  }

  /**
   * A client failed in the background (e.g. the consumer could not rejoin the group): fails the
   * Worker, unless it is stopping, when that's a consequence of the stop.
   */
  clientFailed(error: unknown): void {
    if (!this.#stopRequested) this.#fail(error)
  }

  /**
   * Remembers the first failure, the root cause, reports it at once, and ends the batch, which
   * then fails (fail-fast). Later errors are consequences (closed stores, cancelled starts, a
   * rolled-back transaction) and are dropped, as is everything once a stop gave the batch up.
   */
  #fail(error: unknown): void {
    if (this.#failure !== undefined || this.#abandoned) return
    this.#failure = { error }
    this.#interrupt()
    this.#options.onError(error)
  }

  async run(): Promise<void> {
    this.#next = this.#messages.next()
    try {
      while (!this.#stopRequested) {
        await this.#readBatch()
        await this.#commit()
        this.#measureLagSometimes()
      }
    } catch (error) {
      // After a stop gave up, what was still running fails against closed stores and clients.
      if (this.#abandoned) return
      // The Worker reports its first failure (the root cause), not whatever ended the cycle.
      this.#fail(error)
      const failure = this.#failure ?? { error }
      // Not a failure to abort either. A Kafka abort rolls back the store transactions too; their
      // writes' Changelog records are aborted with it.
      if (await this.#committer.abort().catch(() => false)) this.#countAbort('error')
      throw failure.error
    } finally {
      // The read still pending when the cycle ends settles once the stream closes.
      this.#next.catch(() => {})
      // After a failure the stores are left without a new Checkpoint (at-least-once: any).
      this.#closeTasks()
      this.#cancelStarts()
      this.#ownership.close()
      this.#consumer.off('consumer:group:rebalance', this.#onRebalance)
      this.#consumer.off('consumer:group:join', this.#onJoin)
      // Restores stop after their current fetch; the Worker closes the restore consumer after.
      await Promise.all(this.#cancelling.values())
    }
  }

  /** Rolls back and closes every started Task, and unlocks their directories. */
  #closeTasks(): void {
    for (const partition of [...this.#tasks.keys()]) this.#closeTask(partition)
  }

  /** Rolls back and closes the Task of `partition`, if open, and unlocks its directory. */
  #closeTask(partition: number): void {
    const task = this.#tasks.get(partition)
    if (task !== undefined) closeTask(task)
    this.#tasks.delete(partition)
    this.#reportedCounters.delete(partition)
    this.#unlock(partition)
  }

  #unlock(partition: number): void {
    const { stateDir, applicationId, lockHolder } = this.#options
    if (!this.#locked.delete(partition) || stateDir === undefined) return
    try {
      unlockTask(stateDir, applicationId, partition, lockHolder)
    } catch {
      // The Streams App removes its Task locks once its Workers have stopped.
    }
  }

  /**
   * Reads and processes records until the commit interval runs out, the batch is full, a Task
   * fails, partitions are lost or a stop is requested.
   */
  async #readBatch(): Promise<void> {
    this.#batchStartedAt = performance.now()
    this.#batchRecords = 0
    this.#batchSettled = 0
    this.#batchFirstAt = undefined
    const { commitIntervalMs, commitMaxRecords, transactionTimeoutMs } = this.#options
    // Exactly-once, within the transaction timeout (see the top of this file).
    const maxRecords = this.#exactlyOnce ? this.#maxBatchRecords : commitMaxRecords
    const readMs = this.#exactlyOnce
      ? Math.min(commitIntervalMs, transactionBudget(transactionTimeoutMs))
      : commitIntervalMs
    const ownership = this.#ownership
    // Lost partitions are dealt with first, by committing (or aborting) the batch so far.
    if (ownership.lostPending) return
    let timer: NodeJS.Timeout | undefined
    const timeUp = new Promise<typeof TIME_UP>((resolve) => {
      timer = setTimeout(() => resolve(TIME_UP), readMs)
    })
    const lost = new Promise<typeof INTERRUPTED>((resolve) => {
      this.#endBatch = () => resolve(INTERRUPTED)
    })
    const until = [timeUp, this.#interrupted, lost]
    try {
      let count = await this.#takeStarted(until)
      const held = this.#held
      this.#held = undefined
      if (held !== undefined && this.#take(held)) count++
      while (count < maxRecords && this.#failure === undefined) {
        const result = await Promise.race([
          this.#next,
          timeUp,
          this.#interrupted,
          lost,
          this.#started,
        ])
        if (result === TIME_UP || result === INTERRUPTED) break
        if (result === STARTED) {
          if (ownership.lostPending) break
          count += await this.#takeStarted(until)
          continue
        }
        if (result.done) throw new Error('The consumer stopped delivering records')
        this.#next = this.#messages.next()
        const message = result.value
        if (ownership.lostPending) {
          this.#held = message
          break
        }
        // Exactly-once: the next batch starts with it if the batch can't take it in time.
        if (this.#exactlyOnce && !(await this.#admits(until))) {
          this.#held = message
          break
        }
        if (this.#take(message)) count++
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
    const position = this.#position.get(topicPartitionKey(message.topic, message.partition))
    return (
      this.#ownership.current(message) && (position === undefined || message.offset >= position)
    )
  }

  /**
   * Takes `message` (one the Worker accepts): processes it in its Task, or holds it while its Task
   * starts, starting the Task first if needed. Returns whether it was processed.
   */
  #take(message: ConsumedMessage): boolean {
    if (!this.#accepts(message)) return false
    const { partition } = message
    let starting = this.#starting.get(partition)
    if (starting === undefined && this.#needsStart(partition)) starting = this.#start(partition)
    if (starting !== undefined) {
      this.#hold(starting, message)
      return false
    }
    this.#process(message)
    return true
  }

  /**
   * Whether a record for `partition` creates a Task that must be started first: under
   * exactly-once, or with State Stores.
   */
  #needsStart(partition: number): boolean {
    return (
      (this.#restorer !== undefined || this.#stableOffsets !== undefined) &&
      !this.#tasks.has(partition)
    )
  }

  /** Holds `message` for its starting Task, within the bound (see the top of this file). */
  #hold(starting: StartingTask, message: ConsumedMessage): void {
    if (starting.overflowed) return
    const key = topicPartitionKey(message.topic, message.partition)
    const upTo = starting.heldUpTo.get(key)
    if (upTo !== undefined && message.offset < upTo) return
    if (this.#heldCount >= this.#maxBatchRecords) {
      starting.overflowed = true
      return
    }
    starting.held.push(message)
    starting.heldUpTo.set(key, message.offset + 1n)
    this.#heldCount++
  }

  /** Starts the Task of `partition` in the background (see the top of this file). */
  #start(partition: number): StartingTask {
    const starting: StartingTask = {
      partition,
      cancel: new AbortController(),
      held: [],
      heldUpTo: new Map(),
      overflowed: false,
      stable: undefined,
      task: undefined,
      started: false,
      done: Promise.resolve(),
    }
    const { signal } = starting.cancel
    starting.done = this.#prepare(starting, this.#cancelling.get(partition)).then(
      () => {
        if (signal.aborted) {
          this.#dispose(starting)
        } else {
          starting.started = true
          this.#signalStarted()
        }
      },
      (error: unknown) => {
        this.#dispose(starting)
        if (!signal.aborted) this.#fail(error)
      },
    )
    this.#starting.set(partition, starting)
    this.#updateState()
    return starting
  }

  /**
   * Once an earlier, cancelled start of the same Task has wound down: locks the Task's directory,
   * reads its stable committed offsets, creates it and waits for its Restore. Returns early, with
   * the Task not started, once the start is cancelled.
   */
  async #prepare(starting: StartingTask, previous: Promise<void> | undefined): Promise<void> {
    await previous
    const { partition } = starting
    const { signal } = starting.cancel
    const giveUp = () => signal.aborted
    const { stateDir, applicationId, lockHolder } = this.#options
    if (stateDir !== undefined && this.#graph.stores.size > 0) {
      const waiting = performance.now()
      if (!(await lockTask(stateDir, applicationId, partition, lockHolder, giveUp))) return
      this.#wait(partition, 'task-lock', performance.now() - waiting)
      this.#locked.add(partition)
    }
    if (this.#stableOffsets !== undefined) {
      const topics = [...this.#graph.sources.keys()]
      const waiting = performance.now()
      starting.stable = await this.#stableOffsets.read(topics, partition, giveUp)
      if (starting.stable === undefined) return
      this.#wait(partition, 'stable-offsets', performance.now() - waiting)
    }
    if (signal.aborted) return
    starting.task = this.#createTask(partition, signal)
    await starting.task.restored()
  }

  /**
   * Takes the Tasks that have started over: processes the records they held, after skipping what
   * their stable committed offsets say was processed, and rewinds the stream for those that
   * dropped records. Exactly-once, a held record the batch can't take in time (see `#admits`,
   * waiting at most until one of `until` settles) is dropped too, and read again. Returns how many
   * records were processed.
   */
  async #takeStarted(until: Promise<unknown>[]): Promise<number> {
    this.#renewStarted()
    let count = 0
    let rewind = false
    let full = false
    for (const starting of [...this.#starting.values()]) {
      if (!starting.started || starting.task === undefined) continue
      const { partition } = starting
      this.#starting.delete(partition)
      this.#heldCount -= starting.held.length
      this.#tasks.set(partition, starting.task)
      for (const [topic, offset] of starting.stable ?? []) {
        const key = topicPartitionKey(topic, partition)
        // Committed already: processed, and an offset to fence the Task's timer-driven writes.
        for (const positions of [this.#position, this.#committedPosition]) {
          const position = positions.get(key)
          if (position === undefined || offset > position) positions.set(key, offset)
        }
      }
      for (const message of starting.held) {
        if (!full && this.#exactlyOnce) full = !(await this.#admits(until))
        if (full) {
          rewind = true
          break
        }
        if (this.#accepts(message)) {
          this.#process(message)
          count++
        }
      }
      rewind ||= starting.overflowed
    }
    this.#updateState()
    if (rewind) await this.#rewind()
    return count
  }

  /** Cancels the starts of the Tasks of `partitions` (by default, all). */
  #cancelStarts(partitions?: ReadonlySet<number>): void {
    for (const starting of [...this.#starting.values()]) {
      if (partitions !== undefined && !partitions.has(starting.partition)) continue
      const { partition, done } = starting
      starting.cancel.abort(new Error(`The start of Task ${partition} was cancelled`))
      this.#starting.delete(partition)
      this.#heldCount -= starting.held.length
      if (starting.started) {
        this.#dispose(starting)
      } else {
        // Winding down: a new start of the Task waits for it.
        this.#cancelling.set(partition, done)
        void done.then(() => {
          if (this.#cancelling.get(partition) === done) this.#cancelling.delete(partition)
        })
      }
    }
    this.#updateState()
  }

  /** Closes the Task of a start that won't be taken over, and unlocks its directory. */
  #dispose(starting: StartingTask): void {
    if (starting.task !== undefined) closeTask(starting.task)
    this.#unlock(starting.partition)
  }

  #process(message: ConsumedMessage): void {
    const record: SerializedRecord = {
      key: message.key ?? null,
      value: message.value ?? null,
      timestamp: Number(message.timestamp),
    }
    const task = this.#task(message.partition)
    this.#batchRecords++
    this.#contribute(message.partition, this.#taskEpochs.get(task) ?? -1)
    this.#batchFirstAt ??= performance.now()
    const work = task.process(message.topic, record, message.offset)
    this.#track(work)
    work.then(
      () => {
        this.#batchSettled++
        this.#signalProgress()
      },
      () => {},
    )
    const key = topicPartitionKey(message.topic, message.partition)
    this.#lastRead.set(key, message)
    this.#position.set(key, message.offset + 1n)
  }

  /** Notes that the Task of `partition`, created under ownership epoch `epoch`, has a part in the batch. */
  #contribute(partition: number, epoch: number): void {
    const known = this.#contributors.get(partition)
    this.#contributors.set(partition, known === undefined || known === epoch ? epoch : -1)
  }

  /** Keeps a Task's work for the commit, remembering the first failure. */
  #track(work: Promise<void>): void {
    this.#work.push(work.catch((error: unknown) => this.#fail(error)))
  }

  /** The started Task of `partition`, created if it needs no start. */
  #task(partition: number): Task {
    let task = this.#tasks.get(partition)
    if (task === undefined) {
      task = this.#createTask(partition)
      this.#tasks.set(partition, task)
    }
    return task
  }

  /** Creates the Task of `partition`; `signal` cancels its Restore. */
  #createTask(partition: number, signal?: AbortSignal): Task {
    // Every write of the Task gives it a part in the batch, under the epoch it was created in: a
    // Task left over from an ownership that has ended must not commit anything (ADR-0006).
    const epoch = this.#ownership.epoch(partition)
    const produce = (output: Output) => {
      this.#contribute(partition, epoch)
      this.#committer.produce(output)
    }
    const task = createTask(
      this.#graph,
      (topic, record) => produce(toMessage(topic, record)),
      Date.now,
      {
        task: partition,
        stateDir: this.#options.stateDir,
        // Exactly-once: store transactions as long as the Kafka transaction.
        storeTransactions: this.#exactlyOnce,
        // A Task's State Stores are logged to the Changelog partition of the same number.
        changelog: (topic, record) => produce({ ...toMessage(topic, record), partition }),
        // Every Task with State Stores starts with a Restore, in the background.
        restore: (stores) => {
          const restorer = this.#restorer
          if (restorer === undefined) throw new Error('No restore consumer for State Stores')
          return restoreStores(
            stores,
            partition,
            restorer,
            signal,
            this.#restoreListener(partition),
          )
        },
      },
    )
    this.#taskEpochs.set(task, epoch)
    return task
  }

  /** Reports the Restore of Task `task` as events and metrics. */
  #restoreListener(task: number): RestoreListener {
    const worker = this.#options.workerNumber
    /** The records applied so far, by store, as last reported. */
    const reported = new Map<string, number>()
    const progress = (store: string, restored: number, remaining: number) => {
      const labels = { worker, task, store }
      const more = restored - (reported.get(store) ?? 0)
      reported.set(store, restored)
      if (more > 0) this.#metric({ name: 'records-restored', type: 'counter', value: more, labels })
      this.#metric({ name: 'restore-remaining', type: 'gauge', value: remaining, labels })
    }
    return {
      waited: (ms) => this.#wait(task, 'changelog-transactions', ms),
      started: (store, full, remaining) => {
        this.#event({ name: 'restore-start', payload: { worker, task, store, full, remaining } })
        progress(store, 0, remaining)
      },
      progressed: (store, restored, remaining) => {
        this.#event({
          name: 'restore-progress',
          payload: { worker, task, store, restored, remaining },
        })
        progress(store, restored, remaining)
      },
      ended: (store, restored, durationMs, cancelled) => {
        this.#event({
          name: 'restore-end',
          payload: { worker, task, store, restored, durationMs, cancelled },
        })
        if (!cancelled) progress(store, restored, 0)
      },
    }
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
        if (!lost.has(partition) && this.#fenceable(partition)) {
          this.#track(task.fireWallClockTimers())
        }
      }
    }
    await Promise.all(this.#work)
    this.#work = []
    if (this.#failure !== undefined) throw this.#failure.error
    if (this.#exactlyOnce) this.#fitBatch(performance.now() - this.#batchStartedAt)

    const lastRead = [...this.#lastRead.values()]
    this.#lastRead = new Map()
    const contributors = this.#contributors
    this.#contributors = new Map()
    let guard: CommitGuard | undefined
    if (this.#exactlyOnce) {
      // A transaction with anything of a Task whose ownership was interrupted since (records,
      // outputs, Changelog records, timer-driven writes alike) must not commit (ADR-0006). The
      // committer checks again after each of its own waits, up to the commit.
      const ownedThroughout = () => this.#ownership.ownedThroughout(contributors)
      if (!ownedThroughout() || lastRead.some(({ partition }) => lost.has(partition))) {
        await this.#abortBatch(new Set([...lost, ...this.#ownership.takeLost()]))
        return
      }
      guard = {
        ownedThroughout,
        fencing: fencingOffsets(contributors.keys(), lastRead, this.#committedPosition),
      }
    }
    const committing = this.#exactlyOnce
      ? lastRead
      : lastRead.filter(({ topic, partition }) => this.#ownership.owns(topic, partition))
    const committingSince = performance.now()
    let result: CommitResult
    try {
      result = await this.#committer.commit(committing, guard)
    } catch (error) {
      // Exactly-once: partitions were lost while the batch was committing, or its transaction
      // outlived the transaction timeout.
      const timedOut = error instanceof TransactionTimedOutError
      if (!this.#exactlyOnce || this.#abandoned || !(timedOut || isRebalanceError(error))) {
        throw error
      }
      await this.#abortBatch(
        new Set([...lost, ...this.#ownership.takeLost()]),
        timedOut ? 'transaction-timeout' : 'rebalance',
      )
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
    if (!result.empty) {
      this.#metric({
        name: 'commit-duration',
        type: 'duration',
        value: performance.now() - committingSince,
        labels: { worker: this.#options.workerNumber },
      })
    }
    this.#reportCounters()
    for (const message of committing) {
      const key = topicPartitionKey(message.topic, message.partition)
      if (result.offsetsCommitted) {
        this.#committedPosition.set(key, message.offset + 1n)
      } else if (!this.#lastRead.has(key)) {
        // At-least-once, a rebalance got in the way: the next batch commits them.
        this.#lastRead.set(key, message)
      }
    }
    this.#forget(lost)
    if (lost.size > 0 && !this.#stopRequested) await this.#rewind()
  }

  /**
   * Exactly-once: whether the batch can take one more record and still have processed everything
   * within the transaction budget, at the pace its records have been processed so far. Records
   * still being processed (queued behind slow async Processors) count; until one is done, the
   * pace is unknown, and the batch waits for one (or for `until`, which ends the batch).
   */
  async #admits(until: Promise<unknown>[]): Promise<boolean> {
    const budget = transactionBudget(this.#options.transactionTimeoutMs)
    for (;;) {
      const now = performance.now()
      const unsettled = this.#batchRecords - this.#batchSettled
      if (unsettled === 0) return true
      if (this.#batchSettled > 0) {
        const perRecord = (now - (this.#batchFirstAt ?? now)) / this.#batchSettled
        return now - this.#batchStartedAt + (unsettled + 1) * perRecord <= budget
      }
      if ((await Promise.race([this.#progress, ...until])) !== PROGRESS) return false
    }
  }

  /**
   * Exactly-once: adjusts `#maxBatchRecords` to a batch that took `ms` until its processing was
   * done. One that took longer than the transaction budget (slow async work queued behind each
   * other) makes the next ones smaller, aiming at half the budget; one that read as many records
   * as it could in less than half the budget lets them grow again, at most doubling.
   */
  #fitBatch(ms: number): void {
    const records = this.#batchRecords
    if (records === 0) return
    const budget = transactionBudget(this.#options.transactionTimeoutMs)
    const fit = Math.floor((records * budget) / 2 / Math.max(ms, 1))
    if (ms > budget) {
      this.#maxBatchRecords = Math.max(1, Math.min(fit, records - 1))
    } else if (records >= this.#maxBatchRecords && ms < budget / 2) {
      const grown = Math.max(this.#maxBatchRecords + 1, Math.min(fit, this.#maxBatchRecords * 2))
      this.#maxBatchRecords = Math.min(this.#options.commitMaxRecords, grown)
    }
  }

  /**
   * Whether the transaction can hold an offset of the Task of `partition`, so that its writes are
   * fenced (see `fencingOffsets`): it read a record in the batch, or has a committed offset.
   * Exactly-once, a Task that can't (nothing of it committed yet, and no record read) keeps its
   * wall-clock timers due until it can, rather than commit their writes unfenced.
   */
  #fenceable(partition: number): boolean {
    if (!this.#exactlyOnce) return true
    const ofTask = (key: TopicPartitionKey) => fromTopicPartitionKey(key).partition === partition
    if ([...this.#lastRead.keys()].some(ofTask)) return true
    return [...this.#committedPosition].some(([key, position]) => ofTask(key) && position > 0n)
  }

  /**
   * Exactly-once: aborts the batch, closes every started Task and reads every partition again from
   * its committed offset. Starting Tasks go on: they had no part in the batch, and what they hold
   * is not held twice when the stream delivers it again.
   */
  async #abortBatch(lost: ReadonlySet<number>, reason: AbortReason = 'rebalance'): Promise<void> {
    if (await this.#committer.abort()) this.#countAbort(reason)
    // Rolled back, a Task would still have the batch's Stream Time, timers and Processor state.
    this.#closeTasks()
    this.#held = undefined
    this.#position = new Map(this.#committedPosition)
    this.#forget(lost)
    if (!this.#stopRequested) await this.#rewind()
  }

  /**
   * Closes the Tasks of the `lost` partitions, cancels their starts and forgets how far they were
   * read. The caller then rewinds the stream: the client keeps fetching a lost partition that
   * comes back from where it was, unless it has a committed offset to start from, so the records
   * it fetched during the lost ownership (which the Worker dropped) would never come again.
   */
  #forget(lost: ReadonlySet<number>): void {
    if (lost.size === 0) return
    for (const partition of lost) this.#closeTask(partition)
    this.#cancelStarts(lost)
    for (const positions of [this.#position, this.#committedPosition]) {
      for (const key of positions.keys()) {
        if (lost.has(fromTopicPartitionKey(key).partition)) positions.delete(key)
      }
    }
  }

  /**
   * Reads every owned partition again from its committed offset (from its start if it has none),
   * with a new stream: the records of an aborted batch must come again, and the client only
   * rewinds on a rebalance, and then only partitions with a committed offset. A record the cycle
   * kept back from the old stream comes again too.
   */
  async #rewind(): Promise<void> {
    this.#held = undefined
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
export type ProducedOffsets = ReadonlyMap<number, ChangelogOffsets>

export interface CommitResult {
  /**
   * The offset after the last output in each partition that outputs given an explicit partition
   * (the Changelog records) went to. An offset may be lower than that, never higher: restoring from
   * a Changelog offset too early is harmless.
   */
  produced: ProducedOffsets
  /** False if the outputs committed but the offsets didn't, because of a rebalance. */
  offsetsCommitted: boolean
  /** True if the batch had nothing to commit: no outputs, no offsets. */
  empty: boolean
}

/** Exactly-once: what keeps a batch from committing after its Tasks' ownership was interrupted. */
export interface CommitGuard {
  /** Whether every Task with a part in the batch has been owned throughout since. */
  ownedThroughout(): boolean
  /**
   * Offsets to commit again with the batch, for the Tasks with a part in it but no record read
   * (only timer-driven writes): every transaction commits offsets of each of its Tasks, so that
   * the group coordinator checks the Worker's membership and generation before it commits.
   */
  fencing: ConsumedMessage[]
}

/**
 * Exactly-once: for each Task (partition number) in `tasks` with no record in `lastRead`, a
 * message standing for the last record committed from each of its partitions, from
 * `committedPosition` (the offset after it). Committing it again changes nothing, but the
 * transaction's offset commit then fences a Worker that is no longer the Task's owner. A
 * partition with nothing committed yet gets none.
 */
export function fencingOffsets(
  tasks: Iterable<number>,
  lastRead: readonly { partition: number }[],
  committedPosition: ReadonlyMap<TopicPartitionKey, bigint>,
): ConsumedMessage[] {
  const read = new Set(lastRead.map(({ partition }) => partition))
  const unread = new Set([...tasks].filter((task) => !read.has(task)))
  const fencing: ConsumedMessage[] = []
  for (const [key, position] of committedPosition) {
    const { topic, partition } = fromTopicPartitionKey(key)
    if (!unread.has(partition) || position <= 0n) continue
    fencing.push({ topic, partition, offset: position - 1n, metadata: {} } as ConsumedMessage)
  }
  return fencing
}

/** Commits a batch under one Delivery Guarantee. */
export interface BatchCommitter {
  /** Takes an output of the current batch. */
  produce(output: Output): void
  /**
   * Commits the current batch: its outputs, and the offsets after `lastRead` (the last record
   * read from each partition). Rejects if any part of it failed, except that at-least-once may
   * leave the offsets uncommitted after a rebalance.
   */
  commit(lastRead: ConsumedMessage[], guard?: CommitGuard): Promise<CommitResult>
  /** Gives up on the current batch. Resolves to whether a Kafka transaction was aborted. */
  abort(): Promise<boolean>
}

/** Produces the outputs, waits for the broker to acknowledge them, then commits the offsets. */
class AtLeastOnceCommitter implements BatchCommitter {
  readonly #consumer: Consumer<Bytes, Bytes, Bytes, Bytes>
  readonly #producer: Producer<Bytes, Bytes, Bytes, Bytes>
  readonly #maxBytes: number
  #outputs: Output[] = []

  constructor(
    consumer: Consumer<Bytes, Bytes, Bytes, Bytes>,
    producer: Producer<Bytes, Bytes, Bytes, Bytes>,
    maxBytes: number,
  ) {
    this.#consumer = consumer
    this.#producer = producer
    this.#maxBytes = maxBytes
  }

  produce(output: Output): void {
    this.#outputs.push(output)
  }

  async commit(lastRead: ConsumedMessage[], _guard?: CommitGuard): Promise<CommitResult> {
    const outputs = this.#outputs
    this.#outputs = []
    const empty = outputs.length === 0 && lastRead.length === 0
    const produced = new Map<number, Map<string, bigint>>()
    // One request after the other, so that each partition's outputs keep their order.
    for (const messages of splitBySize(outputs, this.#maxBytes)) {
      const result = await this.#producer.send({ messages })
      mergeProduced(produced, producedOffsets(messages, result.offsets ?? []))
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
        return { produced, offsetsCommitted: false, empty }
      }
    }
    return { produced, offsetsCommitted: true, empty }
  }

  async abort(): Promise<boolean> {
    this.#outputs = []
    return false
  }
}

/**
 * Sends the outputs into a Kafka transaction as they come, then adds the consumed offsets to it
 * and commits it (ADR-0002, ADR-0008). A transaction is only begun once there is something to
 * send or commit: the broker rejects ending an empty one.
 *
 * A transaction about to outlive the transaction timeout isn't committed: `commit` throws a
 * `TransactionTimedOutError`, and so does any step after the broker aborted it for its timeout.
 * The broker answers that with the same epoch errors as fencing by another producer, so an epoch
 * error counts as a timeout only once the transaction has been open for the timeout. The next
 * transaction then takes the transactional ID up again with the epoch the producer had: the
 * broker accepts that only if no other producer took the ID over since, and fences it otherwise,
 * which fails the Worker.
 */
class TransactionalCommitter implements BatchCommitter {
  readonly #consumer: Consumer<Bytes, Bytes, Bytes, Bytes>
  readonly #producer: Producer<Bytes, Bytes, Bytes, Bytes>
  readonly #maxBytes: number
  readonly #timeoutMs: number
  #transaction: Transaction | undefined
  /** When the open transaction began (`performance.now()`). */
  #openedAt: number | undefined
  /** The producer ID and epoch the open transaction began with. */
  #epoch: { producerId: bigint; producerEpoch: number } | undefined
  /**
   * After a transaction the broker aborted for its timeout: the producer ID and epoch to take the
   * transactional ID up again with.
   */
  #retake: { producerId: bigint; producerEpoch: number } | undefined
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
    options: {
      /** The most bytes of records in one produce request. */
      maxBytes: number
      /** The transaction timeout the producer was created with. */
      timeoutMs: number
      fail: (error: unknown) => void
      owns: (topic: string, partition: number) => boolean
    },
  ) {
    this.#consumer = consumer
    this.#producer = producer
    this.#maxBytes = options.maxBytes
    this.#timeoutMs = options.timeoutMs
    this.#fail = options.fail
    this.#owns = options.owns
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
      // One request after the other, so that each partition's outputs keep their order.
      for (const chunk of splitBySize(messages, this.#maxBytes)) {
        const result = await transaction.send({ messages: chunk })
        mergeProduced(this.#produced, producedOffsets(chunk, result.offsets ?? []))
      }
    } catch (error) {
      const failure = this.#timedOut(error)
      this.#sendFailure = { error: failure }
      // A timed-out batch is aborted and read again once its processing is done.
      if (!(failure instanceof TransactionTimedOutError)) this.#fail(failure)
    }
  }

  async #begin(): Promise<Transaction> {
    if (this.#transaction !== undefined) return this.#transaction
    const retake = this.#retake
    this.#retake = undefined
    // The client takes the ID up with `retake` if it dropped its epoch after the error; else it
    // still has the old epoch, and is told to.
    const transaction = await this.#producer.beginTransaction(retake ?? {})
    if (retake !== undefined && this.#producer.producerEpoch === retake.producerEpoch) {
      await this.#producer.initIdempotentProducer(retake)
    }
    this.#transaction = transaction
    this.#openedAt = performance.now()
    const { producerId, producerEpoch } = this.#producer
    this.#epoch =
      producerId === undefined || producerEpoch === undefined
        ? undefined
        : { producerId, producerEpoch }
    return transaction
  }

  /** How long the open transaction has been open, in milliseconds (0 if none is). */
  #openMs(): number {
    return this.#openedAt === undefined ? 0 : performance.now() - this.#openedAt
  }

  /**
   * `error` as a `TransactionTimedOutError` if it means the broker aborted the transaction for its
   * timeout: an epoch error once the transaction has been open (`openMs`) that long. Else `error`.
   */
  #timedOut(error: unknown, openMs = this.#openMs()): unknown {
    if (error instanceof TransactionTimedOutError) return error
    if (!isEpochError(error) || openMs < this.#timeoutMs) return error
    return new TransactionTimedOutError(
      `The broker aborted the batch's transaction, open for ${Math.round(openMs)} ms, past the transaction timeout (${this.#timeoutMs} ms)`,
      { cause: error },
    )
  }

  /**
   * After the broker aborted `transaction` for its timeout: drops it, and has the next
   * transaction take the transactional ID up again with the epoch it began with.
   */
  async #retakeAfter(transaction: Transaction): Promise<void> {
    this.#retake = this.#epoch
    await transaction.cancel().catch(() => {})
  }

  async commit(lastRead: ConsumedMessage[], guard?: CommitGuard): Promise<CommitResult> {
    // Ownership may be interrupted during any wait: check after each, and right before the commit.
    const check = () => {
      if (guard !== undefined && !guard.ownedThroughout()) {
        throw new PartitionRevokedError(
          'A Task of the batch lost its partitions during the batch; aborting its transaction',
        )
      }
    }
    await this.#sending
    if (this.#sendFailure !== undefined) throw this.#sendFailure.error
    check()
    // The broker may abort the transaction any moment now: abort it first, and read it again.
    const openMs = this.#openMs()
    if (openMs >= commitDeadline(this.#timeoutMs)) {
      throw new TransactionTimedOutError(
        `The batch's transaction has been open for ${Math.round(openMs)} ms, too close to the transaction timeout (${this.#timeoutMs} ms) to commit`,
      )
    }
    try {
      const offsets = [...lastRead, ...(guard?.fencing ?? [])]
      if (offsets.length > 0) {
        const transaction = await this.#begin()
        check()
        await transaction.addConsumer(this.#consumer)
        // One offset per partition, and one at a time: the client merges concurrent offset
        // commits.
        for (const message of offsets) {
          check()
          await transaction.addOffset(this.#withCurrentGroup(message))
        }
      }
    } catch (error) {
      // The transaction is still open: `abort` ends it.
      throw this.#timedOut(error)
    }
    check()
    const transaction = this.#transaction
    const produced = this.#produced
    const committingMs = this.#openMs()
    this.#transaction = undefined
    this.#openedAt = undefined
    this.#produced = new Map()
    try {
      await transaction?.commit()
    } catch (error) {
      const failure = this.#timedOut(error, committingMs)
      if (failure instanceof TransactionTimedOutError && transaction !== undefined) {
        await this.#retakeAfter(transaction)
      }
      throw failure
    }
    return { produced, offsetsCommitted: true, empty: transaction === undefined }
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

  async abort(): Promise<boolean> {
    await this.#sending
    this.#pending = []
    this.#produced = new Map()
    this.#sendFailure = undefined
    const transaction = this.#transaction
    const openMs = this.#openMs()
    this.#transaction = undefined
    this.#openedAt = undefined
    if (transaction === undefined) return false
    try {
      await transaction.abort()
    } catch (error) {
      // Unless the broker aborted it already, for its timeout.
      if (!(this.#timedOut(error, openMs) instanceof TransactionTimedOutError)) throw error
      await this.#retakeAfter(transaction)
    }
    return true
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

/** Roughly what a record adds to a produce request besides its key, value and headers. */
const RECORD_OVERHEAD_BYTES = 70

/** About how many bytes `output` takes in a produce request. */
function sizeOf(output: Output): number {
  let size = RECORD_OVERHEAD_BYTES + (output.key?.length ?? 0) + (output.value?.length ?? 0)
  if (output.headers instanceof Map) {
    for (const [name, value] of output.headers) size += name.length + value.length
  }
  return size
}

/**
 * Exported for the unit tests (ADR-0016). Splits `outputs`, in order, into runs of at most
 * `maxBytes` (a record larger than that alone): one produce request writes one record batch per
 * partition, which the broker rejects above its `max.message.bytes`. Bounding the whole request
 * bounds each partition's batch, whichever partition the partitioner picks.
 */
export function splitBySize(outputs: readonly Output[], maxBytes: number): Output[][] {
  const runs: Output[][] = []
  let run: Output[] = []
  let bytes = 0
  for (const output of outputs) {
    const size = sizeOf(output)
    if (run.length > 0 && bytes + size > maxBytes) {
      runs.push(run)
      run = []
      bytes = 0
    }
    run.push(output)
    bytes += size
  }
  if (run.length > 0) runs.push(run)
  return runs
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
  record: OutputRecord,
): MessageToProduce<Bytes, Bytes, Bytes, Bytes> {
  return {
    topic,
    ...(record.key === null ? {} : { key: toBuffer(record.key) }),
    // The client writes a null value as a tombstone.
    value: (record.value === null ? null : toBuffer(record.value)) as Bytes,
    timestamp: BigInt(record.timestamp),
    ...(record.headers === undefined
      ? {}
      : {
          headers: new Map(
            Object.entries(record.headers).map(([name, value]) => [
              Buffer.from(name),
              toBuffer(value),
            ]),
          ),
        }),
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

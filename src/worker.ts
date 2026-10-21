// Internal: a Worker, one consumer-group member that owns its Tasks, its producer and its
// consumer (ADR-0007). With `workers: 0` the Streams App runs one Worker in the main thread.
//
// The commit cycle (at-least-once): read records into a batch until the commit interval runs out
// or the batch is full, process each record in its Task, then produce the batch's outputs, wait
// for the broker to acknowledge them, and only then commit the consumed offsets.

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
import type { ProcessorGraph, SerializedRecord } from './processor-graph.js'
import { createTask, type Task } from './task.js'

export interface WorkerOptions {
  applicationId: string
  brokers: string[]
  /** Identifies this Worker's clients to the broker. */
  clientId: string
  commitIntervalMs: number
  commitMaxRecords: number
  sessionTimeoutMs: number
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

/** Bytes, as the Kafka client wants them. */
type Bytes = Buffer
type ConsumedMessage = Message<Bytes, Bytes, Bytes, Bytes>

/**
 * Joins the consumer group (the Application ID) and starts the commit cycle. Resolves once the
 * Worker has joined; rejects (after closing its clients) if it could not.
 */
export async function startWorker(graph: ProcessorGraph, options: WorkerOptions): Promise<Worker> {
  const { applicationId, brokers, clientId } = options
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
    // Keyed records go where Java clients would put them (murmur2), so Co-partitioned topics
    // written by other producers line up; records without a key are spread round-robin.
    partitioner: (message, key) =>
      key === undefined ? roundRobin++ : compatibilityPartitioner(message, key),
  })

  let stream: MessagesStream<Bytes, Bytes, Bytes, Bytes>
  try {
    stream = await consumer.consume({
      topics: [...graph.sources.keys()],
      // Resume from the committed offsets; a new application starts at the beginning.
      mode: 'committed',
      fallbackMode: 'earliest',
      autocommit: false,
    })
  } catch (error) {
    await closeQuietly(consumer, producer)
    throw error
  }

  const cycle = new CommitCycle(graph, options, consumer, producer, stream)
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
      await closeQuietly(consumer, producer, force)
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
const STOP = Symbol('stop')

class CommitCycle {
  readonly #graph: ProcessorGraph
  readonly #options: WorkerOptions
  readonly #consumer: Consumer<Bytes, Bytes, Bytes, Bytes>
  readonly #producer: Producer<Bytes, Bytes, Bytes, Bytes>
  readonly #messages: AsyncIterator<ConsumedMessage>
  /** One Task per partition number. */
  readonly #tasks = new Map<number, Task>()

  #stopRequested = false
  readonly #stop: Promise<typeof STOP>
  #wakeOnStop: () => void = () => {}

  /** The first error a Task raised. Stops the Worker (fail-fast). */
  #failure: { error: unknown } | undefined
  // The current batch.
  #outputs: MessageToProduce<Bytes, Bytes, Bytes, Bytes>[] = []
  #work: Promise<void>[] = []
  /** The last record read from each partition, by `topic:partition`. */
  #lastRead = new Map<string, ConsumedMessage>()

  constructor(
    graph: ProcessorGraph,
    options: WorkerOptions,
    consumer: Consumer<Bytes, Bytes, Bytes, Bytes>,
    producer: Producer<Bytes, Bytes, Bytes, Bytes>,
    stream: MessagesStream<Bytes, Bytes, Bytes, Bytes>,
  ) {
    this.#graph = graph
    this.#options = options
    this.#consumer = consumer
    this.#producer = producer
    this.#messages = stream[Symbol.asyncIterator]()
    this.#stop = new Promise((resolve) => {
      this.#wakeOnStop = () => resolve(STOP)
    })
  }

  requestStop(): void {
    this.#stopRequested = true
    this.#wakeOnStop()
  }

  async run(): Promise<void> {
    let next = this.#messages.next()
    // The read still pending when the cycle ends settles once the stream closes.
    try {
      while (!this.#stopRequested) {
        next = (await this.#readBatch(next)).next
        await this.#commit()
      }
    } finally {
      next.catch(() => {})
    }
  }

  /**
   * Reads and processes records until the commit interval runs out, the batch is full, a Task
   * fails or a stop is requested. Returns the read that is still pending.
   */
  async #readBatch(
    pending: Promise<IteratorResult<ConsumedMessage>>,
  ): Promise<{ next: Promise<IteratorResult<ConsumedMessage>> }> {
    const { commitIntervalMs, commitMaxRecords } = this.#options
    let timer: NodeJS.Timeout | undefined
    const timeUp = new Promise<typeof TIME_UP>((resolve) => {
      timer = setTimeout(() => resolve(TIME_UP), commitIntervalMs)
    })
    let next = pending
    try {
      for (let count = 0; count < commitMaxRecords; count++) {
        const result = await Promise.race([next, timeUp, this.#stop])
        if (result === TIME_UP || result === STOP) break
        if (result.done) throw new Error('The consumer stopped delivering records')
        next = this.#messages.next()
        this.#process(result.value)
        if (this.#failure !== undefined) break
      }
    } finally {
      clearTimeout(timer)
    }
    return { next }
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
    this.#work.push(
      work.catch((error: unknown) => {
        this.#failure ??= { error }
      }),
    )
  }

  #task(partition: number): Task {
    let task = this.#tasks.get(partition)
    if (task === undefined) {
      task = createTask(
        this.#graph,
        (topic, record) => {
          this.#outputs.push({
            topic,
            ...(record.key === null ? {} : { key: toBuffer(record.key) }),
            // The client writes a null value as a tombstone.
            value: (record.value === null ? null : toBuffer(record.value)) as Bytes,
            timestamp: BigInt(record.timestamp),
          })
        },
        Date.now,
        { task: partition, stateDir: this.#options.stateDir },
      )
      this.#tasks.set(partition, task)
    }
    return task
  }

  /** Finishes the batch: waits for its processing, produces its outputs, commits its offsets. */
  async #commit(): Promise<void> {
    if (this.#failure === undefined) {
      for (const task of this.#tasks.values()) this.#track(task.fireWallClockTimers())
    }
    await Promise.all(this.#work)
    this.#work = []
    if (this.#failure !== undefined) throw this.#failure.error

    const outputs = this.#outputs
    this.#outputs = []
    if (outputs.length > 0) await this.#producer.send({ messages: outputs })

    const lastRead = [...this.#lastRead.values()]
    this.#lastRead = new Map()
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

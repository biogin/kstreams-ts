// Error handlers (ADR-0010): deserialization and processing errors go to pluggable handlers, and
// the app stops (fail-fast) without one. Produce errors never reach a handler: they happen when the
// broker rejects a write, outside any record's processing. Neither does `AsyncNotAllowedError`
// (ADR-0012), nor an error raised by a timer, which has no record to skip.

import { checkTimestamp } from './record.js'
import { skipsErrorHandlers } from './sync-dsl.js'

/** Which kind of error an error handler is given. */
export type ErrorHandlerKind = 'deserialization' | 'processing'

/**
 * What an error handler decides: `'continue'` skips the failed record and goes on with the next
 * one; `'fail'` stops the Streams App with the error, as without a handler.
 */
export type ErrorHandlerResponse = 'continue' | 'fail'

/** A record whose deserialization or processing failed, as it was read from its input topic. */
export interface FailedRecord {
  /** The input topic the record was read from. */
  readonly topic: string
  /** Its partition, which is also the Task's id. */
  readonly partition: number
  /** Its offset in the partition. The Test Driver counts offsets per input topic from 0. */
  readonly offset: bigint
  /** The key's bytes; null for a record without a key. */
  readonly key: Uint8Array | null
  /** The value's bytes; null for a Tombstone. */
  readonly value: Uint8Array | null
  /** Its Event Time. */
  readonly timestamp: number
}

/** A record an error handler writes, e.g. to a dead-letter topic. */
export interface HandlerOutputRecord {
  key: Uint8Array | null
  value: Uint8Array | null
  timestamp: number
  /** Kafka record headers, by name. */
  headers?: Readonly<Record<string, Uint8Array>>
}

/** What an error handler is given. */
export interface ErrorHandlerContext {
  readonly kind: ErrorHandlerKind
  /**
   * The error: a `DeserializationError` for a deserialization error, or whatever a DSL function,
   * a Processor or a Serde of an output topic threw while processing the record.
   */
  readonly error: unknown
  readonly record: FailedRecord
  /**
   * Writes a record to `topic` with the Task's outputs: under exactly-once, in the same Kafka
   * transaction as the batch, so it is committed exactly when the failed record's offset is.
   */
  produce(topic: string, record: HandlerOutputRecord): void
}

/**
 * Decides what happens to a record whose deserialization or processing failed. It must be
 * synchronous; if it throws, the Streams App stops with that error.
 *
 * Outputs and State Store writes the record made before its processing failed are kept: a handler
 * that continues skips only the rest of that record's processing.
 */
export type ErrorHandler = (context: ErrorHandlerContext) => ErrorHandlerResponse

/** The error handlers of a Streams App. Without one, that kind of error stops the app. */
export interface ErrorHandlers {
  /** For records whose key or value the input topic's Serdes can't read. */
  deserialization?: ErrorHandler
  /** For errors raised while a record is processed by the app's DSL functions and Processors. */
  processing?: ErrorHandler
}

const KINDS: readonly string[] = ['deserialization', 'processing'] satisfies ErrorHandlerKind[]
const TOPIC_NAME = /^[A-Za-z0-9._-]+$/
/** The topic of each handler `deadLetter` made. */
const deadLetterTopics = new WeakMap<ErrorHandler, string>()

/**
 * An error handler that writes each failed record to `topic`, unchanged (key, value and Event
 * Time as they were read), and goes on with the next record. Under exactly-once the dead-letter
 * record is written in the same transaction as the batch. Its headers tell what failed:
 *
 * - `kstreams.error.kind`: `deserialization` or `processing`
 * - `kstreams.error.name`, `kstreams.error.message`, `kstreams.error.stack`: the error's
 * - `kstreams.error.topic`, `kstreams.error.partition`, `kstreams.error.offset`: where the record
 *   was read
 *
 * The topic is not created for you.
 *
 * @example
 * createStreamsApp({
 *   applicationId: 'orders',
 *   errorHandlers: { deserialization: deadLetter('orders-dlq') },
 * })
 */
export function deadLetter(topic: string): ErrorHandler {
  if (typeof topic !== 'string' || !TOPIC_NAME.test(topic)) {
    throw new Error(
      `Invalid dead-letter topic ${JSON.stringify(topic)}: use one or more ASCII letters, digits, ".", "_" or "-"`,
    )
  }
  const handler: ErrorHandler = ({ kind, error, record, produce }) => {
    const { key, value, timestamp } = record
    produce(topic, { key, value, timestamp, headers: errorHeaders(kind, error, record) })
    return 'continue'
  }
  deadLetterTopics.set(handler, topic)
  return handler
}

/** Internal: the topics the `deadLetter` handlers among `handlers` write to. */
export function deadLetterTopicsOf(handlers: ErrorHandlers | undefined): string[] {
  return Object.values(handlers ?? {}).flatMap((handler: ErrorHandler) => {
    const topic = deadLetterTopics.get(handler)
    return topic === undefined ? [] : [topic]
  })
}

const encoder = new TextEncoder()

function errorHeaders(
  kind: ErrorHandlerKind,
  error: unknown,
  { topic, partition, offset }: FailedRecord,
): Record<string, Uint8Array> {
  const headers: Record<string, string> = {
    'kstreams.error.kind': kind,
    'kstreams.error.name': error instanceof Error ? error.name : typeof error,
    'kstreams.error.message': error instanceof Error ? error.message : String(error),
    ...(error instanceof Error && error.stack !== undefined
      ? { 'kstreams.error.stack': error.stack }
      : {}),
    'kstreams.error.topic': topic,
    'kstreams.error.partition': String(partition),
    'kstreams.error.offset': String(offset),
  }
  return Object.fromEntries(
    Object.entries(headers).map(([name, value]) => [name, encoder.encode(value)]),
  )
}

/** Internal: checks the `errorHandlers` config when the app is defined. */
export function checkErrorHandlers(handlers: ErrorHandlers | undefined): void {
  if (handlers === undefined) return
  if (typeof handlers !== 'object' || handlers === null) {
    throw new Error(
      'errorHandlers must be an object such as { deserialization: deadLetter(topic) }',
    )
  }
  for (const [kind, handler] of Object.entries(handlers)) {
    if (!KINDS.includes(kind)) {
      throw new Error(
        `Unknown error handler "${kind}": use ${KINDS.map((k) => `"${k}"`).join(' or ')}`,
      )
    }
    if (handler !== undefined && typeof handler !== 'function') {
      throw new Error(`errorHandlers.${kind} must be a function such as deadLetter(topic)`)
    }
  }
}

/**
 * Internal: gives `error` to the handler of its `kind`. Returns if the handler skipped the record;
 * throws the error the Streams App stops with otherwise (the error itself without a handler).
 */
export function handleError(
  handlers: ErrorHandlers | undefined,
  kind: ErrorHandlerKind,
  error: unknown,
  record: FailedRecord,
  produce: (topic: string, record: HandlerOutputRecord) => void,
): void {
  const handler = handlers?.[kind]
  if (handler === undefined || skipsErrorHandlers(error)) throw error
  const response: unknown = handler({
    kind,
    error,
    record,
    produce(topic, output) {
      checkTimestamp(output.timestamp)
      produce(topic, output)
    },
  })
  if (typeof (response as PromiseLike<unknown> | undefined)?.then === 'function') {
    // The app stops on the error below; don't also report the Promise's rejection.
    Promise.resolve(response).catch(() => {})
    throw new TypeError('Error handlers must be synchronous, but one returned a Promise', {
      cause: error,
    })
  }
  if (response === 'continue') return
  if (response === 'fail') throw error
  throw new TypeError(
    `An error handler must return 'continue' or 'fail', got ${JSON.stringify(response)}`,
    { cause: error },
  )
}

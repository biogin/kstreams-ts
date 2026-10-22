// Internal: runs a Worker in a worker thread (`workers > 0`). A worker thread can't be handed the
// Streams App, whose definition holds functions, so it imports the App Module and rebuilds the app
// from its default export (ADR-0013), then runs the Worker as `workers: 0` runs it in the main
// thread: its own consumer-group member with its own consumer, producer and State Stores
// (ADR-0007). No records cross threads; the main thread only starts and stops the Worker and hears
// about its failure, over the messages below.

import { Worker as Thread } from 'node:worker_threads'
import { DeserializationError } from './serde.js'
import { AsyncNotAllowedError } from './sync-dsl.js'
import type { Worker } from './worker.js'

/** What the thread needs to rebuild the app and run its Worker. */
export interface WorkerThreadData {
  appModule: string
  applicationId: string
  /** The App Process ID (see state-directory.ts). */
  processId: string
  workerNumber: number
}

/** Messages from the main thread to a worker thread. */
export type ToWorkerThread = { type: 'stop' }

/** Messages from a worker thread to the main thread. */
export type FromWorkerThread =
  | { type: 'running' }
  | { type: 'stopped' }
  /**
   * The running Worker failed, with this root cause; it is closing its clients and reports
   * `failed` (or `stopped`, if it gave up closing gracefully) once done. Sent first, so that errors
   * its closing causes, in this thread or others, can't be taken for the cause.
   */
  | { type: 'failing'; error: unknown }
  /**
   * The Worker couldn't start, or stopped because of an error; it has closed its clients. The
   * error is sent as {@link transferableError} made it.
   */
  | { type: 'failed'; error: unknown }

export interface WorkerThreadOptions {
  appModule: URL
  applicationId: string
  /** The App Process ID (see state-directory.ts). */
  processId: string
  workerNumber: number
  /** How long the Worker may take to stop gracefully; the thread gets a margin on top. */
  shutdownTimeoutMs: number
  /** Called once if the Worker stops because of an error after it started. */
  onError(error: unknown): void
}

/**
 * How much longer than the shutdown timeout a stopping Worker may take (leaving the group and
 * closing its clients) before its thread is terminated.
 */
const STOP_MARGIN_MS = 10_000

// Next to this file: the built `.js`, or the `.ts` source when the sources run directly (tests).
const entry = new URL(
  `./worker-thread-entry${import.meta.url.endsWith('.ts') ? '.ts' : '.js'}`,
  import.meta.url,
)

/**
 * Starts a worker thread that runs Worker `workerNumber`. Resolves once the Worker has joined the
 * consumer group; rejects, after the thread has ended, if it could not.
 */
export function startWorkerThread(options: WorkerThreadOptions): Promise<Worker> {
  const { appModule, applicationId, processId, workerNumber } = options
  const workerData: WorkerThreadData = {
    appModule: appModule.href,
    applicationId,
    processId,
    workerNumber,
  }
  const thread = new Thread(entry, { workerData, name: `${applicationId}-worker-${workerNumber}` })
  const exited = new Promise<number>((resolve) => thread.once('exit', resolve))

  let running = false
  let stopped = false
  /** The first error: what the Worker reports. */
  let failure: { error: unknown } | undefined
  /** An uncaught exception in the thread, reported once the thread has exited. */
  let uncaught: { error: unknown } | undefined
  let onStart: { resolve(worker: Worker): void; reject(error: unknown): void } | undefined

  /** Remembers the first error the Worker reports; later ones are consequences of it. */
  function report(error: unknown): void {
    if (failure !== undefined || stopped) return
    failure = { error }
    if (running) options.onError(error)
    else void exited.then(() => onStart?.reject(error))
  }

  /** Reports `error`, unless an earlier one was, and ends the thread. */
  function end(error: unknown): void {
    report(error)
    void thread.terminate()
  }

  thread.on('message', (message: FromWorkerThread) => {
    if (message.type === 'running') {
      running = true
      onStart?.resolve(worker)
    } else if (message.type === 'stopped') {
      stopped = true
      void thread.terminate()
    } else if (message.type === 'failing') {
      report(errorFromThread(message.error))
    } else {
      end(errorFromThread(message.error))
    }
  })
  // An uncaught exception in the thread, e.g. thrown by the App Module. The thread exits right
  // after; the messages it sent before are delivered by then (see below), so a root cause it
  // reported comes first even though the exception arrives on another channel.
  thread.on('error', (error) => {
    uncaught ??= { error }
  })
  void exited.then((code) => {
    end(
      uncaught?.error ??
        new Error(`Worker ${workerNumber} exited unexpectedly (exit code ${code})`),
    )
  })

  let stopping: Promise<void> | undefined
  const worker: Worker = {
    stop() {
      stopping ??= stopThread()
      return stopping
    },
  }

  /**
   * Asks a running Worker to stop; a failing one is stopping already. Either reports `stopped` or
   * `failed`, which ends the thread; one that takes too long is terminated.
   */
  async function stopThread(): Promise<void> {
    if (failure === undefined && !stopped) {
      thread.postMessage({ type: 'stop' } satisfies ToWorkerThread)
    }
    let timeout: NodeJS.Timeout | undefined
    const timedOut = new Promise<boolean>((resolve) => {
      timeout = setTimeout(() => resolve(true), options.shutdownTimeoutMs + STOP_MARGIN_MS)
    })
    const late = await Promise.race([exited.then(() => false), timedOut])
    clearTimeout(timeout)
    if (late)
      end(new Error(`Worker ${workerNumber} did not stop in time; its thread was terminated`))
    await exited
    if (failure !== undefined) throw failure.error
  }

  return new Promise<Worker>((resolve, reject) => {
    onStart = { resolve, reject }
  })
}

// Errors cross the thread boundary by structured clone, which keeps only the message, the stack
// and the cause of an Error, and turns every Error subclass into a plain Error. So the thread sends
// an error as below, and the main thread rebuilds it: as an instance of its class for the library's
// errors and JavaScript's built-in ones, with every own property that can be copied (e.g. the
// `topic` of a DeserializationError, or the `code` of a Kafka client error).

/** The classes errors keep across the thread boundary, by `name`. */
const ERROR_CLASSES: Readonly<Record<string, { prototype: Error }>> = {
  AsyncNotAllowedError,
  DeserializationError,
  AggregateError,
  EvalError,
  RangeError,
  ReferenceError,
  SyntaxError,
  TypeError,
  URIError,
}

/** An Error as a worker thread sends it. */
interface TransferredError {
  kstreamsTransferredError: true
  name: string
  stack: string | undefined
  /** Own data properties: name, value, and whether it is enumerable. */
  properties: [string, unknown, boolean][]
}

/** Internal, for the thread side: `error` as it can be posted to the main thread. */
export function transferableError(error: unknown, depth = 0): unknown {
  if (!(error instanceof Error)) return cloneable(error) ? error : String(error)
  // A cause chain that loops back is cut off.
  if (depth > 10) return error.message
  const properties: [string, unknown, boolean][] = []
  for (const name of Object.getOwnPropertyNames(error)) {
    const descriptor = Object.getOwnPropertyDescriptor(error, name)
    if (name === 'stack' || descriptor === undefined || !('value' in descriptor)) continue
    const value: unknown = descriptor.value
    if (value instanceof Error || name === 'cause') {
      properties.push([name, transferableError(value, depth + 1), descriptor.enumerable === true])
    } else if (cloneable(value)) {
      properties.push([name, value, descriptor.enumerable === true])
    }
  }
  return {
    kstreamsTransferredError: true,
    name: error.name,
    stack: error.stack,
    properties,
  } satisfies TransferredError
}

/** Internal, for the main thread: the error a worker thread sent, rebuilt. */
export function errorFromThread(sent: unknown): unknown {
  if (!isTransferred(sent)) return sent
  const known = Object.hasOwn(ERROR_CLASSES, sent.name) ? ERROR_CLASSES[sent.name] : undefined
  const error = Object.create((known ?? Error).prototype) as Error
  const define = (name: string, value: unknown, enumerable: boolean) =>
    Object.defineProperty(error, name, { value, enumerable, writable: true, configurable: true })
  if (sent.stack !== undefined) define('stack', sent.stack, false)
  if (error.name !== sent.name) define('name', sent.name, false)
  for (const [name, value, enumerable] of sent.properties) {
    define(name, errorFromThread(value), enumerable)
  }
  return error
}

function isTransferred(value: unknown): value is TransferredError {
  return (
    typeof value === 'object' &&
    value !== null &&
    (value as Partial<TransferredError>).kstreamsTransferredError === true
  )
}

function cloneable(value: unknown): boolean {
  try {
    structuredClone(value)
    return true
  } catch {
    return false
  }
}

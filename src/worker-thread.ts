// Internal: runs a Worker in a worker thread (`workers > 0`). A worker thread can't be handed the
// Streams App, whose definition holds functions, so it imports the App Module and rebuilds the app
// from its default export (ADR-0013), then runs the Worker as `workers: 0` runs it in the main
// thread: its own consumer-group member with its own consumer, producer and State Stores
// (ADR-0007). No records cross threads; the main thread only starts and stops the Worker and hears
// about its failure, over the messages below.

import { Worker as Thread } from 'node:worker_threads'
import type { Worker } from './worker.js'

/** What the thread needs to rebuild the app and run its Worker. */
export interface WorkerThreadData {
  appModule: string
  applicationId: string
  workerNumber: number
}

/** Messages from the main thread to a worker thread. */
export type ToWorkerThread = { type: 'stop' }

/** Messages from a worker thread to the main thread. */
export type FromWorkerThread =
  | { type: 'running' }
  | { type: 'stopped' }
  /** The Worker couldn't start, or stopped because of an error; it has closed its clients. */
  | { type: 'failed'; error: unknown }

export interface WorkerThreadOptions {
  appModule: URL
  applicationId: string
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
  const { appModule, applicationId, workerNumber } = options
  const workerData: WorkerThreadData = { appModule: appModule.href, applicationId, workerNumber }
  const thread = new Thread(entry, { workerData, name: `${applicationId}-worker-${workerNumber}` })
  const exited = new Promise<number>((resolve) => thread.once('exit', resolve))

  let running = false
  let stopped = false
  let failure: { error: unknown } | undefined
  let onStart: { resolve(worker: Worker): void; reject(error: unknown): void } | undefined
  let onStopped: (() => void) | undefined

  function fail(error: unknown): void {
    if (failure !== undefined || stopped) return
    failure = { error }
    void thread.terminate()
    if (running) options.onError(error)
    else void exited.then(() => onStart?.reject(error))
    onStopped?.()
  }

  thread.on('message', (message: FromWorkerThread) => {
    if (message.type === 'running') {
      running = true
      onStart?.resolve(worker)
    } else if (message.type === 'stopped') {
      stopped = true
      void thread.terminate()
      onStopped?.()
    } else {
      fail(message.error)
    }
  })
  // An uncaught exception in the thread, e.g. thrown by the App Module.
  thread.on('error', fail)
  void exited.then((code) => {
    fail(new Error(`Worker ${workerNumber} exited unexpectedly (exit code ${code})`))
  })

  let stopping: Promise<void> | undefined
  const worker: Worker = {
    stop() {
      stopping ??= stopThread()
      return stopping
    },
  }

  async function stopThread(): Promise<void> {
    if (failure === undefined && !stopped) {
      const done = new Promise<void>((resolve) => {
        onStopped = resolve
      })
      thread.postMessage({ type: 'stop' } satisfies ToWorkerThread)
      let timeout: NodeJS.Timeout | undefined
      const timedOut = new Promise<void>((resolve) => {
        timeout = setTimeout(resolve, options.shutdownTimeoutMs + STOP_MARGIN_MS)
      })
      await Promise.race([done, timedOut])
      clearTimeout(timeout)
      if (failure === undefined && !stopped) {
        fail(new Error(`Worker ${workerNumber} did not stop in time; its thread was terminated`))
      }
    }
    await exited
    if (failure !== undefined) throw failure.error
  }

  return new Promise<Worker>((resolve, reject) => {
    onStart = { resolve, reject }
  })
}

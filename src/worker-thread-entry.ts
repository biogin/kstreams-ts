// Internal: the entry point of a worker thread (see worker-thread.ts). Imports the App Module,
// starts its Streams App's Worker and reports to the main thread; stops the Worker when asked.

import { parentPort, workerData } from 'node:worker_threads'
import type { WorkerReport } from './observability.js'
import { startWorkerOf } from './streams-app.js'
import type { Worker } from './worker.js'
import {
  type FromWorkerThread,
  type ToWorkerThread,
  transferableError,
  type WorkerThreadData,
} from './worker-thread.js'

const port = parentPort
if (port === null) throw new Error('worker-thread-entry runs in a worker thread only')
const { appModule, applicationId, processId, workerNumber } = workerData as WorkerThreadData

function send(message: FromWorkerThread): void {
  flush()
  // The error keeps its class and properties across the boundary (see worker-thread.ts).
  port?.postMessage(
    'error' in message ? { ...message, error: transferableError(message.error) } : message,
  )
}

/** What the Worker reported and the main thread hasn't been sent yet. */
let reports: WorkerReport[] = []

/** Sends the Worker's reports together, once the current work is done; metrics come in bursts. */
function report(next: WorkerReport): void {
  reports.push(next)
  if (reports.length === 1) setImmediate(flush)
}

/** Sends the reports not sent yet, so that they keep their order with other messages. */
function flush(): void {
  if (reports.length === 0) return
  port?.postMessage({ type: 'reports', reports } satisfies FromWorkerThread)
  reports = []
}

let stopping: Promise<void> | undefined
/** Stops the Worker once, and reports how it ended. */
function stop(worker: Worker): Promise<void> {
  stopping ??= worker.stop().then(
    () => send({ type: 'stopped' }),
    (error: unknown) => send({ type: 'failed', error }),
  )
  return stopping
}

try {
  const module = (await import(appModule)) as { default?: unknown }
  const starting = startWorkerOf(module.default, {
    appModule,
    applicationId,
    processId,
    workerNumber,
    // The root cause goes first, before the Worker closes its clients (see `failing`); `failed`
    // follows once it has closed them.
    onError: (error) => {
      send({ type: 'failing', error })
      void starting.then(stop)
    },
    report,
  })
  const worker = await starting
  port.on('message', (message: ToWorkerThread) => {
    if (message.type === 'stop') void stop(worker)
  })
  send({ type: 'running' })
} catch (error) {
  send({ type: 'failed', error })
}

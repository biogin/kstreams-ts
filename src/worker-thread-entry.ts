// Internal: the entry point of a worker thread (see worker-thread.ts). Imports the App Module,
// starts its Streams App's Worker and reports to the main thread; stops the Worker when asked.

import { parentPort, workerData } from 'node:worker_threads'
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
  // The error keeps its class and properties across the boundary (see worker-thread.ts).
  port?.postMessage(
    'error' in message ? { ...message, error: transferableError(message.error) } : message,
  )
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
  })
  const worker = await starting
  port.on('message', (message: ToWorkerThread) => {
    if (message.type === 'stop') void stop(worker)
  })
  send({ type: 'running' })
} catch (error) {
  send({ type: 'failed', error })
}

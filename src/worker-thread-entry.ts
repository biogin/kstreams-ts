// Internal: the entry point of a worker thread (see worker-thread.ts). Imports the App Module,
// starts its Streams App's Worker and reports to the main thread; stops the Worker when asked.

import { parentPort, workerData } from 'node:worker_threads'
import { startWorkerOf } from './streams-app.js'
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
    message.type === 'failed' ? { ...message, error: transferableError(message.error) } : message,
  )
}

try {
  const module = (await import(appModule)) as { default?: unknown }
  const worker = await startWorkerOf(module.default, {
    appModule,
    applicationId,
    processId,
    workerNumber,
    onError: (error) => send({ type: 'failed', error }),
  })
  port.on('message', async (message: ToWorkerThread) => {
    if (message.type !== 'stop') return
    try {
      await worker.stop()
      send({ type: 'stopped' })
    } catch (error) {
      send({ type: 'failed', error })
    }
  })
  send({ type: 'running' })
} catch (error) {
  send({ type: 'failed', error })
}

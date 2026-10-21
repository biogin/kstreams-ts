// Internal: the entry point of a worker thread (see worker-thread.ts). Imports the App Module,
// starts its Streams App's Worker and reports to the main thread; stops the Worker when asked.

import { parentPort, workerData } from 'node:worker_threads'
import { startWorkerOf } from './streams-app.js'
import type { FromWorkerThread, ToWorkerThread, WorkerThreadData } from './worker-thread.js'

const port = parentPort
if (port === null) throw new Error('worker-thread-entry runs in a worker thread only')
const { appModule, applicationId, workerNumber } = workerData as WorkerThreadData

function send(message: FromWorkerThread): void {
  try {
    port?.postMessage(message)
  } catch {
    // The error can't be copied to the main thread (e.g. it holds functions): send what can be.
    if (message.type === 'failed') port?.postMessage({ ...message, error: copyable(message.error) })
  }
}

function copyable(error: unknown): Error {
  const copy = new Error(error instanceof Error ? error.message : String(error))
  if (error instanceof Error) {
    copy.name = error.name
    if (error.stack !== undefined) copy.stack = error.stack
  }
  return copy
}

try {
  const module = (await import(appModule)) as { default?: unknown }
  const worker = await startWorkerOf(module.default, {
    appModule,
    applicationId,
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

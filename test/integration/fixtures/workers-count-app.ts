// App Module for worker threads (ADR-0013): its default export is the Streams App itself, and the
// app names this module in `workers: { count, module: import.meta.url }`, so each worker thread
// imports it and rebuilds the same app. It counts `input` records per key with `count()` (SQLite
// in `stateDir`) under exactly-once and writes each new count to `output`. The first Worker to see the
// value `failOn` (if given) fails; any other processes it normally. With `noAppInWorkers`, worker threads find no app in it (to test that error).
//
// The params come from the KSTREAMS_APP_PARAMS environment variable the App Process harness sets
// (see harness/app-process.ts): worker threads see the same environment, so they build the same
// app.

import { writeFileSync } from 'node:fs'
import { join } from 'node:path'
import { isMainThread } from 'node:worker_threads'
import { createStreamsApp, serdes } from '../../../src/index.js'

export interface WorkersCountParams {
  applicationId: string
  brokers: string[]
  input: string
  output: string
  workers: number
  stateDir: string
  failOn?: string
  /**
   * The failing Worker's thread also crashes right after, with an uncaught "Connection closed" (as
   * a Kafka client's unhandled 'error' event while closing would), before its Worker has closed.
   */
  crashAfterFail?: boolean
  noAppInWorkers?: boolean
}

const {
  applicationId,
  brokers,
  input,
  output,
  workers,
  stateDir,
  failOn,
  crashAfterFail,
  noAppInWorkers,
} = JSON.parse(process.env.KSTREAMS_APP_PARAMS ?? '{}') as WorkersCountParams

const app = createStreamsApp({
  applicationId,
  brokers,
  guarantee: 'exactly-once',
  workers: { count: workers, module: import.meta.url },
  stateDir,
  sessionTimeoutMs: 6_000,
  changelogReplicationFactor: 1,
  commitMaxRecords: 50,
})
app
  .stream(input, { key: serdes.string, value: serdes.string })
  .peek((_key, value) => {
    if (value === failOn && firstToFail()) {
      if (crashAfterFail) {
        setImmediate(() => {
          throw new Error('Connection closed')
        })
      }
      throw new Error('Processor failed on purpose')
    }
  })
  .groupByKey()
  .count({ store: 'counts' })
  .toStream()
  .map((key, count) => ({ key, value: String(count) }))
  .to(output, { key: serdes.string, value: serdes.string })

export default noAppInWorkers && !isMainThread ? undefined : app

/** True for the first caller across all threads: it creates a marker file in the state directory. */
function firstToFail(): boolean {
  try {
    writeFileSync(join(stateDir, 'failed'), '', { flag: 'wx' })
    return true
  } catch {
    return false
  }
}

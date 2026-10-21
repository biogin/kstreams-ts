// App Module for crash tests: counts `input` records per key with `count()` and writes each new
// count to `output` as a string (at-least-once, workers: 0). Its State Store is in memory, so a
// restarted app has nothing but Restore to bring the counts back.

import { createStreamsApp, type StreamsApp, serdes, stores } from '../../../src/index.js'

export interface CountParams {
  applicationId: string
  brokers: string[]
  input: string
  output: string
  /** Bounds how many records one batch (and so one crash) can count twice. */
  commitMaxRecords: number
}

export default function countApp({
  applicationId,
  brokers,
  input,
  output,
  commitMaxRecords,
}: CountParams): StreamsApp {
  const app = createStreamsApp({
    applicationId,
    brokers,
    // Short session: a SIGKILLed Worker's partitions move to the restarted one only after it expires.
    sessionTimeoutMs: 6_000,
    changelogReplicationFactor: 1,
    commitMaxRecords,
  })
  app
    .stream(input, { key: serdes.string, value: serdes.string })
    .groupByKey()
    .count({ store: 'counts', storeSupplier: stores.inMemory() })
    .toStream()
    .map((key, count) => ({ key, value: String(count) }))
    .to(output, { key: serdes.string, value: serdes.string })
  return app
}

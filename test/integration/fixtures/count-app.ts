// App Module for crash tests: counts `input` records per key with `count()` and writes each new
// count to `output` as a string (workers: 0). By default it runs at-least-once with its State
// Store in memory, so a restarted app has nothing but Restore to bring the counts back. With a
// state directory the store is SQLite there, kept across restarts with its Checkpoint.

import {
  createStreamsApp,
  type DeliveryGuarantee,
  type StreamsApp,
  serdes,
  stores,
} from '../../../src/index.js'

export interface CountParams {
  applicationId: string
  brokers: string[]
  input: string
  output: string
  /** Bounds how many records one batch (and so one crash) can count twice. */
  commitMaxRecords: number
  /** Defaults to at-least-once. */
  guarantee?: DeliveryGuarantee
  /** Keeps the counts in SQLite there; without one they are kept in memory. */
  stateDir?: string
}

export default function countApp({
  applicationId,
  brokers,
  input,
  output,
  commitMaxRecords,
  guarantee = 'at-least-once',
  stateDir,
}: CountParams): StreamsApp {
  const app = createStreamsApp({
    applicationId,
    brokers,
    guarantee,
    // Short session: a SIGKILLed Worker's partitions move to the restarted one only after it expires.
    sessionTimeoutMs: 6_000,
    changelogReplicationFactor: 1,
    commitMaxRecords,
    ...(stateDir === undefined ? {} : { stateDir }),
  })
  app
    .stream(input, { key: serdes.string, value: serdes.string })
    .groupByKey()
    .count({
      store: 'counts',
      storeSupplier: stateDir === undefined ? stores.inMemory() : stores.sqlite(),
    })
    .toStream()
    .map((key, count) => ({ key, value: String(count) }))
    .to(output, { key: serdes.string, value: serdes.string })
  return app
}

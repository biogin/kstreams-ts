// App Module of the crash test suite (ADR-0011): a stateful exactly-once app with two SQLite State
// Stores fed by the same grouped Stream. `count()` counts the `input` records of each key into
// `counts`; `aggregate()` folds their values into an order-sensitive checksum in `chains`. Each new
// count and checksum goes to its output topic as a string. Commits are frequent and batches short,
// so that a kill at a random time often lands near a commit.

import { createStreamsApp, type StreamsApp, serdes, stores } from '../../src/index.js'

export interface CrashParams {
  applicationId: string
  brokers: string[]
  input: string
  countsOutput: string
  chainsOutput: string
  stateDir: string
  commitIntervalMs: number
  commitMaxRecords: number
}

/** Keeps checksums well below 2^53, so that they stay exact as doubles. */
const MODULUS = 1_000_000_007

/** The next checksum of a key after `value`: depends on every value and their order. */
export function nextChain(chain: number, value: string): number {
  return (chain * 31 + Number(value)) % MODULUS
}

export default function crashApp({
  applicationId,
  brokers,
  input,
  countsOutput,
  chainsOutput,
  stateDir,
  commitIntervalMs,
  commitMaxRecords,
}: CrashParams): StreamsApp {
  const app = createStreamsApp({
    applicationId,
    brokers,
    guarantee: 'exactly-once',
    stateDir,
    // Short session: a SIGKILLed Worker's partitions move to the restarted one only after it expires.
    sessionTimeoutMs: 6_000,
    changelogReplicationFactor: 1,
    commitIntervalMs,
    commitMaxRecords,
  })
  const grouped = app.stream(input, { key: serdes.string, value: serdes.string }).groupByKey()
  grouped
    .count({ store: 'counts', storeSupplier: stores.sqlite() })
    .toStream()
    .map((key, count) => ({ key, value: String(count) }))
    .to(countsOutput, { key: serdes.string, value: serdes.string })
  grouped
    .aggregate(
      () => 0,
      (_key, value, chain: number) => nextChain(chain, value),
      {
        store: 'chains',
        serde: serdes.number,
        storeSupplier: stores.sqlite(),
      },
    )
    .toStream()
    .map((key, chain) => ({ key, value: String(chain) }))
    .to(chainsOutput, { key: serdes.string, value: serdes.string })
  return app
}

// The one module through which integration and crash tests reach the engine's internal modules
// (ADR-0016). They otherwise drive the public API (`src/index.ts`) and real Kafka; what they need
// from the inside (an abort-aware reader, the on-disk layout of State Stores and Checkpoints, the
// store-key encoding) is re-exported here, together with the helpers built on it, so the
// dependency on internals stays in one visible place.

import { type Serde, type Serdes, serdes, stores } from '../../src/index.js'
import { createChangelogReader } from '../../src/restore.js'
import { keyValueStore, taskDirectory } from '../../src/state-store.js'

export { CHECKPOINT_FILE, readCheckpoint } from '../../src/checkpoint.js'
export { ReadCommittedConsumer } from '../../src/read-committed.js'
export { type ChangelogRecord, createChangelogReader } from '../../src/restore.js'
export { keyValueStore, taskDirectory } from '../../src/state-store.js'
export { keyOf } from '../../src/store-key.js'
export { windowedSerde, windowKeyEncoding } from '../../src/windows.js'

/** A committed record of a topic, with its key and value as strings. */
export interface CommittedRecord {
  key: string
  value: string | null
  partition: number
}

/**
 * Every committed record of partitions 0..`partitions - 1` of `topic`, aborted transactions
 * skipped, read with Restore's reader: it first waits for the transactions open when it starts to
 * end, so it reads everything committed up to then.
 */
export async function readCommitted(
  brokers: string[],
  clientId: string,
  topic: string,
  partitions: number,
): Promise<CommittedRecord[]> {
  const reader = createChangelogReader({ brokers, clientId })
  const records: CommittedRecord[] = []
  try {
    for (let partition = 0; partition < partitions; partition++) {
      await reader.read(topic, partition, (chunk) => {
        for (const { key, value } of chunk) {
          records.push({
            key: Buffer.from(key ?? new Uint8Array()).toString(),
            value: value === null ? null : Buffer.from(value).toString(),
            partition,
          })
        }
      })
    }
  } finally {
    await reader.close()
  }
  return records
}

/**
 * The entries of the SQLite State Store `name` of Task `task`, as the app left it on disk, with
 * string keys. Opens the store and closes it again.
 */
export function storedEntries<V>(
  stateDir: string,
  applicationId: string,
  task: number,
  name: string,
  value: Serde<V>,
): [string, V][] {
  const directory = taskDirectory(stateDir, applicationId, task)
  const store = stores.sqlite()({ name, applicationId, task, directory })
  const storeSerdes: Serdes<string, V> = { key: serdes.string, value }
  try {
    return keyValueStore(store, storeSerdes, { topic: name }).all()
  } finally {
    store.close()
  }
}

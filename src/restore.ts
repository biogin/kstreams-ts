// Internal: Restore, rebuilding a Task's State Stores from their Changelogs before the Task
// processes records (ADR-0009). A store with a usable Checkpoint replays only the committed
// Changelog records after it; any other store is wiped and every committed record replayed (a full
// Restore). See checkpoint.ts for when a Checkpoint is usable.
//
// Changelogs are read by a restore consumer outside the consumer group, with the low-level
// `fetch()` under `read_committed`, from the start of the partition up to its last stable offset.
// Aborted transactions are filtered out per fetch response by read-committed.ts, not by the client
// (platformatic/kafka#467).
//
// A Task that moved may still have a transaction of its previous owner open on its Changelog
// partition (one the previous owner is about to abort, or one that committed but whose markers
// aren't written yet). Its records would then sit after the last stable offset and be missed, so a
// read first waits until every transaction open on the partition when it started has ended: until
// the last stable offset reaches the end the partition had then.

import { FetchIsolationLevels, IS_CONTROL, ListOffsetTimestamps } from '@platformatic/kafka'
import type { StoreNode } from './processor-graph.js'
import { ReadCommittedConsumer } from './read-committed.js'
import { type StateStore, singleKeyEncoding } from './state-store.js'

/** A record of a Changelog: a store key as the key Serde's bytes; a null value is a Tombstone. */
export interface ChangelogRecord {
  key: Uint8Array | null
  value: Uint8Array | null
}

/** Reads the committed records of Changelog partitions. */
export interface ChangelogReader {
  /**
   * Reads `partition` of `topic` from offset `from` (by default its start) to its end (its last
   * stable offset, once the transactions open at the call have ended), skipping aborted ones. Calls `apply` with the records,
   * in offset order, a chunk at a time. Resolves to the offset after the last one read: where a
   * later read would continue. Resolves to undefined, reading nothing, when `from` is outside the
   * partition (before its start or past its end). Once `signal` is aborted, stops reading and
   * rejects with its reason.
   */
  read(
    topic: string,
    partition: number,
    apply: (records: ChangelogRecord[]) => void,
    from?: bigint,
    signal?: AbortSignal,
  ): Promise<bigint | undefined>
}

/** A State Store of a Task, as opened, with its definition. */
export interface RestoringStore {
  node: StoreNode
  store: StateStore
  /**
   * The store's Checkpoint: the Changelog offset it has applied up to. Undefined when it has
   * none, or it is unclean: the store is then wiped and fully restored.
   */
  checkpoint?: bigint | undefined
}

/**
 * Restores each store from the Task's partition of its Changelog. A store with a Checkpoint
 * replays the Changelog after it; others (and one whose Checkpoint is outside the Changelog) are
 * wiped, then replay all of it. Keys are decoded with the key Serde and re-encoded as store keys
 * (ADR-0015); Tombstones delete. Each chunk is applied in one store transaction. Resolves to the
 * Changelog offset each store has applied up to, by store name.
 *
 * Once `signal` is aborted (the Task was revoked, or the Worker stops), no further chunk is applied
 * and it rejects with the signal's reason. The stores are then partly restored: a Task whose
 * Restore was cancelled must be closed without a new Checkpoint.
 */
export async function restoreStores(
  stores: readonly RestoringStore[],
  partition: number,
  reader: ChangelogReader,
  signal?: AbortSignal,
): Promise<Map<string, bigint>> {
  const restored = new Map<string, bigint>()
  for (const { node, store, checkpoint } of stores) {
    signal?.throwIfAborted()
    const topic = node.changelogTopic
    const context = { topic }
    const encoding = node.keyEncoding ?? singleKeyEncoding(node.serdes.key)
    const apply = (records: ChangelogRecord[]) => {
      signal?.throwIfAborted()
      inTransaction(store, () => {
        for (const { key, value } of records) {
          if (key === null) {
            throw new Error(
              `Changelog "${topic}" partition ${partition} has a record without a key`,
            )
          }
          const storeKey = encoding.encode(node.serdes.key.deserialize(key, context), context)
          if (value === null) store.delete(storeKey)
          else store.put(storeKey, value)
        }
      })
    }
    let offset =
      checkpoint === undefined
        ? undefined
        : await reader.read(topic, partition, apply, checkpoint, signal)
    if (offset === undefined) {
      signal?.throwIfAborted()
      inTransaction(store, () => {
        // Collected first: a store must not be written while it is iterated.
        for (const key of Array.from(store.all(), ([key]) => key)) store.delete(key)
      })
      offset = await reader.read(topic, partition, apply, undefined, signal)
      if (offset === undefined)
        throw new Error(`Cannot read Changelog "${topic}" partition ${partition}`)
    }
    restored.set(node.name, offset)
  }
  return restored
}

function inTransaction(store: StateStore, work: () => void): void {
  store.begin()
  try {
    work()
  } catch (error) {
    store.rollback()
    throw error
  }
  store.commit()
}

export interface KafkaChangelogReaderOptions {
  brokers: string[]
  clientId: string
  /** At most this many bytes per fetch. Small values make tests span several fetches. */
  partitionMaxBytes?: number
}

/** A {@link ChangelogReader} on Kafka. Close it when done. */
export interface KafkaChangelogReader extends ChangelogReader {
  close(): Promise<void>
}

const OFFSET_OUT_OF_RANGE = 1
/** Fetch errors that new metadata (another leader, a newer leader epoch) fixes. */
const RETRIABLE_FETCH_ERRORS = new Set([
  3, // UNKNOWN_TOPIC_OR_PARTITION
  6, // NOT_LEADER_OR_FOLLOWER
  74, // FENCED_LEADER_EPOCH
  75, // UNKNOWN_LEADER_EPOCH
])
const MAX_FETCH_RETRIES = 50
/** How often a read checks whether the transactions open on its partition have ended. */
const TRANSACTION_POLL_MS = 100
const FETCH_RETRY_DELAY_MS = 100

/** Creates a restore consumer: a Kafka client that reads Changelogs without joining a group. */
export function createChangelogReader({
  brokers,
  clientId,
  partitionMaxBytes = 1 << 20,
}: KafkaChangelogReaderOptions): KafkaChangelogReader {
  const consumer = new ReadCommittedConsumer({
    clientId,
    // Required by the client, but `fetch()` never joins the group.
    groupId: `${clientId}-restore`,
    bootstrapBrokers: brokers,
    isolationLevel: FetchIsolationLevels.READ_COMMITTED,
  })

  async function offset(
    topic: string,
    partition: number,
    timestamp: bigint,
    isolationLevel: number = FetchIsolationLevels.READ_COMMITTED,
  ): Promise<bigint> {
    const offsets = await consumer.listOffsets({
      topics: [topic],
      partitions: { [topic]: [partition] },
      timestamp,
      isolationLevel,
    })
    const found = offsets.get(topic)?.[partition]
    if (found === undefined) throw new Error(`No offset for Changelog "${topic}" ${partition}`)
    return found
  }

  async function leader(
    topic: string,
    partition: number,
    forceUpdate: boolean,
  ): Promise<{ topicId: string; node: number }> {
    const metadata = await consumer.metadata({ topics: [topic], forceUpdate })
    const found = metadata.topics.get(topic)
    const node = found?.partitions[partition]?.leader
    if (found === undefined || node === undefined || node < 0) {
      throw new Error(`Changelog "${topic}" partition ${partition} has no leader`)
    }
    return { topicId: found.id, node }
  }

  return {
    async read(topic, partition, apply, from, signal) {
      // With read_committed, the latest offset is the last stable offset: every aborted
      // transaction before it has its marker. It must reach the end as of now (read_uncommitted):
      // see the top of this file.
      const written = await offset(
        topic,
        partition,
        ListOffsetTimestamps.LATEST,
        FetchIsolationLevels.READ_UNCOMMITTED,
      )
      let end = await offset(topic, partition, ListOffsetTimestamps.LATEST)
      while (end < written) {
        await sleep(TRANSACTION_POLL_MS, signal)
        end = await offset(topic, partition, ListOffsetTimestamps.LATEST)
      }
      signal?.throwIfAborted()
      const start = await offset(topic, partition, ListOffsetTimestamps.EARLIEST)
      if (from !== undefined && (from < start || from > end)) return undefined
      let position = from ?? start
      let target = await leader(topic, partition, false)
      let retries = 0
      while (position < end) {
        signal?.throwIfAborted()
        const response = await consumer.fetch({
          node: target.node,
          isolationLevel: FetchIsolationLevels.READ_COMMITTED,
          maxWaitTime: 500,
          topics: [
            {
              topicId: target.topicId,
              partitions: [
                {
                  partition,
                  fetchOffset: position,
                  currentLeaderEpoch: -1,
                  lastFetchedEpoch: -1,
                  partitionMaxBytes,
                },
              ],
            },
          ],
        })
        const fetched = response.responses[0]?.partitions[0]
        if (fetched === undefined) throw new Error(`No fetch response for Changelog "${topic}"`)
        if (fetched.errorCode !== 0) {
          if (++retries > MAX_FETCH_RETRIES) {
            throw new Error(
              `Restoring from Changelog "${topic}" partition ${partition} failed with error code ${fetched.errorCode}`,
            )
          }
          if (fetched.errorCode === OFFSET_OUT_OF_RANGE) {
            position = await offset(topic, partition, ListOffsetTimestamps.EARLIEST)
          } else if (RETRIABLE_FETCH_ERRORS.has(fetched.errorCode)) {
            await sleep(FETCH_RETRY_DELAY_MS, signal)
            target = await leader(topic, partition, true)
          } else {
            throw new Error(
              `Restoring from Changelog "${topic}" partition ${partition} failed with error code ${fetched.errorCode}`,
            )
          }
          continue
        }
        retries = 0
        // Aborted transactions' batches come without records (see read-committed.ts).
        const records: ChangelogRecord[] = []
        for (const batch of fetched.records ?? []) {
          const last = batch.firstOffset + BigInt(batch.lastOffsetDelta)
          if (last < position) continue
          if (!(batch.attributes & IS_CONTROL)) {
            for (const record of batch.records) {
              if (batch.firstOffset + BigInt(record.offsetDelta) < position) continue
              records.push({ key: record.key ?? null, value: record.value ?? null })
            }
          }
          position = last + 1n
        }
        if (records.length > 0) apply(records)
      }
      return position
    },
    close: () => consumer.close(true),
  }
}

/** Waits `ms`; rejects with `signal`'s reason as soon as it is aborted. */
function sleep(ms: number, signal: AbortSignal | undefined): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) {
      reject(signal.reason)
      return
    }
    const onAbort = () => {
      clearTimeout(timer)
      reject(signal?.reason)
    }
    const timer = setTimeout(() => {
      signal?.removeEventListener('abort', onAbort)
      resolve()
    }, ms)
    signal?.addEventListener('abort', onAbort, { once: true })
  })
}

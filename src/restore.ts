// Internal: Restore, rebuilding a Task's State Stores from their Changelogs before the Task
// processes records (ADR-0009). A store with a usable Checkpoint replays only the committed
// Changelog records after it; any other store is wiped and every committed record replayed (a full
// Restore). See checkpoint.ts for when a Checkpoint is usable.
//
// Changelogs are read by a restore consumer outside the consumer group, with the low-level
// `fetch()` under `read_committed`, from the start of the partition up to its last stable offset
// (as of the start of the read).

import {
  Consumer,
  FetchIsolationLevels,
  IS_CONTROL,
  IS_TRANSACTIONAL,
  ListOffsetTimestamps,
  type RecordsBatch,
} from '@platformatic/kafka'
import type { StoreNode } from './processor-graph.js'
import { type StateStore, singleKeyEncoding } from './state-store.js'

/** A record of a Changelog: a store key as the key Serde's bytes; a null value is a Tombstone. */
export interface ChangelogRecord {
  key: Uint8Array | null
  value: Uint8Array | null
}

/** Reads the committed records of Changelog partitions. */
export interface ChangelogReader {
  /**
   * Reads `partition` of `topic` from offset `from` (by default its start) to its end as of the
   * call (its last stable offset), skipping aborted transactions. Calls `apply` with the records,
   * in offset order, a chunk at a time. Resolves to the offset after the last one read: where a
   * later read would continue. Resolves to undefined, reading nothing, when `from` is outside the
   * partition (before its start or past its end).
   */
  read(
    topic: string,
    partition: number,
    apply: (records: ChangelogRecord[]) => void,
    from?: bigint,
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
 */
export async function restoreStores(
  stores: readonly RestoringStore[],
  partition: number,
  reader: ChangelogReader,
): Promise<Map<string, bigint>> {
  const restored = new Map<string, bigint>()
  for (const { node, store, checkpoint } of stores) {
    const topic = node.changelogTopic
    const context = { topic }
    const encoding = node.keyEncoding ?? singleKeyEncoding(node.serdes.key)
    const apply = (records: ChangelogRecord[]) =>
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
    let offset =
      checkpoint === undefined ? undefined : await reader.read(topic, partition, apply, checkpoint)
    if (offset === undefined) {
      inTransaction(store, () => {
        // Collected first: a store must not be written while it is iterated.
        for (const key of Array.from(store.all(), ([key]) => key)) store.delete(key)
      })
      offset = await reader.read(topic, partition, apply)
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

/** A partition of a fetch response; the client doesn't export the type. */
type FetchedPartition = Awaited<
  ReturnType<Consumer['fetch']>
>['responses'][number]['partitions'][number]

const OFFSET_OUT_OF_RANGE = 1
/** Fetch errors that new metadata (another leader, a newer leader epoch) fixes. */
const RETRIABLE_FETCH_ERRORS = new Set([
  3, // UNKNOWN_TOPIC_OR_PARTITION
  6, // NOT_LEADER_OR_FOLLOWER
  74, // FENCED_LEADER_EPOCH
  75, // UNKNOWN_LEADER_EPOCH
])
const MAX_FETCH_RETRIES = 50
const FETCH_RETRY_DELAY_MS = 100

/** The control record type of an abort marker (the second int16 of its key). */
const ABORT_MARKER = 0

/** Creates a restore consumer: a Kafka client that reads Changelogs without joining a group. */
export function createChangelogReader({
  brokers,
  clientId,
  partitionMaxBytes = 1 << 20,
}: KafkaChangelogReaderOptions): KafkaChangelogReader {
  const consumer = new Consumer({
    clientId,
    // Required by the client, but `fetch()` never joins the group.
    groupId: `${clientId}-restore`,
    bootstrapBrokers: brokers,
    isolationLevel: FetchIsolationLevels.READ_COMMITTED,
  })

  async function offset(topic: string, partition: number, timestamp: bigint): Promise<bigint> {
    const offsets = await consumer.listOffsets({
      topics: [topic],
      partitions: { [topic]: [partition] },
      timestamp,
      isolationLevel: FetchIsolationLevels.READ_COMMITTED,
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
    async read(topic, partition, apply, from) {
      // With read_committed, the latest offset is the last stable offset: no open transaction
      // is waited for, and every aborted one before it has its marker.
      const end = await offset(topic, partition, ListOffsetTimestamps.LATEST)
      const start = await offset(topic, partition, ListOffsetTimestamps.EARLIEST)
      if (from !== undefined && (from < start || from > end)) return undefined
      let position = from ?? start
      const aborted = new AbortedTransactions()
      let target = await leader(topic, partition, false)
      let retries = 0
      while (position < end) {
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
            await new Promise((resolve) => setTimeout(resolve, FETCH_RETRY_DELAY_MS))
            target = await leader(topic, partition, true)
          } else {
            throw new Error(
              `Restoring from Changelog "${topic}" partition ${partition} failed with error code ${fetched.errorCode}`,
            )
          }
          continue
        }
        retries = 0
        const records: ChangelogRecord[] = []
        position = aborted.committedRecords(fetched, position, records)
        if (records.length > 0) apply(records)
      }
      return position
    },
    close: () => consumer.close(true),
  }
}

/**
 * Tracks the aborted transactions of one partition across fetch responses, so their records are
 * skipped even when the abort marker comes in a later response. The client drops an aborted
 * transaction's records only if its marker is in the same response, and `fetch()` responses end
 * wherever the byte limit falls.
 */
class AbortedTransactions {
  /** Producers whose aborted transaction has started but whose abort marker is not read yet. */
  readonly #open = new Set<bigint>()

  /**
   * Adds the committed data records of `fetched` from `position` on to `records`, and returns
   * the position after the last record batch read.
   */
  committedRecords(
    fetched: FetchedPartition,
    position: bigint,
    records: ChangelogRecord[],
  ): bigint {
    // Aborted transactions overlapping this response, by first offset. One that started before
    // `position` is open: its marker comes at or after `position`.
    const upcoming = (fetched.abortedTransactions ?? [])
      .filter(({ producerId, firstOffset }) => {
        if (firstOffset >= position) return true
        this.#open.add(producerId)
        return false
      })
      .sort((a, b) => (a.firstOffset < b.firstOffset ? -1 : a.firstOffset > b.firstOffset ? 1 : 0))
    let next = position
    for (const batch of fetched.records ?? []) {
      const last = batch.firstOffset + BigInt(batch.lastOffsetDelta)
      if (last < next) continue
      while (upcoming.length > 0 && (upcoming[0]?.firstOffset ?? last) <= last) {
        this.#open.add(upcoming.shift()?.producerId ?? -1n)
      }
      if (batch.attributes & IS_CONTROL) {
        if (isAbortMarker(batch)) this.#open.delete(batch.producerId)
      } else if (!(batch.attributes & IS_TRANSACTIONAL && this.#open.has(batch.producerId))) {
        for (const record of batch.records) {
          const offset = batch.firstOffset + BigInt(record.offsetDelta)
          if (offset < next) continue
          records.push({ key: record.key ?? null, value: record.value ?? null })
        }
      }
      next = last + 1n
    }
    return next
  }
}

function isAbortMarker(batch: RecordsBatch): boolean {
  const key = batch.records[0]?.key
  return key !== undefined && key !== null && key.length >= 4 && key.readInt16BE(2) === ABORT_MARKER
}

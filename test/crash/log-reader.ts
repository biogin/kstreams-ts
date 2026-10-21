// An abort-aware reader for the crash suite's oracle that relies neither on the engine (Restore's
// reader) nor on the client's read_committed filtering (platformatic/kafka#467). It fetches a
// partition read_uncommitted, so the client filters nothing, and resolves transactions itself from
// the log: a transactional batch is held per producer until that producer's next control batch,
// then kept on a commit marker and dropped on an abort marker. That is exact for a partition whose
// transactions have all ended (here: after the app stopped gracefully).

import {
  Consumer,
  FetchIsolationLevels,
  IS_CONTROL,
  IS_TRANSACTIONAL,
  ListOffsetTimestamps,
  type RecordsBatch,
} from '@platformatic/kafka'

/** A committed record, with its offset. */
export interface LogRecord {
  offset: bigint
  key: Buffer | null
  value: Buffer | null
}

/** A record batch as laid out in the log, for failure messages. */
export interface LogBatch {
  firstOffset: bigint
  lastOffset: bigint
  producerId: bigint
  producerEpoch: number
  kind: 'data' | 'transactional' | 'commit' | 'abort'
  records: number
}

export interface PartitionLog {
  /** The committed data records, in offset order. */
  records: LogRecord[]
  /** Every batch, data and control, in offset order. */
  batches: LogBatch[]
  /** Producers with transactional batches and no control batch after them. */
  unresolved: bigint[]
}

/** The control type of a control batch: the second int16 of its record's key (0 abort, 1 commit). */
function controlType(batch: RecordsBatch): 'commit' | 'abort' | undefined {
  const key = batch.records[0]?.key
  if (key === undefined || key === null || key.length < 4) return undefined
  const type = key.readInt16BE(2)
  return type === 0 ? 'abort' : type === 1 ? 'commit' : undefined
}

export async function readPartitionLog(
  brokers: string[],
  clientId: string,
  topic: string,
  partition: number,
): Promise<PartitionLog> {
  const consumer = new Consumer({
    clientId,
    groupId: `${clientId}-group`,
    bootstrapBrokers: brokers,
  })
  try {
    const offsets = async (timestamp: bigint) => {
      const found = await consumer.listOffsets({
        topics: [topic],
        partitions: { [topic]: [partition] },
        timestamp,
        isolationLevel: FetchIsolationLevels.READ_UNCOMMITTED,
      })
      const offset = found.get(topic)?.[partition]
      if (offset === undefined) throw new Error(`No offset for ${topic} ${partition}`)
      return offset
    }
    const end = await offsets(ListOffsetTimestamps.LATEST)
    let position = await offsets(ListOffsetTimestamps.EARLIEST)
    const metadata = await consumer.metadata({ topics: [topic], forceUpdate: true })
    const found = metadata.topics.get(topic)
    const node = found?.partitions[partition]?.leader
    if (found === undefined || node === undefined || node < 0) {
      throw new Error(`${topic} ${partition} has no leader`)
    }

    const log: PartitionLog = { records: [], batches: [], unresolved: [] }
    const pending = new Map<bigint, LogRecord[]>()
    while (position < end) {
      const response = await consumer.fetch({
        node,
        isolationLevel: FetchIsolationLevels.READ_UNCOMMITTED,
        maxWaitTime: 500,
        topics: [
          {
            topicId: found.id,
            partitions: [
              {
                partition,
                fetchOffset: position,
                currentLeaderEpoch: -1,
                lastFetchedEpoch: -1,
                partitionMaxBytes: 1 << 20,
              },
            ],
          },
        ],
      })
      const fetched = response.responses[0]?.partitions[0]
      if (fetched === undefined || fetched.errorCode !== 0) {
        throw new Error(`Fetching ${topic} ${partition} failed (${fetched?.errorCode})`)
      }
      for (const batch of fetched.records ?? []) {
        const lastOffset = batch.firstOffset + BigInt(batch.lastOffsetDelta)
        if (lastOffset < position) continue
        const { producerId, producerEpoch } = batch
        const described = { firstOffset: batch.firstOffset, lastOffset, producerId, producerEpoch }
        if (batch.attributes & IS_CONTROL) {
          const kind = controlType(batch)
          if (kind === undefined) throw new Error(`Unknown control batch at ${batch.firstOffset}`)
          log.batches.push({ ...described, kind, records: 0 })
          if (kind === 'commit') log.records.push(...(pending.get(producerId) ?? []))
          pending.delete(producerId)
        } else {
          const records = batch.records.map((record) => ({
            offset: batch.firstOffset + BigInt(record.offsetDelta),
            key: record.key ?? null,
            value: record.value ?? null,
          }))
          const transactional = (batch.attributes & IS_TRANSACTIONAL) !== 0
          log.batches.push({
            ...described,
            kind: transactional ? 'transactional' : 'data',
            records: records.length,
          })
          if (transactional)
            pending.set(producerId, [...(pending.get(producerId) ?? []), ...records])
          else log.records.push(...records)
        }
        position = lastOffset + 1n
      }
    }
    log.records.sort((a, b) => (a.offset < b.offset ? -1 : a.offset > b.offset ? 1 : 0))
    log.unresolved = [...pending.keys()]
    return log
  } finally {
    await consumer.close(true)
  }
}

/** The batches of `log` from a few before `offset` to a few after, one per line. */
export function layoutAround(log: PartitionLog, offset: bigint, context = 6): string {
  const at = log.batches.findIndex((batch) => batch.lastOffset >= offset)
  const from = Math.max(0, (at === -1 ? log.batches.length : at) - context)
  return log.batches
    .slice(from, from + 2 * context + 1)
    .map(
      (batch) =>
        `    ${batch.firstOffset}-${batch.lastOffset} ${batch.kind} producer ${batch.producerId}` +
        ` epoch ${batch.producerEpoch} (${batch.records} records)`,
    )
    .join('\n')
}

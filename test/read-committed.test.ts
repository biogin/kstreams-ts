import { IS_CONTROL, IS_TRANSACTIONAL, type RecordsBatch } from '@platformatic/kafka'
import { describe, expect, it } from 'vitest'
import { type AbortedTransaction, filterAbortedBatches } from '../src/read-committed.js'

const P = 5049n
const Q = 7001n

/** A data batch of `count` records from `firstOffset`, transactional unless said otherwise. */
function data(
  producerId: bigint,
  firstOffset: number,
  count: number,
  { transactional = true, epoch = 0 } = {},
): RecordsBatch {
  return {
    firstOffset: BigInt(firstOffset),
    length: 0,
    partitionLeaderEpoch: 0,
    magic: 2,
    crc: 0,
    attributes: transactional ? IS_TRANSACTIONAL : 0,
    lastOffsetDelta: count - 1,
    firstTimestamp: 0n,
    maxTimestamp: 0n,
    producerId,
    producerEpoch: epoch,
    firstSequence: 0,
    records: Array.from({ length: count }, (_, i) => ({
      length: 0,
      attributes: 0,
      timestampDelta: 0n,
      offsetDelta: i,
      key: Buffer.from(`k${firstOffset + i}`),
      value: Buffer.from(`v${firstOffset + i}`),
      headers: [],
    })),
  }
}

/** A control batch (transaction marker) at `offset`. */
function marker(producerId: bigint, offset: number, type: 'abort' | 'commit'): RecordsBatch {
  const key = Buffer.alloc(4)
  key.writeInt16BE(0, 0)
  key.writeInt16BE(type === 'abort' ? 0 : 1, 2)
  return {
    ...data(producerId, offset, 1),
    attributes: IS_TRANSACTIONAL | IS_CONTROL,
    records: [
      {
        length: 0,
        attributes: 0,
        timestampDelta: 0n,
        offsetDelta: 0,
        key,
        value: Buffer.alloc(0),
        headers: [],
      },
    ],
  }
}

/** The offsets of the data records kept, in order. */
function kept(batches: RecordsBatch[]): number[] {
  return batches
    .filter((batch) => !(batch.attributes & IS_CONTROL))
    .flatMap((batch) => batch.records.map((r) => Number(batch.firstOffset) + r.offsetDelta))
}

function range(from: number, to: number): number[] {
  return Array.from({ length: to - from + 1 }, (_, i) => from + i)
}

function aborted(producerId: bigint, firstOffset: number): AbortedTransaction {
  return { producerId, firstOffset: BigInt(firstOffset) }
}

describe('filterAbortedBatches', () => {
  it('keeps everything when no transaction is aborted', () => {
    const batches = [data(P, 0, 3), marker(P, 3, 'commit'), data(Q, 4, 2, { transactional: false })]
    expect(kept(filterAbortedBatches(batches, null))).toEqual([0, 1, 2, 4, 5])
    expect(kept(filterAbortedBatches(batches, []))).toEqual([0, 1, 2, 4, 5])
  })

  it('removes an aborted transaction whose marker is in a later response', () => {
    // First response: the transaction's records only, its marker not yet.
    const first = filterAbortedBatches([data(P, 0, 1), data(P, 1, 1)], [aborted(P, 0)])
    expect(kept(first)).toEqual([])
    // Still there, without records, so a reader advances past them.
    expect(first.map((batch) => batch.firstOffset)).toEqual([0n, 1n])
    // Second response, from offset 2: the broker lists the transaction again.
    const second = filterAbortedBatches(
      [data(P, 2, 1), marker(P, 3, 'abort'), data(P, 4, 1), marker(P, 5, 'commit')],
      [aborted(P, 0)],
    )
    expect(kept(second)).toEqual([4])
  })

  it('keeps committed transactions of a producer ID between its aborted ones (the crash pattern)', () => {
    // A Worker is killed mid-transaction (epoch 3); fencing aborts it (marker, epoch 4); the next
    // incarnation reuses the producer ID (epoch 5) and commits. Later in the same response an
    // abort marker of the same producer ID comes without an aborted-transaction entry (a
    // transaction that wrote nothing to this partition before it was aborted).
    const batches = [
      data(P, 618, 19, { epoch: 3 }),
      marker(P, 637, 'abort'),
      data(P, 638, 15, { epoch: 5 }),
      marker(P, 653, 'commit'),
      data(P, 654, 10, { epoch: 5 }),
      marker(P, 664, 'commit'),
      marker(P, 665, 'abort'),
      data(P, 666, 3, { epoch: 7 }),
      marker(P, 669, 'commit'),
    ]
    const result = filterAbortedBatches(batches, [aborted(P, 618)])
    expect(kept(result)).toEqual([...range(638, 652), ...range(654, 663), ...range(666, 668)])
    // Every control batch is kept.
    expect(result.filter((batch) => batch.attributes & IS_CONTROL)).toHaveLength(5)
  })

  it('handles two aborted transactions of the same producer ID in one response', () => {
    const batches = [
      data(P, 10, 3),
      marker(P, 13, 'abort'),
      data(P, 14, 2),
      marker(P, 16, 'commit'),
      data(P, 17, 2),
      marker(P, 19, 'abort'),
      data(P, 20, 2),
      marker(P, 22, 'commit'),
    ]
    // In any order: the filter sorts them.
    const result = filterAbortedBatches(batches, [aborted(P, 17), aborted(P, 10)])
    expect(kept(result)).toEqual([14, 15, 20, 21])
  })

  it('keeps non-transactional batches of a producer ID with an aborted transaction', () => {
    const batches = [
      data(P, 0, 2),
      data(P, 2, 2, { transactional: false }),
      data(Q, 4, 1),
      marker(P, 5, 'abort'),
      marker(Q, 6, 'commit'),
    ]
    expect(kept(filterAbortedBatches(batches, [aborted(P, 0)]))).toEqual([2, 3, 4])
  })

  it('removes the rest of an aborted transaction that started before the fetch offset', () => {
    // Fetched from offset 105; the transaction started at 100.
    const batches = [data(P, 105, 3), data(Q, 108, 1), marker(P, 109, 'abort'), data(P, 110, 1)]
    const result = filterAbortedBatches(batches, [aborted(P, 100)])
    expect(kept(result)).toEqual([108, 110])
  })

  it('opens an aborted transaction at the batch that reaches its first offset', () => {
    // Q's aborted transaction starts after P's committed batch: P is unaffected, interleaved
    // batches are attributed to the right producer.
    const batches = [
      data(Q, 0, 2),
      marker(Q, 2, 'commit'),
      data(P, 3, 2),
      data(Q, 5, 2),
      data(P, 7, 1),
      marker(P, 8, 'commit'),
      marker(Q, 9, 'abort'),
    ]
    expect(kept(filterAbortedBatches(batches, [aborted(Q, 5)]))).toEqual([0, 1, 3, 4, 7])
  })

  it('keeps control batches, in order, and does not mutate its input', () => {
    const batches = [data(P, 0, 2), marker(P, 2, 'abort'), data(P, 3, 1), marker(P, 4, 'commit')]
    const result = filterAbortedBatches(batches, [aborted(P, 0)])
    expect(result.map((batch) => batch.firstOffset)).toEqual([0n, 2n, 3n, 4n])
    expect(result[1]).toBe(batches[1])
    expect(result[3]).toBe(batches[3])
    expect(batches[0]?.records).toHaveLength(2)
  })
})

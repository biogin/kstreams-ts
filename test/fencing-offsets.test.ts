import { describe, expect, it } from 'vitest'
import { topicPartitionKey } from '../src/topic-partition.js'
import { fencingOffsets } from '../src/worker.js'

describe('fencingOffsets (exactly-once, Tasks with only timer-driven writes)', () => {
  const committed = new Map([
    [topicPartitionKey('orders', 0), 10n],
    [topicPartitionKey('customers', 0), 4n],
    [topicPartitionKey('orders', 1), 7n],
    [topicPartitionKey('orders', 2), 0n],
  ])

  it('commits the committed offsets again for every partition of a Task with no record read', () => {
    const fencing = fencingOffsets([0, 1], [{ partition: 1 }], committed)
    expect(fencing.map(({ topic, partition, offset }) => ({ topic, partition, offset }))).toEqual([
      // The offset of the last record committed: the commit stores the offset after it.
      { topic: 'orders', partition: 0, offset: 9n },
      { topic: 'customers', partition: 0, offset: 3n },
    ])
  })

  it('adds nothing for Tasks that read records, or with nothing committed yet', () => {
    expect(fencingOffsets([1], [{ partition: 1 }], committed)).toEqual([])
    expect(fencingOffsets([2, 3], [], committed)).toEqual([])
  })
})

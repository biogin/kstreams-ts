// Internal: the stable committed offsets of a Task's input partitions, for a Worker that takes the
// Task over under exactly-once.
//
// The client starts each newly assigned partition from its committed offset as `OffsetFetch` reads
// it without `requireStable`. An offset that the previous owner's transaction commits is only
// visible once that transaction has completed, so a new owner can start from an older offset and
// process records again (duplicates). With `requireStable`, the broker answers
// UNSTABLE_OFFSET_COMMIT while a transaction with offsets of the partition is open; we ask again
// until it isn't. The Worker then skips the records before the stable offset.

import { Admin, findErrorBy } from '@platformatic/kafka'

export interface StableOffsetReader {
  /**
   * The stable committed offset of partition `partition` of each of `topics` that has one. Waits
   * while a transaction with offsets of them is open; resolves to undefined once `giveUp()` returns
   * true.
   */
  read(
    topics: readonly string[],
    partition: number,
    giveUp: () => boolean,
  ): Promise<Map<string, bigint> | undefined>
  close(): Promise<void>
}

const UNSTABLE_RETRY_MS = 100

export function createStableOffsetReader(options: {
  brokers: string[]
  clientId: string
  groupId: string
}): StableOffsetReader {
  const admin = new Admin({
    clientId: options.clientId,
    bootstrapBrokers: options.brokers,
    retries: 20,
    retryDelay: UNSTABLE_RETRY_MS,
  })
  return {
    async read(topics, partition, giveUp) {
      for (;;) {
        if (giveUp()) return undefined
        try {
          const groups = await admin.listConsumerGroupOffsets({
            groups: [
              {
                groupId: options.groupId,
                topics: topics.map((name) => ({ name, partitionIndexes: [partition] })),
              },
            ],
            requireStable: true,
          })
          const offsets = new Map<string, bigint>()
          for (const { name, partitions } of groups[0]?.topics ?? []) {
            for (const { partitionIndex, committedOffset } of partitions) {
              if (partitionIndex === partition && committedOffset >= 0n) {
                offsets.set(name, committedOffset)
              }
            }
          }
          return offsets
        } catch (error) {
          if (findErrorBy(error as Error, 'apiId', 'UNSTABLE_OFFSET_COMMIT') === null) throw error
          await new Promise((resolve) => setTimeout(resolve, UNSTABLE_RETRY_MS))
        }
      }
    },
    close: () => admin.close(),
  }
}

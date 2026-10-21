// Internal: creates the Internal Topics an application needs (its Changelogs) before a Worker
// joins the consumer group, and checks the ones that already exist.

import { Admin } from '@platformatic/kafka'
import type { ProcessorGraph } from './processor-graph.js'

export interface InternalTopicOptions {
  brokers: string[]
  clientId: string
  /** The replication factor of the Changelogs created. */
  replicationFactor: number
}

/**
 * Makes sure every State Store's Changelog exists: compacted, with as many partitions as the input
 * topics, so that Task `n` writes to Changelog partition `n`. Missing ones are created; an existing
 * one with another partition count is an error, because its partitions wouldn't line up with the
 * Tasks. Resolves once every Changelog partition has a leader.
 */
export async function ensureChangelogs(
  graph: ProcessorGraph,
  { brokers, clientId, replicationFactor }: InternalTopicOptions,
): Promise<void> {
  const changelogs = [...graph.stores.values()].map(({ changelogTopic }) => changelogTopic)
  if (changelogs.length === 0) return

  const admin = new Admin({ clientId, bootstrapBrokers: brokers })
  try {
    const partitions = await inputPartitions(admin, [...graph.sources.keys()])
    const existing = new Set(await admin.listTopics())
    const missing = changelogs.filter((topic) => !existing.has(topic))
    if (missing.length > 0) {
      await admin
        .createTopics({
          topics: missing,
          partitions,
          replicas: replicationFactor,
          configs: [{ name: 'cleanup.policy', value: 'compact' }],
        })
        .catch(async (error: unknown) => {
          // Fine if another instance, started at the same time, created them first.
          const now = new Set(await admin.listTopics())
          if (missing.some((topic) => !now.has(topic))) throw error
        })
    }
    await checkPartitions(admin, changelogs, partitions)
  } finally {
    await admin.close()
  }
}

/** The partition count of the input topics, i.e. the number of Tasks. */
async function inputPartitions(admin: Admin, topics: string[]): Promise<number> {
  const metadata = await admin.metadata({ topics })
  return Math.max(...topics.map((topic) => metadata.topics.get(topic)?.partitionsCount ?? 0))
}

/** Fails on a Changelog with the wrong partition count; waits until all have leaders. */
async function checkPartitions(admin: Admin, topics: string[], expected: number): Promise<void> {
  for (let attempt = 0; ; attempt++) {
    const metadata = await admin.metadata({ topics, forceUpdate: true })
    let ready = true
    for (const topic of topics) {
      const found = metadata.topics.get(topic)
      if (found === undefined) {
        ready = false
        continue
      }
      if (found.partitionsCount !== expected) {
        const count = `${found.partitionsCount} partition${found.partitionsCount === 1 ? '' : 's'}`
        throw new Error(
          `Changelog "${topic}" has ${count}, but the input topics have ${expected}: delete it or give it ${expected} partitions`,
        )
      }
      if (!found.partitions.every((partition) => partition.leader >= 0)) ready = false
    }
    if (ready) return
    if (attempt === 100)
      throw new Error(`Changelogs ${topics.join(', ')} have no leaders after 10 s`)
    await new Promise((resolve) => setTimeout(resolve, 100))
  }
}

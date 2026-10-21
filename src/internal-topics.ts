// Internal: creates the Internal Topics an application needs (its Changelogs) before a Worker
// joins the consumer group, and checks the ones that already exist.

import { Admin } from '@platformatic/kafka'
import type { ProcessorGraph } from './processor-graph.js'

export interface InternalTopicOptions {
  applicationId: string
  brokers: string[]
  clientId: string
  /** The replication factor of the Changelogs created. */
  replicationFactor: number
  /** Create missing Changelogs (true), or only check that they exist (false). */
  autoCreate: boolean
}

/**
 * Makes sure every State Store's Changelog exists: compacted, with as many partitions as the input
 * topics, so that Task `n` writes to Changelog partition `n`. Missing ones are created, or, when
 * `autoCreate` is off, listed in one error. An existing one with another partition count is an
 * error, because its partitions wouldn't line up with the Tasks. Resolves once every Changelog partition has a leader.
 */
export async function ensureChangelogs(
  graph: ProcessorGraph,
  { applicationId, brokers, clientId, replicationFactor, autoCreate }: InternalTopicOptions,
): Promise<void> {
  const changelogs = [...graph.stores.values()].map(({ changelogTopic }) => changelogTopic)
  if (changelogs.length === 0) return

  const admin = new Admin({ clientId, bootstrapBrokers: brokers })
  try {
    const partitions = await inputPartitions(admin, [...graph.sources.keys()])
    let existing = new Set(await admin.listTopics())
    let missing = changelogs.filter((topic) => !existing.has(topic))
    // Topics created just before (e.g. by the operator's tooling) can take a moment to be listed.
    for (let attempt = 0; attempt < 20 && missing.length > 0 && !autoCreate; attempt++) {
      await new Promise((resolve) => setTimeout(resolve, 100))
      existing = new Set(await admin.listTopics())
      missing = changelogs.filter((topic) => !existing.has(topic))
    }
    if (missing.length > 0 && !autoCreate) {
      throw new Error(missingMessage(applicationId, missing, partitions))
    }
    if (missing.length > 0) {
      await admin
        .createTopics({
          topics: missing,
          partitions,
          replicas: replicationFactor,
          configs: [{ name: 'cleanup.policy', value: 'compact' }],
        })
        .catch(async (error: unknown) => {
          // Fine if they exist after all: another instance, started at the same time, created them
          // first, or the broker's metadata didn't list them yet (it can lag behind a creation).
          for (let attempt = 0; attempt < 20; attempt++) {
            const now = new Set(await admin.listTopics())
            if (missing.every((topic) => now.has(topic))) return
            await new Promise((resolve) => setTimeout(resolve, 100))
          }
          throw error
        })
    }
    await checkPartitions(admin, applicationId, changelogs, partitions)
  } finally {
    await admin.close()
  }
}

function missingMessage(applicationId: string, missing: string[], partitions: number): string {
  const listed = missing.map((topic) => `  - "${topic}"`).join('\n')
  const count = `${partitions} partition${partitions === 1 ? '' : 's'}`
  return (
    `Streams App "${applicationId}" cannot start: autoCreateInternalTopics is off and ` +
    `${missing.length} Internal Topic${missing.length === 1 ? ' is' : 's are'} missing. Create ` +
    `${missing.length === 1 ? 'it' : 'each'} with ${count} and cleanup.policy=compact:\n${listed}`
  )
}

/** The partition count of the input topics, i.e. the number of Tasks. */
async function inputPartitions(admin: Admin, topics: string[]): Promise<number> {
  const metadata = await admin.metadata({ topics })
  return Math.max(...topics.map((topic) => metadata.topics.get(topic)?.partitionsCount ?? 0))
}

/** Fails on a Changelog with the wrong partition count; waits until all have leaders. */
async function checkPartitions(
  admin: Admin,
  applicationId: string,
  topics: string[],
  expected: number,
): Promise<void> {
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
          `Streams App "${applicationId}" cannot start: Internal Topic "${topic}" has ${count}, but ` +
            `it needs ${expected} (as many as the input topics, so that Task n uses partition n). ` +
            `Delete it, or recreate it with ${expected} partitions and cleanup.policy=compact.`,
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

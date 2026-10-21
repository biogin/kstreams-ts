// Internal: the startup check that a Streams App's input topics are Co-partitioned. A Task is one
// partition number across all of them, so their partition counts must match.

/** Reads topic metadata from the broker, as every `@platformatic/kafka` client can. */
export interface MetadataSource {
  metadata(options: {
    topics: string[]
    forceUpdate?: boolean
  }): Promise<{ topics: ReadonlyMap<string, { partitionsCount: number }> }>
}

/**
 * Resolves with the shared partition count of `topics`; rejects with an error naming every topic's
 * partition count if they differ.
 */
export async function checkCoPartitioned(
  client: MetadataSource,
  applicationId: string,
  topics: readonly string[],
): Promise<number> {
  const metadata = await client.metadata({ topics: [...topics], forceUpdate: true })
  const counts = topics.map((topic) => {
    const found = metadata.topics.get(topic)
    if (found === undefined) throw new Error(`Input topic "${topic}" does not exist`)
    return { topic, partitions: found.partitionsCount }
  })
  const [first] = counts
  if (first === undefined) return 0
  if (counts.every(({ partitions }) => partitions === first.partitions)) return first.partitions
  const listed = counts
    .map(({ topic, partitions }) => `"${topic}" has ${partitions} partitions`)
    .join(', ')
  throw new Error(
    `Streams App "${applicationId}" cannot start: its input topics are not Co-partitioned (${listed}). ` +
      'A Task reads the same partition number from every input topic, so they need the same ' +
      'partition count. Write the inputs to topics with matching counts (adding partitions ' +
      'later moves keys to other partitions).',
  )
}

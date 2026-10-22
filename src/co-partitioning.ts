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
 * The metadata of the input topics. A missing topic makes the client fail with its own "Unknown
 * topic" error, so on a failure each topic is asked for alone, and every missing one is reported
 * in a single error.
 */
async function inputMetadata(
  client: MetadataSource,
  applicationId: string,
  topics: readonly string[],
): Promise<ReadonlyMap<string, { partitionsCount: number }>> {
  try {
    const { topics: found } = await client.metadata({
      topics: [...topics],
      forceUpdate: true,
    })
    const missing = topics.filter((topic) => !found.has(topic))
    if (missing.length === 0) return found
    throw missingInputs(applicationId, missing)
  } catch (error) {
    if (error instanceof MissingInputsError) throw error
    const missing: string[] = []
    for (const topic of topics) {
      try {
        const { topics: found } = await client.metadata({
          topics: [topic],
          forceUpdate: true,
        })
        if (!found.has(topic)) missing.push(topic)
      } catch {
        missing.push(topic)
      }
    }
    if (missing.length === 0) throw error
    throw missingInputs(applicationId, missing)
  }
}

class MissingInputsError extends Error {}

function missingInputs(applicationId: string, missing: string[]): Error {
  const noun = missing.length === 1 ? 'input topic does' : 'input topics do'
  return new MissingInputsError(
    `Streams App "${applicationId}" cannot start: ${missing.length} ${noun} not exist: ` +
      `${missing.map((topic) => `"${topic}"`).join(', ')}. Create ${missing.length === 1 ? 'it' : 'them'} ` +
      'before starting the app (the app only creates its own Internal Topics).',
  )
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
  const metadata = await inputMetadata(client, applicationId, topics)
  const counts = topics.map((topic) => ({
    topic,
    partitions: metadata.get(topic)?.partitionsCount ?? 0,
  }))
  const [first] = counts
  if (first === undefined) return 0
  if (counts.every(({ partitions }) => partitions === first.partitions)) return first.partitions
  const listed = counts
    .map(({ topic, partitions }) => `"${topic}" has ${partitions} partitions`)
    .join(', ')
  throw new Error(
    `Streams App "${applicationId}" cannot start: its input topics are not Co-partitioned (${listed}). ` +
      'In kstreams-ts 0.1 all input topics of an app must be Co-partitioned: a Task reads the ' +
      'same partition number from every input topic, so they need the same partition count ' +
      '(there are no Repartition Topics yet). Write the inputs to topics with matching counts ' +
      '(adding partitions later moves keys to other partitions).',
  )
}

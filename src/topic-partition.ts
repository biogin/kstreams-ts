// Internal: a partition of a topic, and the string key that identifies it in maps and sets.

/** A partition of a topic. */
export interface TopicPartition {
  topic: string
  partition: number
}

/** The key of a {@link TopicPartition}, made by {@link topicPartitionKey}. */
export type TopicPartitionKey = string & { readonly topicPartitionKey: unique symbol }

/** The key of `partition` of `topic`: `<topic>:<partition>`. */
export function topicPartitionKey(topic: string, partition: number): TopicPartitionKey {
  return `${topic}:${partition}` as TopicPartitionKey
}

/** The topic and partition a {@link topicPartitionKey} key stands for. */
export function fromTopicPartitionKey(key: TopicPartitionKey): TopicPartition {
  // Topic names can't contain ':', but partition numbers come last either way.
  const at = key.lastIndexOf(':')
  return { topic: key.slice(0, at), partition: Number(key.slice(at + 1)) }
}

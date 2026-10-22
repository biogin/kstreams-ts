/** A keyed record together with its Event Time (milliseconds since the epoch). */
export interface StreamRecord<K, V> {
  key: K
  value: V
  timestamp: number
}

/**
 * Internal: throws unless `timestamp` is a safe integer, as a Kafka record's timestamp (an int64
 * of milliseconds) must be. Checked where records enter the graph, so the Test Driver rejects what
 * Kafka would.
 */
export function checkTimestamp(timestamp: number): void {
  if (!Number.isSafeInteger(timestamp)) {
    throw new TypeError(
      `A record's timestamp must be an integer number of milliseconds, got ${timestamp}`,
    )
  }
}

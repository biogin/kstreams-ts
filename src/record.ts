/** A keyed record together with its Event Time (milliseconds since the epoch). */
export interface StreamRecord<K, V> {
  key: K
  value: V
  timestamp: number
}

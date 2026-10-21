/**
 * A typed pair of serializer and deserializer for record keys or values.
 *
 * Implement this interface to plug in your own format (Avro, Protobuf, ...).
 */
export interface Serde<T> {
  serialize(value: T): Uint8Array
  deserialize(bytes: Uint8Array): T
}

/** The key and value Serdes of a topic. */
export interface Serdes<K, V> {
  key: Serde<K>
  value: Serde<V>
}

const encoder = new TextEncoder()
const decoder = new TextDecoder()

/** Built-in Serdes. */
export const serdes = {
  /** UTF-8 text. */
  string: {
    serialize: (value: string) => encoder.encode(value),
    deserialize: (bytes: Uint8Array) => decoder.decode(bytes),
  } satisfies Serde<string> as Serde<string>,
}

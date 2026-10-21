/** What a Serde is told about the bytes it is converting. */
export interface SerdeContext {
  /** The topic the bytes are read from or written to (e.g. to pick a Schema Registry subject). */
  readonly topic: string
}

/**
 * A typed pair of serializer and deserializer for record keys or values.
 *
 * Implement this interface to plug in your own format (Avro, Protobuf, ...). A Serde never sees
 * `null`: a null key or value is passed through as is, and a null value is a tombstone. Throw from
 * `deserialize` when the bytes are malformed; the Streams App reports it as a
 * {@link DeserializationError}.
 *
 * @example
 * const point: Serde<{ x: number; y: number }> = {
 *   serialize: ({ x, y }) => new TextEncoder().encode(`${x},${y}`),
 *   deserialize: (bytes) => {
 *     const [x, y] = new TextDecoder().decode(bytes).split(',').map(Number)
 *     return { x, y }
 *   },
 * }
 */
export interface Serde<T> {
  serialize(value: T, context: SerdeContext): Uint8Array
  deserialize(bytes: Uint8Array, context: SerdeContext): T
}

/** The key and value Serdes of a topic. */
export interface Serdes<K, V> {
  key: Serde<K>
  value: Serde<V>
}

/** Raised when a record read from a topic can't be deserialized by that topic's Serde. */
export class DeserializationError extends Error {
  override readonly name = 'DeserializationError'
  /** The topic the record was read from. */
  readonly topic: string
  /** Which part of the record failed to deserialize. */
  readonly part: 'key' | 'value'
  /** Event Time of the record. */
  readonly timestamp: number

  constructor(
    topic: string,
    part: 'key' | 'value',
    timestamp: number,
    options: { cause: unknown },
  ) {
    const reason = options.cause instanceof Error ? options.cause.message : String(options.cause)
    super(`Cannot deserialize the ${part} of a record from topic "${topic}": ${reason}`, options)
    this.topic = topic
    this.part = part
    this.timestamp = timestamp
  }
}

const encoder = new TextEncoder()
const decoder = new TextDecoder()
const strictDecoder = new TextDecoder('utf-8', { fatal: true })

/** Built-in Serdes. */
export const serdes = {
  /** UTF-8 text. */
  string: {
    serialize: (value: string) => encoder.encode(value),
    deserialize: (bytes: Uint8Array) => decoder.decode(bytes),
  } satisfies Serde<string> as Serde<string>,

  /**
   * JSON as UTF-8 text. `T` is trusted, not checked: validate the parsed value yourself if the
   * topic's producers can't be trusted.
   */
  json<T>(): Serde<T> {
    return {
      serialize(value) {
        const text = JSON.stringify(value)
        if (text === undefined) throw new TypeError(`Cannot serialize ${typeof value} as JSON`)
        return encoder.encode(text)
      },
      deserialize: (bytes) => JSON.parse(strictDecoder.decode(bytes)) as T,
    }
  },

  /** A number as an 8-byte big-endian IEEE 754 double (compatible with Kafka's DoubleSerde). */
  number: {
    serialize(value: number) {
      const bytes = new Uint8Array(8)
      new DataView(bytes.buffer).setFloat64(0, value)
      return bytes
    },
    deserialize(bytes: Uint8Array) {
      if (bytes.byteLength !== 8) {
        throw new RangeError(`Expected 8 bytes for a number, got ${bytes.byteLength}`)
      }
      return new DataView(bytes.buffer, bytes.byteOffset, 8).getFloat64(0)
    },
  } satisfies Serde<number> as Serde<number>,

  /** Raw bytes, unchanged. */
  bytes: {
    serialize: (value: Uint8Array) => value,
    deserialize: (bytes: Uint8Array) => bytes,
  } satisfies Serde<Uint8Array> as Serde<Uint8Array>,
}

// Internal (not exported from the package entry point): null-aware wrappers the runtime calls, so
// that null keys and tombstones never reach a Serde.

export function serializeNullable<T>(
  serde: Serde<T>,
  value: T | null,
  context: SerdeContext,
): Uint8Array | null {
  return value === null ? null : serde.serialize(value, context)
}

export function deserializeNullable<T>(
  serde: Serde<T>,
  bytes: Uint8Array | null,
  context: SerdeContext,
): T | null {
  return bytes === null ? null : serde.deserialize(bytes, context)
}

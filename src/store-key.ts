// Internal: the store key encoding (ADR-0005). Typed keys are encoded to deterministic bytes whose
// unsigned byte order (as SQLite and the in-memory store compare them) is the keys' own order,
// whatever the key Serde, so range scans work. Keys of other types fall back to their Serde bytes.
//
// A store key is a sequence of parts; each part is a tag byte followed by its payload, and is
// self-delimiting, so a key's encoding is a prefix of the encoding of every key that extends it
// (e.g. a window key `[key, windowStart]` starts with the encoding of `[key]`). Scan those keys
// from `encodeKey(parts)` to `prefixEnd(encodeKey(parts))`.
//
//   0x01                    null
//   0x02 / 0x03             false / true
//   0x10 + 8 bytes          number: IEEE 754 big-endian, sign bit flipped for positives, all bits
//                           flipped for negatives. -0 is encoded as 0, every NaN as the canonical
//                           NaN (sorting after Infinity).
//   0x20 + escaped + 0x00   string: UTF-8 (code point order); 0x00 inside is escaped as 0x00 0xFF
//   0x30 + escaped + 0x00   bytes (Uint8Array), escaped the same way
//   0x40 + parts + 0x00     array: its elements' parts, then a terminator that sorts first, so a
//                           shorter array sorts before the arrays it is a prefix of
//   0x50 + escaped + 0x00   Serde bytes of a key with no encoding of its own (objects, bigints,
//                           strings that aren't well-formed UTF-16, arrays holding any of these)
//
// Values of different types sort by tag. The format is ours (ADR-0003) and is part of the on-disk
// format of State Stores: changing it needs a migration or a Restore.

import type { Serde, SerdeContext } from './serde.js'

/** A value the store key encoding orders by itself. */
export type OrderedValue = null | boolean | number | string | Uint8Array | readonly OrderedValue[]

/** The Serde bytes of a key that has no ordered encoding; ordered as bytes. */
export class SerializedKey {
  constructor(readonly bytes: Uint8Array) {}
}

export type KeyPart = OrderedValue | SerializedKey

const NULL = 0x01
const FALSE = 0x02
const TRUE = 0x03
const NUMBER = 0x10
const STRING = 0x20
const BYTES = 0x30
const ARRAY = 0x40
const SERIALIZED = 0x50
const END = 0x00
const ESCAPE = 0xff

const encoder = new TextEncoder()
const decoder = new TextDecoder('utf-8', { fatal: true })

/** Whether the encoding orders `value` itself, without its Serde. */
export function isOrderedValue(value: unknown): value is OrderedValue {
  switch (typeof value) {
    case 'boolean':
    case 'number':
      return true
    case 'string':
      return value.isWellFormed()
    case 'object':
      return (
        value === null ||
        value instanceof Uint8Array ||
        (Array.isArray(value) && value.every(isOrderedValue))
      )
    default:
      return false
  }
}

/** Encodes the parts of a store key, one after another. */
export function encodeKey(parts: readonly KeyPart[]): Uint8Array {
  const out: number[] = []
  for (const part of parts) writePart(out, part)
  return Uint8Array.from(out)
}

/**
 * The exclusive end of a range scan for the keys that start with the parts encoded as `prefix`:
 * scan from `prefix` to `prefixEnd(prefix)`. The scan stops before `prefix` + 0xFF, because 0xFF
 * right after a part's terminator is an escaped 0x00 (the part goes on, e.g. `'a\0'` after `'a'`)
 * and no tag is 0xFF.
 */
export function prefixEnd(prefix: Uint8Array): Uint8Array {
  const end = new Uint8Array(prefix.length + 1)
  end.set(prefix)
  end[prefix.length] = ESCAPE
  return end
}

/** Decodes the parts of a store key. Throws if `bytes` isn't one. */
export function decodeKey(bytes: Uint8Array): KeyPart[] {
  const reader = { bytes, at: 0 }
  const parts: KeyPart[] = []
  while (reader.at < bytes.length) parts.push(readPart(reader))
  return parts
}

/** The store key of a typed key: its own encoding if it has one, else its Serde bytes. */
export function storeKey<K>(key: K, serde: Serde<K>, context: SerdeContext): Uint8Array {
  return encodeKey([isOrderedValue(key) ? key : new SerializedKey(serde.serialize(key, context))])
}

/** The typed key of a store key made by {@link storeKey}. */
export function keyOf<K>(bytes: Uint8Array, serde: Serde<K>, context: SerdeContext): K {
  const parts = decodeKey(bytes)
  const [part] = parts
  if (parts.length !== 1) throw invalid('expected a single part')
  return part instanceof SerializedKey ? serde.deserialize(part.bytes, context) : (part as K)
}

function writePart(out: number[], part: KeyPart): void {
  if (part === null) out.push(NULL)
  else if (typeof part === 'boolean') out.push(part ? TRUE : FALSE)
  else if (typeof part === 'number') writeNumber(out, part)
  else if (typeof part === 'string') writeEscaped(out, STRING, encoder.encode(part))
  else if (part instanceof Uint8Array) writeEscaped(out, BYTES, part)
  else if (part instanceof SerializedKey) writeEscaped(out, SERIALIZED, part.bytes)
  else {
    out.push(ARRAY)
    for (const element of part) writePart(out, element)
    out.push(END)
  }
}

function writeNumber(out: number[], value: number): void {
  const bytes = new Uint8Array(8)
  // `+ 0` turns -0 into 0; setFloat64 writes every NaN as the canonical NaN.
  new DataView(bytes.buffer).setFloat64(0, value + 0)
  if ((bytes[0] as number) & 0x80) for (let i = 0; i < 8; i++) bytes[i] = ~(bytes[i] as number)
  else bytes[0] = (bytes[0] as number) ^ 0x80
  out.push(NUMBER, ...bytes)
}

function writeEscaped(out: number[], tag: number, bytes: Uint8Array): void {
  out.push(tag)
  for (const byte of bytes) {
    out.push(byte)
    if (byte === END) out.push(ESCAPE)
  }
  out.push(END)
}

interface Reader {
  readonly bytes: Uint8Array
  at: number
}

function readPart(reader: Reader): KeyPart {
  const tag = reader.bytes[reader.at++]
  switch (tag) {
    case NULL:
      return null
    case FALSE:
      return false
    case TRUE:
      return true
    case NUMBER:
      return readNumber(reader)
    case STRING:
      try {
        return decoder.decode(readEscaped(reader))
      } catch (error) {
        throw invalid('a string is not valid UTF-8', error)
      }
    case BYTES:
      return readEscaped(reader)
    case SERIALIZED:
      return new SerializedKey(readEscaped(reader))
    case ARRAY: {
      const elements: KeyPart[] = []
      while (reader.bytes[reader.at] !== END) {
        if (reader.at >= reader.bytes.length) throw invalid('an array has no end')
        const element = readPart(reader)
        if (element instanceof SerializedKey) throw invalid('Serde bytes inside an array')
        elements.push(element)
      }
      reader.at++
      return elements as OrderedValue[]
    }
    default:
      throw invalid(`unknown tag ${tag}`)
  }
}

function readNumber(reader: Reader): number {
  if (reader.at + 8 > reader.bytes.length) throw invalid('a number is cut short')
  const bytes = reader.bytes.slice(reader.at, reader.at + 8)
  reader.at += 8
  if ((bytes[0] as number) & 0x80) bytes[0] = (bytes[0] as number) ^ 0x80
  else for (let i = 0; i < 8; i++) bytes[i] = ~(bytes[i] as number)
  return new DataView(bytes.buffer).getFloat64(0)
}

function readEscaped(reader: Reader): Uint8Array {
  const out: number[] = []
  const { bytes } = reader
  for (;;) {
    if (reader.at >= bytes.length) throw invalid('a string or bytes part has no end')
    const byte = bytes[reader.at++] as number
    if (byte !== END) out.push(byte)
    else if (bytes[reader.at] === ESCAPE) {
      out.push(END)
      reader.at++
    } else return Uint8Array.from(out)
  }
}

function invalid(reason: string, cause?: unknown): Error {
  return new Error(`Invalid store key: ${reason}`, cause === undefined ? undefined : { cause })
}

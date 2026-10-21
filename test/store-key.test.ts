// Store key encoding (ADR-0005): typed keys become bytes whose order is the keys' own order,
// whatever their Serde, so range scans (and windows) work.

import { describe, expect, it } from 'vitest'
import { serdes } from '../src/index.js'
import { decodeKey, encodeKey, type KeyPart, prefixEnd, SerializedKey } from '../src/store-key.js'

/** Negative when a's encoding sorts before b's, as SQLite and the in-memory store compare. */
const compareBytes = (a: Uint8Array, b: Uint8Array): number => Buffer.compare(a, b)

const sortedByEncoding = (parts: KeyPart[]): KeyPart[] =>
  parts
    .map((part) => ({ part, bytes: encodeKey([part]) }))
    .sort((a, b) => compareBytes(a.bytes, b.bytes))
    .map(({ part }) => part)

describe('store key encoding', () => {
  it('orders numbers numerically, negatives and infinities included', () => {
    const numbers = [10, -1, 2, 0, -Infinity, 0.5, -100.25, Infinity, Number.MAX_SAFE_INTEGER, -2]
    expect(sortedByEncoding(numbers)).toEqual([...numbers].sort((a, b) => a - b))
  })

  it('encodes -0 like 0 and every NaN alike, after Infinity', () => {
    expect(encodeKey([-0])).toEqual(encodeKey([0]))
    expect(sortedByEncoding([Number.NaN, Infinity, 1])).toEqual([1, Infinity, Number.NaN])
    expect(decodeKey(encodeKey([-0]))).toEqual([0])
  })

  it('orders strings by code point, shorter prefix first, NUL characters included', () => {
    const strings = ['b', 'a\u0000', 'a', '', 'ab', '\u0000', 'é', 'Z', '😀', '￿']
    expect(sortedByEncoding(strings)).toEqual([
      '',
      '\u0000',
      'Z',
      'a',
      'a\u0000',
      'ab',
      'b',
      'é',
      '￿',
      '😀',
    ])
  })

  it('orders byte arrays by unsigned bytes, shorter prefix first', () => {
    const b = (...values: number[]) => new Uint8Array(values)
    expect(sortedByEncoding([b(1), b(0, 0), b(), b(0), b(255), b(0, 255)])).toEqual([
      b(),
      b(0),
      b(0, 0),
      b(0, 255),
      b(1),
      b(255),
    ])
  })

  it('orders arrays element by element, shorter prefix first', () => {
    expect(
      sortedByEncoding([['a', 2], ['a', 10], ['a'], [], ['b'], [['a'], 1], ['a', 2, 0]]),
    ).toEqual([[], ['a'], ['a', 2], ['a', 2, 0], ['a', 10], ['b'], [['a'], 1]])
  })

  it('orders values of different types by type: null, false, true, numbers, strings, bytes, arrays', () => {
    const parts: KeyPart[] = [[1], new Uint8Array([0]), 'a', 1, true, false, null]
    expect(sortedByEncoding(parts)).toEqual([null, false, true, 1, 'a', new Uint8Array([0]), [1]])
  })

  it('decodes what it encodes', () => {
    const parts: KeyPart[] = [
      null,
      true,
      false,
      -1.5,
      'a\u0000b',
      new Uint8Array([0, 1, 255]),
      ['x', [null, 2]],
      new SerializedKey(new Uint8Array([0, 0])),
    ]
    expect(decodeKey(encodeKey(parts))).toEqual(parts)
  })

  it('range-scans the keys that extend a key with its parts from encodeKey to prefixEnd', () => {
    // A window key: the record key, then the window start.
    const windowKey = (key: string, start: number) => encodeKey([key, start])
    const keys = [
      windowKey('', 0),
      windowKey('a', 0),
      windowKey('a', 1000),
      windowKey('a\u0000', 0),
      windowKey('ab', -5),
      windowKey('b', 0),
    ]
    const from = encodeKey(['a'])
    const to = prefixEnd(from)
    const extending = keys.filter(
      (key) => compareBytes(key, from) >= 0 && compareBytes(key, to) < 0,
    )
    expect(extending.map(decodeKey)).toEqual([
      ['a', 0],
      ['a', 1000],
    ])
  })

  it('orders Serde bytes of other keys (e.g. objects) as bytes, after native values', () => {
    const json = serdes.json<{ id: number }>()
    const serialized = (id: number) => new SerializedKey(json.serialize({ id }, { topic: 't' }))
    expect(sortedByEncoding([serialized(2), [1], serialized(1)])).toEqual([
      [1],
      serialized(1),
      serialized(2),
    ])
  })

  it('rejects bytes that are not an encoded key', () => {
    expect(() => decodeKey(new Uint8Array([0xee]))).toThrow(/store key/)
    expect(() => decodeKey(new Uint8Array([0x20, 0x61]))).toThrow(/store key/)
  })
})

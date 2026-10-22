// Seam 3: the State Store contract suite. Every State Store implementation runs it, because
// State Stores are a public, pluggable interface (ADR-0005).

import { afterEach, describe, expect, it } from 'vitest'
import type { StateStore } from '../src/index.js'

const bytes = (...values: number[]) => new Uint8Array(values)
const text = (value: string) => new TextEncoder().encode(value)

/** Runs the contract against stores made by `open`, a fresh empty store per test. */
export function describeStateStoreContract(implementation: string, open: () => StateStore): void {
  describe(`State Store contract: ${implementation}`, () => {
    const opened: StateStore[] = []
    const store = (): StateStore => {
      const s = open()
      opened.push(s)
      return s
    }
    afterEach(() => {
      for (const s of opened.splice(0)) s.close()
    })

    describe('get, put and delete', () => {
      it('returns undefined for a key that was never written', () => {
        expect(store().get(text('missing'))).toBeUndefined()
      })

      it('returns the value put for a key', () => {
        const s = store()
        s.put(text('a'), text('1'))
        expect(s.get(text('a'))).toEqual(text('1'))
      })

      it('compares keys by their bytes, not by identity', () => {
        const s = store()
        s.put(bytes(1, 2), text('x'))
        expect(s.get(bytes(1, 2))).toEqual(text('x'))
        expect(s.get(bytes(1))).toBeUndefined()
        expect(s.get(bytes(1, 2, 0))).toBeUndefined()
      })

      it('overwrites the value of an existing key', () => {
        const s = store()
        s.put(text('a'), text('1'))
        s.put(text('a'), text('2'))
        expect(s.get(text('a'))).toEqual(text('2'))
      })

      it('deletes a key, and ignores deleting a missing one', () => {
        const s = store()
        s.put(text('a'), text('1'))
        s.delete(text('a'))
        s.delete(text('never'))
        expect(s.get(text('a'))).toBeUndefined()
        expect([...s.all()]).toEqual([])
      })

      it('accepts empty keys and values', () => {
        const s = store()
        s.put(bytes(), bytes())
        expect(s.get(bytes())).toEqual(bytes())
      })

      it('is not affected by later changes to the bytes it was given or returned', () => {
        const s = store()
        const key = text('a')
        const value = text('1')
        s.put(key, value)
        key[0] = 0
        value[0] = 0
        const read = s.get(text('a'))
        if (read !== undefined) read[0] = 0
        expect(s.get(text('a'))).toEqual(text('1'))
      })
    })

    describe('all', () => {
      it('returns every entry in key byte order', () => {
        const s = store()
        for (const key of [bytes(2), bytes(1, 0), bytes(), bytes(1), bytes(0, 255), bytes(255)]) {
          s.put(key, bytes(key.length))
        }
        expect([...s.all()]).toEqual([
          [bytes(), bytes(0)],
          [bytes(0, 255), bytes(2)],
          [bytes(1), bytes(1)],
          [bytes(1, 0), bytes(2)],
          [bytes(2), bytes(1)],
          [bytes(255), bytes(1)],
        ])
      })
    })

    describe('range', () => {
      const filled = (): StateStore => {
        const s = store()
        for (const key of [bytes(2), bytes(1, 0), bytes(), bytes(1), bytes(0, 255), bytes(255)]) {
          s.put(key, bytes(key.length))
        }
        return s
      }

      it('returns the entries from `from` (inclusive) to `to` (exclusive) in key byte order', () => {
        expect([...filled().range(bytes(0, 255), bytes(2))]).toEqual([
          [bytes(0, 255), bytes(2)],
          [bytes(1), bytes(1)],
          [bytes(1, 0), bytes(2)],
        ])
      })

      it('compares bounds by bytes, so a bound need not be a key of the store', () => {
        expect([...filled().range(bytes(1, 0, 0), bytes(254))]).toEqual([[bytes(2), bytes(1)]])
      })

      it('scans from the first key when `from` is undefined, to the last when `to` is', () => {
        const s = filled()
        expect([...s.range(undefined, bytes(1))]).toEqual([
          [bytes(), bytes(0)],
          [bytes(0, 255), bytes(2)],
        ])
        expect([...s.range(bytes(2), undefined)]).toEqual([
          [bytes(2), bytes(1)],
          [bytes(255), bytes(1)],
        ])
        expect([...s.range(undefined, undefined)]).toEqual([...s.all()])
      })

      it('is empty when `from` is not before `to`', () => {
        const s = filled()
        expect([...s.range(bytes(1), bytes(1))]).toEqual([])
        expect([...s.range(bytes(2), bytes(1))]).toEqual([])
      })

      it('sees the writes of an open transaction, and not after rollback', () => {
        const s = filled()
        s.begin()
        s.put(bytes(1, 5), bytes(9))
        s.delete(bytes(1))
        expect([...s.range(bytes(1), bytes(2))]).toEqual([
          [bytes(1, 0), bytes(2)],
          [bytes(1, 5), bytes(9)],
        ])
        s.rollback()
        expect([...s.range(bytes(1), bytes(2))]).toEqual([
          [bytes(1), bytes(1)],
          [bytes(1, 0), bytes(2)],
        ])
      })

      it('returns bytes the caller may change', () => {
        const s = filled()
        for (const [key, value] of s.range(bytes(1), bytes(2))) {
          key[0] = 9
          value[0] = 9
        }
        expect(s.get(bytes(1))).toEqual(bytes(1))
      })
    })

    describe('transactions', () => {
      it('applies writes outside a transaction immediately', () => {
        const s = store()
        s.put(text('a'), text('1'))
        s.begin()
        s.put(text('b'), text('2'))
        s.rollback()
        expect(s.get(text('a'))).toEqual(text('1'))
      })

      it('sees its own writes inside a transaction, and keeps them on commit', () => {
        const s = store()
        s.begin()
        s.put(text('a'), text('1'))
        expect(s.get(text('a'))).toEqual(text('1'))
        s.commit()
        expect(s.get(text('a'))).toEqual(text('1'))
      })

      it('undoes every put, overwrite and delete since begin on rollback', () => {
        const s = store()
        s.put(text('kept'), text('1'))
        s.put(text('overwritten'), text('1'))
        s.put(text('deleted'), text('1'))
        s.begin()
        s.put(text('added'), text('2'))
        s.put(text('overwritten'), text('2'))
        s.put(text('overwritten'), text('3'))
        s.delete(text('deleted'))
        s.put(text('added-then-deleted'), text('2'))
        s.delete(text('added-then-deleted'))
        s.rollback()
        expect([...s.all()]).toEqual([
          [text('deleted'), text('1')],
          [text('kept'), text('1')],
          [text('overwritten'), text('1')],
        ])
      })

      it('allows a new transaction after commit or rollback', () => {
        const s = store()
        s.begin()
        s.put(text('a'), text('1'))
        s.commit()
        s.begin()
        s.put(text('a'), text('2'))
        s.rollback()
        s.begin()
        s.delete(text('a'))
        s.commit()
        expect(s.get(text('a'))).toBeUndefined()
      })

      it('rejects begin inside a transaction, and commit or rollback outside one', () => {
        const s = store()
        expect(() => s.commit()).toThrow(/transaction/i)
        expect(() => s.rollback()).toThrow(/transaction/i)
        s.begin()
        expect(() => s.begin()).toThrow(/transaction/i)
      })
    })

    describe('clear', () => {
      it('removes every entry, and leaves the store usable', () => {
        const s = store()
        for (const key of ['a', 'b', 'c']) s.put(text(key), text('1'))
        s.clear()
        expect([...s.all()]).toEqual([])
        expect(s.get(text('a'))).toBeUndefined()
        s.put(text('d'), text('2'))
        expect([...s.all()]).toEqual([[text('d'), text('2')]])
      })

      it('does nothing on an empty store', () => {
        const s = store()
        s.clear()
        expect([...s.all()]).toEqual([])
      })

      it('is undone by rollback inside a transaction, and kept by commit', () => {
        const s = store()
        s.put(text('a'), text('1'))
        s.put(text('b'), text('1'))
        s.begin()
        s.put(text('b'), text('2'))
        s.clear()
        s.put(text('c'), text('2'))
        expect([...s.all()]).toEqual([[text('c'), text('2')]])
        s.rollback()
        expect([...s.all()]).toEqual([
          [text('a'), text('1')],
          [text('b'), text('1')],
        ])
        s.begin()
        s.clear()
        s.put(text('c'), text('3'))
        s.commit()
        expect([...s.all()]).toEqual([[text('c'), text('3')]])
      })
    })

    describe('Buffers and mutation', () => {
      it('keeps stored bytes apart from the Buffers given to put', () => {
        const s = store()
        const key = Buffer.from('k')
        const value = Buffer.from('abc')
        s.put(key, value)
        value[0] = 0x7a
        key[0] = 0x7a
        expect(s.get(text('k'))).toEqual(text('abc'))
        expect([...s.all()].map(([k]) => k)).toEqual([text('k')])
      })

      it('returns bytes that can be changed without changing the store', () => {
        const s = store()
        s.put(text('k'), text('abc'))
        const got = s.get(text('k'))
        got?.fill(0)
        for (const [k, v] of s.all()) {
          k.fill(0)
          v.fill(0)
        }
        for (const [k, v] of s.range(undefined, undefined)) {
          k.fill(0)
          v.fill(0)
        }
        expect(s.get(text('k'))).toEqual(text('abc'))
        expect([...s.all()]).toEqual([[text('k'), text('abc')]])
      })
    })
  })
}

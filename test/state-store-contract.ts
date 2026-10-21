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
  })
}

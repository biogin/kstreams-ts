import { existsSync, mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterAll, describe, expect, it } from 'vitest'
import { type StateStoreContext, stores } from '../src/index.js'
import { describeStateStoreContract } from './state-store-contract.js'

const root = mkdtempSync(join(tmpdir(), 'kstreams-sqlite-'))
afterAll(() => rmSync(root, { recursive: true, force: true }))

let next = 0
/** A context for a fresh store: its own Task directory under the temp root. */
const context = (name = 'contract'): StateStoreContext => ({
  name,
  applicationId: 'sqlite-test',
  task: 0,
  directory: join(root, String(next++)),
})

describeStateStoreContract('SQLite (file)', () => stores.sqlite()(context()))
describeStateStoreContract('SQLite (no state directory)', () =>
  stores.sqlite()({ ...context(), directory: undefined }),
)

describe('SQLite State Store', () => {
  const text = (value: string) => new TextEncoder().encode(value)

  it('creates <directory>/<store>.sqlite, directory included', () => {
    const ctx = context('counts')
    const store = stores.sqlite()(ctx)
    store.close()
    expect(existsSync(join(ctx.directory ?? '', 'counts.sqlite'))).toBe(true)
  })

  it('keeps committed writes when closed and opened again', () => {
    const ctx = context()
    const first = stores.sqlite()(ctx)
    first.put(text('autocommitted'), text('1'))
    first.begin()
    first.put(text('committed'), text('2'))
    first.commit()
    first.close()

    const second = stores.sqlite()(ctx)
    expect([...second.all()]).toEqual([
      [text('autocommitted'), text('1')],
      [text('committed'), text('2')],
    ])
    second.close()
  })

  it('loses the writes of a transaction still open when it is closed', () => {
    const ctx = context()
    const first = stores.sqlite()(ctx)
    first.put(text('kept'), text('1'))
    first.begin()
    first.put(text('lost'), text('2'))
    first.close()

    const second = stores.sqlite()(ctx)
    expect([...second.all()]).toEqual([[text('kept'), text('1')]])
    second.close()
  })
})

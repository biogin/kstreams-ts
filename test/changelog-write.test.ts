import { describe, expect, it } from 'vitest'
import { type Serde, serdes, stores } from '../src/index.js'
import { keyValueStore } from '../src/state-store.js'

const context = { topic: 'app-store-changelog' }
const open = () =>
  stores.inMemory()({ name: 'store', applicationId: 'app', task: 0, directory: undefined })

describe('writing to a State Store with a Changelog', () => {
  it('leaves the store unchanged when the Changelog key Serde fails', () => {
    const broken: Serde<string> = {
      serialize: () => {
        throw new Error('key Serde failed')
      },
      deserialize: (bytes, ctx) => serdes.string.deserialize(bytes, ctx),
    }
    const logged: unknown[] = []
    const backing = open()
    const view = keyValueStore<string, string>(
      backing,
      { key: broken, value: serdes.string },
      context,
      undefined,
      (key, value) => logged.push([key, value]),
    )

    expect(() => view.put('k', 'v')).toThrow(/key Serde failed/)
    expect(() => view.delete('k')).toThrow(/key Serde failed/)

    expect([...backing.all()]).toEqual([])
    expect(logged).toEqual([])
  })

  it('fails with an unskippable error when the Changelog write fails after the store changed', () => {
    const view = keyValueStore<string, string>(
      open(),
      { key: serdes.string, value: serdes.string },
      context,
      undefined,
      () => {
        throw new Error('produce failed')
      },
    )

    expect(() => view.put('k', 'v')).toThrow(/could not be logged/)
  })
})

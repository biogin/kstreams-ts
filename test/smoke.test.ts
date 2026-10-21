import { describe, expect, it } from 'vitest'

describe('package entry point', () => {
  it('loads as an ES module', async () => {
    const mod = await import('../src/index.js')
    expect(mod).toBeTypeOf('object')
  })
})

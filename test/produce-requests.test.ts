import { describe, expect, it } from 'vitest'
import { type Output, splitBySize } from '../src/worker.js'

const output = (value: number, bytes: number): Output => ({
  topic: 't',
  key: Buffer.from(String(value)),
  value: Buffer.alloc(bytes),
})

describe('splitBySize', () => {
  it('splits outputs, in order, into requests of at most the given bytes', () => {
    const outputs = Array.from({ length: 10 }, (_, i) => output(i, 1_000))
    const requests = splitBySize(outputs, 3_500)
    expect(requests.map((request) => request.length)).toEqual([3, 3, 3, 1])
    expect(requests.flat()).toEqual(outputs)
  })

  it('sends a record larger than the bound on its own', () => {
    const outputs = [output(0, 10), output(1, 5_000), output(2, 10)]
    expect(splitBySize(outputs, 1_000).map((request) => request.length)).toEqual([1, 1, 1])
  })

  it('keeps everything in one request when it fits', () => {
    expect(splitBySize([output(0, 10), output(1, 10)], 1_000)).toHaveLength(1)
    expect(splitBySize([], 1_000)).toEqual([])
  })
})

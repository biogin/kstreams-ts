// The user docs' code examples are real: every ```ts block in the README and docs/ is a verbatim
// excerpt of a file under examples/ (a line `// ...` stands for skipped code), and `npm run
// typecheck` checks those files. The Test Driver example asserts something, so it runs here too.

import { readdirSync, readFileSync } from 'node:fs'
import { join } from 'node:path'
import { describe, expect, it } from 'vitest'

const root = new URL('..', import.meta.url).pathname
const sources = readdirSync(join(root, 'examples'))
  .filter((name) => name.endsWith('.ts'))
  .map((name) => normalize(readFileSync(join(root, 'examples', name), 'utf8')))
const docs = [
  'README.md',
  ...readdirSync(join(root, 'docs'))
    .filter((name) => name.endsWith('.md'))
    .map((name) => join('docs', name)),
]

function normalize(code: string): string {
  return code.replace(/\s+/g, ' ').trim()
}

describe('user docs', () => {
  for (const doc of docs) {
    it(`${doc}: every ts code block comes from examples/`, () => {
      const text = readFileSync(join(root, doc), 'utf8')
      for (const [, block = ''] of text.matchAll(/```ts\n([\s\S]*?)```/g)) {
        const pieces = block
          .split(/^\s*\/\/ \.\.\.\s*$/m)
          .map(normalize)
          .filter((piece) => piece !== '')
        expect(
          sources.some((source) => pieces.every((piece) => source.includes(piece))),
          `not found in examples/:\n${block}`,
        ).toBe(true)
      }
    })
  }

  it('the Test Driver example passes', async () => {
    await import('../examples/test-driver.js')
  })
})

// Preloaded (`--import`) into child processes so plain Node can run the TypeScript sources with its
// built-in type stripping: the sources import `./x.js` (NodeNext style), which only exists as
// `./x.ts` until the build runs.

import { registerHooks } from 'node:module'

registerHooks({
  resolve(specifier, context, nextResolve) {
    try {
      return nextResolve(specifier, context)
    } catch (error) {
      const fromTypeScript = context.parentURL?.endsWith('.ts') ?? false
      if (fromTypeScript && specifier.startsWith('.') && specifier.endsWith('.js')) {
        return nextResolve(`${specifier.slice(0, -'.js'.length)}.ts`, context)
      }
      throw error
    }
  },
})

import { defineConfig } from 'vitest/config'

export default defineConfig({
  // The examples (and the user docs) import the package by name.
  resolve: { alias: { 'kstreams-ts': new URL('./src/index.ts', import.meta.url).pathname } },
  test: {
    include: ['test/**/*.test.ts'],
    // Integration tests and the crash suite need a broker: `npm run test:integration` and
    // `npm run test:crash` (vitest.integration.config.ts, vitest.crash.config.ts).
    exclude: ['test/integration/**', 'test/crash/**'],
  },
})

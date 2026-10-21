import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    include: ['test/**/*.test.ts'],
    // Integration tests and the crash suite need a broker: `npm run test:integration` and
    // `npm run test:crash` (vitest.integration.config.ts, vitest.crash.config.ts).
    exclude: ['test/integration/**', 'test/crash/**'],
  },
})

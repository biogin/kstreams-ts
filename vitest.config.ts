import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    include: ['test/**/*.test.ts'],
    // Integration tests need a broker: `npm run test:integration` (vitest.integration.config.ts).
    exclude: ['test/integration/**'],
  },
})

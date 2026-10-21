import { defineConfig } from 'vitest/config'

// Integration tests against a real broker given by KSTREAMS_KAFKA_BROKERS (seam 2).
export default defineConfig({
  test: {
    include: ['test/integration/**/*.test.ts'],
    globalSetup: ['test/integration/global-setup.ts'],
    testTimeout: 60_000,
    hookTimeout: 30_000,
  },
})

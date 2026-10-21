import { defineConfig } from 'vitest/config'

// The crash test suite (ADR-0011) against a real broker given by KSTREAMS_KAFKA_BROKERS: slow and
// randomized, so it runs on its own (nightly-style), not with `npm test` or `test:integration`.
export default defineConfig({
  test: {
    include: ['test/crash/**/*.test.ts'],
    globalSetup: ['test/integration/global-setup.ts'],
    hookTimeout: 60_000,
    // Show each iteration's seed, plan and kills, also when it passes.
    reporters: ['verbose'],
    silent: false,
  },
})

import { defineConfig } from 'vitest/config'

// Integration tests against a real broker given by KSTREAMS_KAFKA_BROKERS (seam 2).
export default defineConfig({
  test: {
    include: ['test/integration/**/*.test.ts'],
    globalSetup: ['test/integration/global-setup.ts'],
    // Worker threads (`workers > 0`) run the TypeScript sources with Node's type stripping, outside
    // Vite: they inherit this preload of the `.js` → `.ts` resolve hook from the test process.
    execArgv: [
      '--import',
      new URL('./test/integration/harness/ts-resolve.ts', import.meta.url).href,
    ],
    testTimeout: 60_000,
    hookTimeout: 30_000,
  },
})

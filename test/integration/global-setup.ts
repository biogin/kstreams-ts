import { BROKERS_ENV, brokers } from './kafka.js'

export default function setup(): void {
  if (brokers.length === 0) {
    console.warn(
      `\n${BROKERS_ENV} is not set: skipping the integration tests.\n` +
        'Start a local broker with `npm run kafka -- start`, then run\n' +
        `  ${BROKERS_ENV}=localhost:9092 npm run test:integration\n`,
    )
  }
}

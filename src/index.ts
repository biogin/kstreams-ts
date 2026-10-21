// Public entry point.
export type { StreamRecord } from './record.js'
export {
  DeserializationError,
  type Serde,
  type SerdeContext,
  type Serdes,
  serdes,
} from './serde.js'
export {
  createStreamsApp,
  type Stream,
  type StreamsApp,
  type StreamsAppConfig,
} from './streams-app.js'
export {
  createTestDriver,
  type PipeOptions,
  type TestDriver,
  type TestInputTopic,
  type TestOutputTopic,
} from './test-driver.js'

// Public entry point.
export {
  deadLetter,
  type ErrorHandler,
  type ErrorHandlerContext,
  type ErrorHandlerKind,
  type ErrorHandlerResponse,
  type ErrorHandlers,
  type FailedRecord,
  type HandlerOutputRecord,
} from './error-handlers.js'
export type {
  AbortReason,
  ApplicationState,
  Metric,
  MetricName,
  MetricsSink,
  RebalanceEvent,
  RestoreEndEvent,
  RestoreProgressEvent,
  RestoreStartEvent,
  StateChangeEvent,
  StreamsAppErrorEvent,
  StreamsAppEventName,
  StreamsAppEvents,
  TopicPartition,
  WaitKind,
} from './observability.js'
export type {
  Processor,
  ProcessorContext,
  ProcessorSupplier,
  Timer,
  TimerCallback,
  TimerType,
} from './processor-graph.js'
export type { StreamRecord } from './record.js'
export {
  DeserializationError,
  type Serde,
  type SerdeContext,
  type Serdes,
  serdes,
} from './serde.js'
export {
  type KeyValueStore,
  type NamedStore,
  type StateStore,
  type StateStoreContext,
  type StateStoreSupplier,
  stores,
} from './state-store.js'
export {
  type CountOptions,
  createStreamsApp,
  type DeliveryGuarantee,
  type GroupedStream,
  type NamedProcessor,
  type StoreOptions,
  type Stream,
  type StreamsApp,
  type StreamsAppConfig,
  type Table,
  type WindowedStream,
  type WorkersConfig,
} from './streams-app.js'
export { AsyncNotAllowedError } from './sync-dsl.js'
export {
  createTestDriver,
  type PipeOptions,
  type TestDriver,
  type TestDriverMetrics,
  type TestDriverOptions,
  type TestInputTopic,
  type TestOutputRecord,
  type TestOutputTopic,
  type TestStore,
} from './test-driver.js'
export {
  type Duration,
  type TumblingOptions,
  type TumblingWindows,
  tumbling,
  type Window,
  type Windowed,
} from './windows.js'

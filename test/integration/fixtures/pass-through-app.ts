// App Module for crash tests: copies `input` to `output` unchanged (at-least-once, workers: 0).

import { createStreamsApp, type StreamsApp, serdes } from '../../../src/index.js'

export interface PassThroughParams {
  applicationId: string
  brokers: string[]
  input: string
  output: string
}

export default function passThroughApp({
  applicationId,
  brokers,
  input,
  output,
}: PassThroughParams): StreamsApp {
  // Short session: a SIGKILLed Worker's partitions move to the restarted one only after it expires.
  const app = createStreamsApp({ applicationId, brokers, sessionTimeoutMs: 6_000 })
  app.stream(input, { key: serdes.string, value: serdes.string }).to(output)
  return app
}

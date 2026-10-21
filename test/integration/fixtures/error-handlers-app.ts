// App Module for worker threads (ADR-0013), for error handlers. It reads JSON values from `input`
// and writes their `value` field to `output`. A value with `"async": true` makes `filter` return a
// Promise (an AsyncNotAllowedError); `"fail": true` makes `map` throw. With `deadLetter` set,
// deserialization and processing errors go to that topic.
//
// The params come from the KSTREAMS_APP_PARAMS environment variable, which worker threads see too.

import { createStreamsApp, type DeliveryGuarantee, deadLetter, serdes } from '../../../src/index.js'

export interface ErrorHandlersAppParams {
  applicationId: string
  brokers: string[]
  input: string
  output: string
  guarantee: DeliveryGuarantee
  workers: number
  deadLetter?: string
}

interface Value {
  value: string
  async?: boolean
  fail?: boolean
}

const params = JSON.parse(process.env.KSTREAMS_APP_PARAMS ?? '{}') as ErrorHandlersAppParams

const app = createStreamsApp({
  applicationId: params.applicationId,
  brokers: params.brokers,
  guarantee: params.guarantee,
  workers: { count: params.workers, module: import.meta.url },
  sessionTimeoutMs: 6_000,
  ...(params.deadLetter === undefined
    ? {}
    : {
        errorHandlers: {
          deserialization: deadLetter(params.deadLetter),
          processing: deadLetter(params.deadLetter),
        },
      }),
})
app
  .stream(params.input, { key: serdes.string, value: serdes.json<Value>() })
  .filter((_key, value) => (value?.async === true ? (Promise.resolve(true) as never) : true))
  .map((key, value) => {
    if (value?.fail === true) throw new Error('Processing failed on purpose')
    return { key, value: value?.value ?? null }
  })
  .to(params.output, { key: serdes.string, value: serdes.string })

export default app

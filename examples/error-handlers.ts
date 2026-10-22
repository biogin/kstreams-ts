import { createStreamsApp, deadLetter, type ErrorHandler, serdes } from 'kstreams-ts'

// Custom handler: skip records that fail with a RangeError, but stop the app for anything else.
const rejectBadAmounts: ErrorHandler = ({ error, record, produce }) => {
  if (!(error instanceof RangeError)) return 'fail'
  produce('payments-rejected', {
    key: record.key,
    value: record.value,
    timestamp: record.timestamp,
  })
  return 'continue'
}

const app = createStreamsApp({
  applicationId: 'payments',
  errorHandlers: {
    deserialization: deadLetter('payments-dlq'),
    processing: rejectBadAmounts,
  },
})

app
  .stream('payments', { key: serdes.string, value: serdes.json<{ cents: number }>() })
  .peek((_id, payment) => {
    if (payment !== null && payment.cents < 0) throw new RangeError('negative amount')
  })
  .to('payments-checked')

export default app

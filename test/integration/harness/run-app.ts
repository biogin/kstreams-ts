// Child-process entry point of the App Process harness (see app-process.ts). It imports the App
// Module named by the parent, builds the Streams App from the parent's params, starts it and tells
// the parent over IPC once it is running. SIGTERM stops it gracefully; SIGKILL comes from outside.

import { inspect } from 'node:util'
import type { StreamsApp } from '../../../src/index.js'
import type { AppProcessMessage } from './app-process.js'

function send(message: AppProcessMessage): void {
  process.send?.(message)
}

const moduleUrl = process.env.KSTREAMS_APP_MODULE
const params: unknown = JSON.parse(process.env.KSTREAMS_APP_PARAMS ?? '{}')
if (moduleUrl === undefined) throw new Error('KSTREAMS_APP_MODULE is not set')

// The default export builds the app from the params, or is the app (an App Module for worker
// threads, which reads the params from KSTREAMS_APP_PARAMS itself).
const appModule = (await import(moduleUrl)) as {
  default: StreamsApp | ((params: unknown) => StreamsApp)
}
const app = typeof appModule.default === 'function' ? appModule.default(params) : appModule.default

async function stopAndExit(): Promise<never> {
  try {
    await app.stop()
    process.exit(0)
  } catch (error) {
    console.error(inspect(error, { depth: 8 }))
    process.exit(1)
  }
}

process.once('SIGTERM', () => void stopAndExit())
// The parent went away without killing us: don't linger as an orphan group member.
process.once('disconnect', () => void stopAndExit())

try {
  await app.start()
} catch (error) {
  console.error(inspect(error, { depth: 8 }))
  process.exit(1)
}
send({ type: 'running' })

// No events yet (ticket 25): poll for a fatal error and exit with it.
const watchdog = setInterval(() => {
  if (app.state === 'error') {
    clearInterval(watchdog)
    void stopAndExit()
  }
}, 100)

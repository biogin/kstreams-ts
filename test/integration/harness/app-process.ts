// App Process harness for crash tests (seam 2, ADR-0011): runs a Streams App in a child process so
// a test can SIGKILL it from outside at any point, with no fault-injection hooks in the engine.
//
// The app comes from an App Module: a fixture whose default export builds the Streams App from
// JSON-serializable params, without side effects (it must not call `start`):
//
//   export default function (params: MyParams): StreamsApp { ... }
//
// An app with worker threads must instead export the app itself (ADR-0013); it reads its params
// from the KSTREAMS_APP_PARAMS environment variable (see fixtures/workers-count-app.ts). Worker
// threads inherit the child's `--import` of ts-resolve.ts, so they can run the sources too.
//
// Give the app a short `sessionTimeoutMs` (the broker's minimum is 6 s): a SIGKILLed Worker never
// leaves its consumer group, so its partitions move to the restarted one only after its session
// times out. Use a unique Application ID and topic names per test (the broker is shared).

import { type ChildProcess, fork } from 'node:child_process'
import { fileURLToPath } from 'node:url'

/** Messages the child sends to the parent over IPC. */
export type AppProcessMessage = { type: 'running' }

export interface AppProcessExit {
  code: number | null
  signal: NodeJS.Signals | null
}

/** A Streams App running in a child process. */
export interface AppProcess {
  readonly pid: number
  /** Resolves when the child process exits, for whatever reason. */
  readonly exited: Promise<AppProcessExit>
  /** Everything the child wrote to stdout and stderr so far, for failure messages. */
  readonly log: () => string
  /** Kills the child with SIGKILL (no graceful stop) and waits for it to exit. Idempotent. */
  kill(): Promise<AppProcessExit>
  /** Sends SIGTERM, which stops the Streams App gracefully, and waits for the child to exit. */
  stop(): Promise<AppProcessExit>
}

export interface LaunchOptions {
  /** How long to wait for the app to reach `running`. Default 30 s. */
  timeoutMs?: number
}

const runApp = fileURLToPath(new URL('./run-app.ts', import.meta.url))
const tsResolve = new URL('./ts-resolve.ts', import.meta.url).href

/**
 * Starts the Streams App built by `appModule`'s default export from `params` in a child process.
 * Resolves once the app is running (has joined its consumer group). Rejects, with the child's
 * output, if the child exits or times out first.
 */
export async function launchAppModule<P>(
  appModule: URL,
  params: P,
  { timeoutMs = 30_000 }: LaunchOptions = {},
): Promise<AppProcess> {
  const child = fork(runApp, [], {
    execArgv: ['--import', tsResolve],
    env: {
      ...process.env,
      KSTREAMS_APP_MODULE: appModule.href,
      KSTREAMS_APP_PARAMS: JSON.stringify(params),
    },
    stdio: ['ignore', 'pipe', 'pipe', 'ipc'],
  })
  let output = ''
  child.stdout?.on('data', (chunk: Buffer) => {
    output += chunk.toString()
  })
  child.stderr?.on('data', (chunk: Buffer) => {
    output += chunk.toString()
  })
  const exited = new Promise<AppProcessExit>((resolve) => {
    child.once('exit', (code, signal) => resolve({ code, signal }))
  })

  const app = appProcess(child, exited, () => output)
  try {
    await waitUntilRunning(child, exited, timeoutMs)
  } catch (error) {
    await app.kill()
    throw new Error(`${(error as Error).message}. Child output:\n${output}`)
  }
  return app
}

function appProcess(
  child: ChildProcess,
  exited: Promise<AppProcessExit>,
  log: () => string,
): AppProcess {
  const signal = async (name: NodeJS.Signals) => {
    if (child.exitCode === null && child.signalCode === null) child.kill(name)
    return exited
  }
  return {
    pid: child.pid ?? -1,
    exited,
    log,
    kill: () => signal('SIGKILL'),
    stop: () => signal('SIGTERM'),
  }
}

function waitUntilRunning(
  child: ChildProcess,
  exited: Promise<AppProcessExit>,
  timeoutMs: number,
): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    const timeout = setTimeout(
      () => reject(new Error(`App Process not running after ${timeoutMs} ms`)),
      timeoutMs,
    )
    child.on('message', (message: AppProcessMessage) => {
      if (message.type === 'running') {
        clearTimeout(timeout)
        resolve()
      }
    })
    void exited.then(({ code, signal }) => {
      clearTimeout(timeout)
      reject(new Error(`App Process exited before running (code ${code}, signal ${signal})`))
    })
  })
}

/** A random integer in [min, max), for killing at a random point. */
export function randomBetween(min: number, max: number): number {
  return min + Math.floor(Math.random() * (max - min))
}

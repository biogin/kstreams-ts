import { type ChildProcessWithoutNullStreams, spawn } from 'node:child_process'
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { afterEach, describe, expect, it } from 'vitest'
import {
  claimProcessIdentity,
  lockTask,
  removeTaskLocks,
  taskLockHolder,
  unlockTask,
} from '../src/state-directory.js'

const directories: string[] = []
const children: ChildProcessWithoutNullStreams[] = []

afterEach(() => {
  for (const child of children.splice(0)) child.kill('SIGKILL')
  for (const directory of directories.splice(0)) rmSync(directory, { recursive: true })
})

function stateDir(): string {
  const directory = mkdtempSync(join(tmpdir(), 'kstreams-state-dir-'))
  directories.push(directory)
  return directory
}

const tsResolve = new URL('./integration/harness/ts-resolve.ts', import.meta.url).href
const stateDirectoryModule = new URL('../src/state-directory.ts', import.meta.url).href

/**
 * Another process that claims `dir` for `app` as an App Process starting on it does, and holds it
 * until killed. Resolves to its App Process ID, or rejects with its error.
 */
async function claimInAnotherProcess(dir: string): Promise<{ id: string; kill(): Promise<void> }> {
  const child = spawn(
    process.execPath,
    [
      '--import',
      tsResolve,
      '--input-type=module',
      '-e',
      `const { claimProcessIdentity } = await import(${JSON.stringify(stateDirectoryModule)})
      try {
        process.stdout.write('id:' + claimProcessIdentity(${JSON.stringify(dir)}, 'app').id + '\\n')
      } catch (error) {
        process.stdout.write('error:' + error.message + '\\n')
        process.exit(1)
      }
      setInterval(() => {}, 1_000)`,
    ],
    { cwd: fileURLToPath(new URL('..', import.meta.url)) },
  )
  children.push(child)
  let output = ''
  child.stderr.on('data', (chunk: Buffer) => {
    output += chunk.toString()
  })
  const line = await new Promise<string>((resolve, reject) => {
    child.stdout.on('data', (chunk: Buffer) => {
      output += chunk.toString()
      if (output.includes('\n')) resolve(output.slice(0, output.indexOf('\n')))
    })
    child.once('exit', () => reject(new Error(`exited before claiming: ${output}`)))
  })
  if (line.startsWith('error:')) throw new Error(line.slice('error:'.length))
  return {
    id: line.slice('id:'.length),
    async kill() {
      const exited = new Promise((resolve) => child.once('exit', resolve))
      child.kill('SIGKILL')
      await exited
    },
  }
}

describe('the state directory', () => {
  it('is refused to a second process while the first lives, and free once it was killed', async () => {
    const dir = stateDir()
    const first = await claimInAnotherProcess(dir)
    await expect(claimInAnotherProcess(dir)).rejects.toThrow(
      `stateDir ${dir} is in use by another process; give each process its own stateDir`,
    )
    expect(() => claimProcessIdentity(dir, 'app')).toThrow(/is in use by another process/)

    await first.kill()
    // The OS released its lock: the next process starts on the directory, and takes its ID.
    const restarted = await claimInAnotherProcess(dir)
    expect(restarted.id).toBe(first.id)
  }, 30_000)

  it('is refused to a second Streams App of this process while the first has it', () => {
    const dir = stateDir()
    const first = claimProcessIdentity(dir, 'app')
    expect(() => claimProcessIdentity(dir, 'other-app')).toThrow(/is in use by another Streams App/)
    first.release()
    claimProcessIdentity(dir, 'other-app').release()
  })
})

describe('the App Process ID', () => {
  it('is kept in the state directory: the same after a restart', () => {
    const dir = stateDir()
    const first = claimProcessIdentity(dir, 'app')
    expect(first.id).toMatch(/^[0-9a-f]{12}$/)
    first.release()
    const restarted = claimProcessIdentity(dir, 'app')
    expect(restarted.id).toBe(first.id)
    restarted.release()
  })

  it('is new on every start without a state directory', () => {
    expect(claimProcessIdentity(undefined, 'app').id).not.toBe(
      claimProcessIdentity(undefined, 'app').id,
    )
  })
})

describe('Task locks', () => {
  it('make the next owner wait until the Task is unlocked', async () => {
    const dir = stateDir()
    const a = taskLockHolder('p1', 0)
    const b = taskLockHolder('p1', 1)
    expect(await lockTask(dir, 'app', 3, a, () => true)).toBe(true)
    // b gives up at once while a holds it.
    expect(await lockTask(dir, 'app', 3, b, () => true)).toBe(false)
    const waiting = lockTask(dir, 'app', 3, b, () => false)
    unlockTask(dir, 'app', 3, a)
    expect(await waiting).toBe(true)
    // a's unlock no longer removes b's lock.
    unlockTask(dir, 'app', 3, a)
    expect(await lockTask(dir, 'app', 3, a, () => true)).toBe(false)
    // b holds it already.
    expect(await lockTask(dir, 'app', 3, b, () => true)).toBe(true)
  })

  it('left by a process that is gone are removed when the next one starts', async () => {
    const dir = stateDir()
    const task = join(dir, 'app', '0')
    mkdirSync(task, { recursive: true })
    writeFileSync(join(task, '.lock'), 'old/worker-0')
    const identity = claimProcessIdentity(dir, 'app')
    expect(await lockTask(dir, 'app', 0, taskLockHolder(identity.id, 0), () => true)).toBe(true)
    identity.release()
  })

  it('are all removed once the Workers of the App Process stopped', async () => {
    const dir = stateDir()
    await lockTask(dir, 'app', 0, taskLockHolder('p1', 0), () => true)
    await lockTask(dir, 'app', 1, taskLockHolder('p1', 1), () => true)
    removeTaskLocks(join(dir, 'app'))
    expect(await lockTask(dir, 'app', 0, taskLockHolder('p1', 1), () => true)).toBe(true)
    expect(await lockTask(dir, 'app', 1, taskLockHolder('p1', 0), () => true)).toBe(true)
  })
})

import { spawnSync } from 'node:child_process'
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, describe, expect, it } from 'vitest'
import {
  claimProcessIdentity,
  lockTask,
  taskLockHolder,
  unlockTask,
  unlockTasksOf,
} from '../src/state-directory.js'

const directories: string[] = []

afterEach(() => {
  for (const directory of directories.splice(0)) rmSync(directory, { recursive: true })
})

function stateDir(): string {
  const directory = mkdtempSync(join(tmpdir(), 'kstreams-state-dir-'))
  directories.push(directory)
  return directory
}

/** The PID of a process that has exited. */
function deadPid(): number {
  const { pid } = spawnSync(process.execPath, ['-e', ''])
  if (pid === undefined) throw new Error('no child')
  return pid
}

describe('the App Process ID', () => {
  it('is the same after a restart, and different for a second process meanwhile', () => {
    const dir = stateDir()
    const first = claimProcessIdentity(dir, 'app')
    expect(first.id).toMatch(/^[0-9a-f]{12}$/)
    // A second process with the same state directory, while the first one runs.
    const second = claimProcessIdentity(dir, 'app')
    expect(second.id).not.toBe(first.id)
    first.release()
    second.release()
    // Restarted: the first free ID again.
    const restarted = claimProcessIdentity(dir, 'app')
    expect([first.id, second.id].sort()[0]).toBe(restarted.id)
    restarted.release()
  })

  it('is taken over from a process that is gone', () => {
    const dir = stateDir()
    const crashed = claimProcessIdentity(dir, 'app')
    const lock = join(dir, 'app', 'processes', `${crashed.id}.lock`)
    const content = JSON.parse(readFileSync(lock, 'utf8')) as { pid: number }
    writeFileSync(lock, JSON.stringify({ ...content, pid: deadPid() }))
    expect(claimProcessIdentity(dir, 'app').id).toBe(crashed.id)
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
  })

  it('are taken over from a process that is gone', async () => {
    const dir = stateDir()
    const task = join(dir, 'app', '0')
    mkdirSync(task, { recursive: true })
    writeFileSync(
      join(task, '.lock'),
      JSON.stringify({ holder: 'old/worker-0', pid: deadPid(), booted: 0 }),
    )
    expect(await lockTask(dir, 'app', 0, taskLockHolder('new', 0), () => true)).toBe(true)
  })

  it('of one App Process are all removed once its Workers stopped', async () => {
    const dir = stateDir()
    await lockTask(dir, 'app', 0, taskLockHolder('p1', 0), () => true)
    await lockTask(dir, 'app', 1, taskLockHolder('p1', 1), () => true)
    await lockTask(dir, 'app', 2, taskLockHolder('p2', 0), () => true)
    unlockTasksOf(dir, 'app', 'p1')
    expect(await lockTask(dir, 'app', 0, taskLockHolder('p3', 0), () => true)).toBe(true)
    expect(await lockTask(dir, 'app', 1, taskLockHolder('p3', 0), () => true)).toBe(true)
    expect(await lockTask(dir, 'app', 2, taskLockHolder('p3', 0), () => true)).toBe(false)
  })
})

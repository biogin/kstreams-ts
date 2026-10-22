import { spawnSync } from 'node:child_process'
import {
  linkSync,
  mkdirSync,
  mkdtempSync,
  readdirSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from 'node:fs'
import { tmpdir } from 'node:os'
import { dirname, join } from 'node:path'
import { afterEach, describe, expect, it } from 'vitest'
import {
  claimProcessIdentity,
  type LockFiles,
  lockTask,
  taskLockHolder,
  tryLock,
  unlock,
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

describe('taking over a stale lock', () => {
  /** Real file operations; `preempt` runs before each one. */
  function hooked(preempt: () => void): LockFiles {
    return {
      write(path, content) {
        preempt()
        writeFileSync(path, content)
      },
      link(existing, path) {
        preempt()
        linkSync(existing, path)
      },
      read(path) {
        preempt()
        try {
          return readFileSync(path, 'utf8')
        } catch (error) {
          if ((error as NodeJS.ErrnoException).code === 'ENOENT') return undefined
          throw error
        }
      },
      remove(path) {
        preempt()
        rmSync(path, { force: true })
      },
    }
  }

  /** A stale lock: its process is gone. */
  function staleLock(dead: number): string {
    const lock = join(stateDir(), '.lock')
    writeFileSync(lock, JSON.stringify({ holder: 'old/worker-0', pid: dead, booted: 0 }))
    return lock
  }

  /**
   * What is wrong with the lock `lock` once the contenders are done, `holding` saying which of them
   * believe they hold it: two owners, a lock that doesn't name its owner, or, with no owner, a lock
   * that nobody can take.
   */
  function problem(lock: string, holding: Map<string, boolean>): string | undefined {
    const owners = [...holding].filter(([, held]) => held).map(([name]) => name)
    let named: string | undefined
    try {
      named = (JSON.parse(readFileSync(lock, 'utf8')) as { holder: string }).holder
    } catch {
      named = undefined
    }
    if (owners.length > 1) return `${owners.join(' and ')} all hold the lock`
    if (owners.length === 1 && named !== owners[0]) {
      return `${owners[0]} holds it, but the lock names ${named}`
    }
    if (owners.length === 0 && !tryLock(lock, 'W'))
      return `nobody holds it, nor can take it (${named})`
    return undefined
  }

  it('keeps one owner whatever other contenders do between the steps of a takeover', () => {
    const dead = deadPid()
    const failures: string[] = []
    let schedules = 0
    // Contender Y takes a stale lock over. Before Y's file operation number `i`, X takes the lock
    // (and maybe releases it); before operation number `j`, so does Z.
    for (let i = 0; ; i++) {
      let reached = false
      for (const xReleases of [false, true]) {
        for (const zReleases of [false, true]) {
          for (let j = i; ; j++) {
            const lock = staleLock(dead)
            const holding = new Map<string, boolean>()
            const contend = (name: string, releases: boolean) => {
              const held = tryLock(lock, name)
              if (held && releases) unlock(lock, name)
              holding.set(name, held && !releases)
            }
            let step = 0
            let reachedJ = false
            const files = hooked(() => {
              if (step === i) {
                reached = true
                contend('X', xReleases)
              }
              if (step === j) {
                reachedJ = true
                contend('Z', zReleases)
              }
              step++
            })
            holding.set('Y', tryLock(lock, 'Y', false, files))
            if (!reachedJ) break
            schedules++
            const found = problem(lock, holding)
            if (found !== undefined) {
              failures.push(
                `X ${xReleases ? 'takes and releases' : 'takes'} at ${i}, ` +
                  `Z ${zReleases ? 'takes and releases' : 'takes'} at ${j}: ${found}`,
              )
            }
          }
        }
      }
      if (!reached) break
    }
    expect(failures).toEqual([])
    expect(schedules).toBeGreaterThan(100)
  })

  it('is taken over after a contender died at any step of its takeover', () => {
    const dead = deadPid()
    const failures: string[] = []
    for (let k = 0; ; k++) {
      const lock = staleLock(dead)
      let step = 0
      const crash = new Error('crashed')
      const files = hooked(() => {
        if (step++ === k) throw crash
      })
      let crashed = false
      try {
        tryLock(lock, 'C', false, files)
      } catch (error) {
        if (error !== crash) throw error
        crashed = true
      }
      if (!crashed) break
      // C's process is gone: every lock it left (the lock, or a claim) is stale.
      const directory = dirname(lock)
      for (const name of readdirSync(directory)) {
        const path = join(directory, name)
        const content = readFileSync(path, 'utf8')
        if (content.includes('"holder":"C"')) {
          writeFileSync(path, content.replace(`"pid":${process.pid}`, `"pid":${dead}`))
        }
      }
      const holding = new Map([['Y', tryLock(lock, 'Y')]])
      if (holding.get('Y') !== true) failures.push(`C crashed at ${k}: Y could not take the lock`)
      const found = problem(lock, holding)
      if (found !== undefined) failures.push(`C crashed at ${k}: ${found}`)
    }
    expect(failures).toEqual([])
  })
})

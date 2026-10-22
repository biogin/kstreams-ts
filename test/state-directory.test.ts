import { spawn, spawnSync } from 'node:child_process'
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
  adoptProcessIncarnation,
  claimProcessIdentity,
  type LockFiles,
  livenessIn,
  lockTask,
  processIncarnation,
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

  it('is taken back, with its Task locks, by a restart that got the same PID', async () => {
    const dir = stateDir()
    const current = processIncarnation()
    let crashed: string
    try {
      // An earlier process with this same PID (a container's restarted process typically has
      // it): it dies holding its ID and a Task lock.
      adoptProcessIncarnation('earlier')
      crashed = claimProcessIdentity(dir, 'app').id
      expect(await lockTask(dir, 'app', 0, taskLockHolder(crashed, 0), () => true)).toBe(true)
    } finally {
      adoptProcessIncarnation(current)
    }
    const restarted = claimProcessIdentity(dir, 'app')
    expect(restarted.id).toBe(crashed)
    expect(await lockTask(dir, 'app', 0, taskLockHolder(restarted.id, 0), () => true)).toBe(true)
    // While this process holds them, a second claim of this process doesn't take them.
    expect(claimProcessIdentity(dir, 'app').id).not.toBe(crashed)
    expect(await lockTask(dir, 'app', 0, taskLockHolder('other', 0), () => true)).toBe(false)
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

  it('of a live process are not taken over after a wall-clock step, given boot IDs', () => {
    const dir = stateDir()
    const lock = join(dir, '.lock')
    // Taken by a live process (our parent) in this boot, before the clock stepped by 200 s.
    const live = { holder: 'other/worker-0', pid: process.ppid, booted: 1_000, bootId: 'boot-1' }
    writeFileSync(lock, JSON.stringify(live))
    const afterStep = { bootId: 'boot-1', booted: 1_200 }
    expect(tryLock(lock, 'new/worker-0', false, undefined, afterStep)).toBe(false)
    // Another boot ID is a reboot since: the lock is stale, whatever the clock says.
    expect(
      tryLock(lock, 'new/worker-0', false, undefined, { bootId: 'boot-2', booted: 1_000 }),
    ).toBe(true)
  })

  it('without boot IDs, tell a reboot by the boot time', () => {
    const dir = stateDir()
    const lock = join(dir, '.lock')
    writeFileSync(
      lock,
      JSON.stringify({ holder: 'other/worker-0', pid: process.ppid, booted: 1_000 }),
    )
    expect(tryLock(lock, 'new/worker-0', false, undefined, { booted: 1_060 })).toBe(false)
    expect(tryLock(lock, 'new/worker-0', false, undefined, { booted: 1_200 })).toBe(true)
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

describe('locks across PID namespaces (containers sharing a state directory)', () => {
  /** This host, in PID namespace `pidNamespace`. */
  const host = (pidNamespace: string) => ({ bootId: 'boot-1', booted: 1_000, pidNamespace })

  /**
   * A lock of a process in PID namespace `pidNamespace` that has this very PID, as the main
   * process of another container typically has (PID 1), with liveness token `alive`.
   */
  function lockOf(pidNamespace: string, alive?: string): string {
    const lock = join(stateDir(), '.lock')
    const holder = { holder: 'other/worker-0', pid: process.pid, booted: 1_000, bootId: 'boot-1' }
    writeFileSync(
      lock,
      JSON.stringify({ ...holder, incarnation: 'other', pidNamespace, ...(alive && { alive }) }),
    )
    return lock
  }

  const liveness = (alive: boolean) => ({
    token: () => 'mine',
    alive(token: string) {
      expect(token).toBe('other-token')
      return alive
    },
  })

  it('are not taken over while their process lives, though it has this PID', () => {
    const lock = lockOf('pid:[2]', 'other-token')
    expect(tryLock(lock, 'new/worker-0', false, undefined, host('pid:[1]'), liveness(true))).toBe(
      false,
    )
  })

  it('are taken over once their process is gone, e.g. by the restarted container', () => {
    const lock = lockOf('pid:[2]', 'other-token')
    expect(tryLock(lock, 'new/worker-0', false, undefined, host('pid:[1]'), liveness(false))).toBe(
      true,
    )
  })

  it('without a liveness token are kept: their PID tells nothing in this namespace', () => {
    const lock = lockOf('pid:[2]')
    expect(tryLock(lock, 'new/worker-0', false, undefined, host('pid:[1]'), liveness(false))).toBe(
      false,
    )
  })

  it('in this PID namespace, are stale when this PID is another incarnation', () => {
    const lock = lockOf('pid:[1]', 'other-token')
    const unused = {
      token: () => 'mine',
      alive(): boolean {
        throw new Error('the PID tells in one namespace')
      },
    }
    expect(tryLock(lock, 'new/worker-0', false, undefined, host('pid:[1]'), unused)).toBe(true)
  })

  it('record the PID namespace and liveness token of their process', () => {
    const dir = stateDir()
    const lock = join(dir, '.lock')
    expect(tryLock(lock, 'me', false, undefined, host('pid:[1]'), liveness(true))).toBe(true)
    expect(JSON.parse(readFileSync(lock, 'utf8'))).toMatchObject({
      pidNamespace: 'pid:[1]',
      alive: 'mine',
    })
  })
})

describe('liveness files', () => {
  it('tell whether the process holding one runs, by its SQLite lock', async () => {
    const dir = stateDir()
    // Another process holds a liveness file the way an App Process holds its own.
    const child = spawn(
      process.execPath,
      [
        '-e',
        `const db = new (require('better-sqlite3'))(${JSON.stringify(join(dir, 'other.alive'))})
        db.pragma('locking_mode = EXCLUSIVE')
        db.exec('BEGIN EXCLUSIVE; COMMIT')
        process.stdout.write('held')
        process.stdin.on('end', () => process.exit(0)).resume()`,
      ],
      { cwd: process.cwd(), stdio: ['pipe', 'pipe', 'inherit'] },
    )
    await new Promise((resolve) => child.stdout.once('data', resolve))
    const liveness = livenessIn(dir)
    expect(liveness.alive('other')).toBe(true)
    const exited = new Promise((resolve) => child.once('exit', resolve))
    child.stdin.end()
    await exited
    expect(liveness.alive('other')).toBe(false)
    expect(liveness.alive('missing')).toBe(false)
    expect(liveness.alive('../escape')).toBe(false)
  })

  it('are held by an App Process while it has its ID, and recorded in its locks', async () => {
    const dir = stateDir()
    const identity = claimProcessIdentity(dir, 'app')
    const processes = join(dir, 'app', 'processes')
    expect(readdirSync(processes)).toContain(`${processIncarnation()}.alive`)
    expect(await lockTask(dir, 'app', 0, taskLockHolder(identity.id, 0), () => true)).toBe(true)
    expect(JSON.parse(readFileSync(join(dir, 'app', '0', '.lock'), 'utf8'))).toMatchObject({
      alive: processIncarnation(),
    })
    unlockTask(dir, 'app', 0, taskLockHolder(identity.id, 0))
    identity.release()
    expect(readdirSync(processes)).not.toContain(`${processIncarnation()}.alive`)
  })
})

// Internal: what an App Process keeps in its state directory besides State Stores — its identity,
// and a lock per Task directory.
//
// App Process ID. Transactional IDs must be unique among the live producers of an application,
// and should stay the same when a process restarts, so its new producers fence the old ones and
// the broker aborts their open transactions at once. So each App Process has an ID, folded into
// its Workers' transactional IDs (`<applicationId>-<processId>-worker-<n>`):
// - With a state directory, IDs are kept in `<stateDir>/<applicationId>/processes/`: one empty
//   file per ID, and a lock file `<id>.lock` while a process uses it. A starting process takes
//   the first ID nobody holds, or creates a new random one. A restarted process thus takes its old
//   ID again. Two processes that share a state directory (on one host) get different IDs, so they
//   don't fence each other; their Tasks' directories are locked (below).
// - Without a state directory, a process gets a new random ID on every start. A crashed process's
//   open transaction is then only aborted by the broker when it times out (the commit interval
//   plus 10 s), and the new owners of its Tasks wait for that.
//
// Task locks. Workers of one process, and processes that share a state directory, share Task
// directories. A Task moving between them must be closed by its old owner before its new owner
// opens its State Stores and Checkpoint, so its directory has a lock file, `.lock`, while a Worker
// has the Task open; the new owner waits for it.
//
// A lock file names its holder and the process (PID, and when the host booted). A lock is stale,
// and taken over, when that process is gone: its PID isn't running, or the host has rebooted
// since. A lock of this same process is never stale: the Streams App removes the Task locks of its
// Workers when they have all stopped, even those of a worker thread that was terminated. Taking
// over a stale lock can race with another process taking it over at the same moment; the loser
// notices, except in a tiny window where both believe they hold it.

import { randomBytes } from 'node:crypto'
import {
  linkSync,
  mkdirSync,
  readdirSync,
  readFileSync,
  renameSync,
  rmSync,
  writeFileSync,
} from 'node:fs'
import { uptime } from 'node:os'
import { join } from 'node:path'
import { taskDirectory } from './state-store.js'

/** The ID of this App Process, while it holds it. */
export interface ProcessIdentity {
  readonly id: string
  /** Gives the ID up, for the next process that starts with this state directory. */
  release(): void
}

const PROCESS_ID = /^[0-9a-f]{12}$/
const LOCK_FILE = '.lock'
const LOCK_POLL_MS = 50

/** When this host booted, in seconds since the epoch (to a few seconds). */
const BOOTED = Math.round(Date.now() / 1000 - uptime())
/** How far two readings of the boot time may differ for the same boot. */
const BOOT_TOLERANCE_S = 120

/** Takes an App Process ID for `applicationId`, kept in `stateDir` if there is one. */
export function claimProcessIdentity(
  stateDir: string | undefined,
  applicationId: string,
): ProcessIdentity {
  if (stateDir === undefined) return { id: newProcessId(), release() {} }
  const directory = join(stateDir, applicationId, 'processes')
  mkdirSync(directory, { recursive: true })
  const known = readdirSync(directory)
    .filter((name) => PROCESS_ID.test(name))
    .sort()
  for (;;) {
    const id = known.shift() ?? newProcessId()
    writeFileSync(join(directory, id), '', { flag: 'a' })
    const lock = join(directory, `${id}.lock`)
    if (tryLock(lock, id)) return { id, release: () => unlock(lock, id) }
  }
}

function newProcessId(): string {
  return randomBytes(6).toString('hex')
}

/** The holder name of a Task lock taken by Worker `workerNumber` of App Process `processId`. */
export function taskLockHolder(processId: string, workerNumber: number): string {
  return `${processId}/worker-${workerNumber}`
}

/**
 * Locks the directory of `task` for `holder`, waiting while another Worker holds it. Resolves to
 * false, without the lock, once `giveUp()` returns true.
 */
export async function lockTask(
  stateDir: string,
  applicationId: string,
  task: number,
  holder: string,
  giveUp: () => boolean,
): Promise<boolean> {
  const directory = taskDirectory(stateDir, applicationId, task)
  mkdirSync(directory, { recursive: true })
  const lock = join(directory, LOCK_FILE)
  while (!tryLock(lock, holder, true)) {
    if (giveUp()) return false
    await new Promise((resolve) => setTimeout(resolve, LOCK_POLL_MS))
  }
  return true
}

/** Unlocks the directory of `task`, if `holder` holds its lock. */
export function unlockTask(
  stateDir: string,
  applicationId: string,
  task: number,
  holder: string,
): void {
  unlock(join(taskDirectory(stateDir, applicationId, task), LOCK_FILE), holder)
}

/** Removes every Task lock that a Worker of App Process `processId` (this process) holds. */
export function unlockTasksOf(stateDir: string, applicationId: string, processId: string): void {
  let tasks: string[]
  try {
    tasks = readdirSync(join(stateDir, applicationId)).filter((name) => /^\d+$/.test(name))
  } catch {
    return
  }
  for (const task of tasks) {
    const lock = join(taskDirectory(stateDir, applicationId, Number(task)), LOCK_FILE)
    const holder = readLock(lock)
    if (holder?.pid === process.pid && holder.holder.startsWith(`${processId}/`)) {
      rmSync(lock, { force: true })
    }
  }
}

interface LockHolder {
  holder: string
  pid: number
  booted: number
}

/**
 * Takes the lock `file` for `holder` unless someone else holds it. If `holder` holds it already,
 * returns true when `reentrant`, false otherwise.
 */
export function tryLock(file: string, holder: string, reentrant = false): boolean {
  const mine: LockHolder = { holder, pid: process.pid, booted: BOOTED }
  for (;;) {
    // Written in full under another name first, so nobody ever reads a half-written lock.
    const draft = `${file}.${randomBytes(4).toString('hex')}.tmp`
    writeFileSync(draft, JSON.stringify(mine))
    try {
      linkSync(draft, file)
      return true
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== 'EEXIST') throw error
    } finally {
      rmSync(draft, { force: true })
    }
    const content = readContent(file)
    if (content === undefined) continue // just removed
    const current = parseLock(content)
    if (current !== undefined && current.pid === process.pid && current.holder === holder) {
      return reentrant
    }
    if (current !== undefined && !isStale(current)) return false
    // Stale: move it away, and make sure it was the stale lock that moved.
    const moved = `${file}.${randomBytes(4).toString('hex')}.stale`
    try {
      renameSync(file, moved)
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === 'ENOENT') continue
      throw error
    }
    if (readContent(moved) !== content) {
      // Someone took it over meanwhile: put their lock back.
      try {
        linkSync(moved, file)
      } catch {
        // They hold another one by now.
      }
    }
    rmSync(moved, { force: true })
  }
}

/** Removes the lock `file` if `holder` (in this process) holds it. */
function unlock(file: string, holder: string): void {
  const current = readLock(file)
  if (current?.pid === process.pid && current.holder === holder) rmSync(file, { force: true })
}

function readLock(file: string): LockHolder | undefined {
  const content = readContent(file)
  return content === undefined ? undefined : parseLock(content)
}

function readContent(file: string): string | undefined {
  try {
    return readFileSync(file, 'utf8')
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') return undefined
    throw error
  }
}

function parseLock(content: string): LockHolder | undefined {
  try {
    const parsed = JSON.parse(content) as Partial<LockHolder>
    if (
      typeof parsed.holder === 'string' &&
      typeof parsed.pid === 'number' &&
      typeof parsed.booted === 'number'
    ) {
      return { holder: parsed.holder, pid: parsed.pid, booted: parsed.booted }
    }
  } catch {
    // Unreadable: stale.
  }
  return undefined
}

/** Whether the process that took `lock` is gone. */
function isStale(lock: LockHolder): boolean {
  if (lock.pid === process.pid) return false
  if (Math.abs(lock.booted - BOOTED) > BOOT_TOLERANCE_S) return true
  try {
    process.kill(lock.pid, 0)
    return false
  } catch (error) {
    // EPERM: it runs, as another user.
    return (error as NodeJS.ErrnoException).code === 'ESRCH'
  }
}

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
// A lock file names its holder and the process (PID, when the host booted, and the process's
// incarnation: a random value of this process, shared by its worker threads). A lock is stale,
// and taken over, when that process is gone: its PID isn't running, or the host has rebooted
// since, or the PID is this process's but the incarnation isn't (an earlier process that had the
// same PID, as a container's restarted process typically has). A lock of this same process is
// never stale: the Streams App removes the Task locks of its Workers when they have all stopped,
// even those of a worker thread that was terminated.
//
// Taking over a stale lock keeps it exclusive throughout, however many contenders race: a lock is
// only ever created where none is (a hard link, which fails if the name exists), and removed by
// its holder or, once stale, by whoever holds its claim: a lock file named after the stale lock's
// content (`.lock.<digest>.claim`), taken the same way. Each lock's content is unique (it has a
// random nonce), so a contender that saw a lock go stale and claims it late finds another lock in
// its place, and leaves it be. A claim whose holder died is stale too, and taken over likewise.

import { createHash, randomBytes } from 'node:crypto'
import { linkSync, mkdirSync, readdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
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

/** This process's incarnation (see above); its worker threads adopt the main thread's. */
let incarnation = randomBytes(8).toString('hex')

/** This process's incarnation, for its worker threads to adopt. */
export function processIncarnation(): string {
  return incarnation
}

/** Makes this worker thread's locks those of the process whose incarnation is `value`. */
export function adoptProcessIncarnation(value: string): void {
  incarnation = value
}

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
    const directory = taskDirectory(stateDir, applicationId, Number(task))
    let names: string[]
    try {
      names = readdirSync(directory)
    } catch {
      continue
    }
    // The Task's lock, and claims of stale ones that a terminated worker thread left behind.
    const locks = names.filter(
      (name) =>
        name === LOCK_FILE || (name.startsWith(`${LOCK_FILE}.`) && name.endsWith(CLAIM_SUFFIX)),
    )
    for (const name of locks) {
      const lock = join(directory, name)
      const holder = readLock(lock)
      if (holder !== undefined && isMine(holder) && holder.holder.startsWith(`${processId}/`)) {
        rmSync(lock, { force: true })
      }
    }
  }
}

interface LockHolder {
  holder: string
  pid: number
  booted: number
  incarnation?: string
}

const CLAIM_SUFFIX = '.claim'

/**
 * Takes the lock `file` for `holder` unless someone else holds it. If `holder` holds it already,
 * returns true when `reentrant`, false otherwise.
 */
export function tryLock(
  file: string,
  holder: string,
  reentrant = false,
  files: LockFiles = lockFiles,
): boolean {
  for (;;) {
    // Written in full under another name first, so nobody ever reads a half-written lock.
    const mine = {
      holder,
      pid: process.pid,
      booted: BOOTED,
      incarnation,
      nonce: randomBytes(8).toString('hex'),
    }
    const draft = `${file}.${randomBytes(4).toString('hex')}.tmp`
    files.write(draft, JSON.stringify(mine))
    try {
      files.link(draft, file)
      return true
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== 'EEXIST') throw error
    } finally {
      files.remove(draft)
    }
    const content = files.read(file)
    if (content === undefined) continue // just removed
    const current = parseLock(content)
    if (current !== undefined && isMine(current) && current.holder === holder) {
      return reentrant
    }
    if (current !== undefined && !isStale(current)) return false
    // Stale: only the holder of its claim removes it, and only while it is still this very lock.
    const claim = `${file}.${createHash('sha256').update(content).digest('hex').slice(0, 16)}${CLAIM_SUFFIX}`
    // Reentrant: a claim of this very holder can only be left over from a terminated worker thread.
    if (!tryLock(claim, holder, true, files)) return false // another contender takes it over
    try {
      if (files.read(file) === content) files.remove(file)
    } finally {
      unlock(claim, holder, files)
    }
  }
}

/** Removes the lock `file` if `holder` (in this process) holds it. */
export function unlock(file: string, holder: string, files: LockFiles = lockFiles): void {
  const content = files.read(file)
  const current = content === undefined ? undefined : parseLock(content)
  if (current !== undefined && isMine(current) && current.holder === holder) files.remove(file)
}

/**
 * The file operations of locks. Unit tests run other contenders between them, to check every
 * interleaving.
 */
export interface LockFiles {
  /** Writes `content` to the new or existing file `path`. */
  write(path: string, content: string): void
  /** Links `path` to `existing`; throws EEXIST if `path` exists. */
  link(existing: string, path: string): void
  /** The content of `path`, or undefined if it doesn't exist. */
  read(path: string): string | undefined
  /** Removes `path`, if it exists. */
  remove(path: string): void
}

const lockFiles: LockFiles = {
  write: (path, content) => writeFileSync(path, content),
  link: (existing, path) => linkSync(existing, path),
  read: (path) => readContent(path),
  remove: (path) => rmSync(path, { force: true }),
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
      return {
        holder: parsed.holder,
        pid: parsed.pid,
        booted: parsed.booted,
        ...(typeof parsed.incarnation === 'string' ? { incarnation: parsed.incarnation } : {}),
      }
    }
  } catch {
    // Unreadable: stale.
  }
  return undefined
}

/** Whether `lock` was taken by this process (not an earlier one that had its PID). */
function isMine(lock: LockHolder): boolean {
  return lock.pid === process.pid && lock.incarnation === incarnation
}

/** Whether the process that took `lock` is gone. */
function isStale(lock: LockHolder): boolean {
  // A live process's PID is its own, so an earlier process that had this PID is gone.
  if (lock.pid === process.pid) return lock.incarnation !== incarnation
  if (Math.abs(lock.booted - BOOTED) > BOOT_TOLERANCE_S) return true
  try {
    process.kill(lock.pid, 0)
    return false
  } catch (error) {
    // EPERM: it runs, as another user.
    return (error as NodeJS.ErrnoException).code === 'ESRCH'
  }
}

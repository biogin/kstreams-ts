// Internal: what an App Process keeps in its state directory besides State Stores — its lock on
// the directory, its identity, and a lock per Task directory.
//
// One state directory per App Process (ADR-0017). A state directory is used by one App Process at
// a time: on start, the process takes `<stateDir>/.lock`, a SQLite database it keeps an exclusive
// lock on (an OS file lock, which the kernel releases when the process ends, however it ends) until
// it stops. A second process that tries to start on the directory meanwhile is refused; once the
// first is gone, the lock is free again. Nothing tells whether a process lives by its PID, the
// clock or the host's boot: only the OS lock does. This needs a file system whose POSIX advisory
// locks the processes share (a local file system; not NFS without locking).
//
// App Process ID. Transactional IDs must be unique among the live producers of an application,
// and should stay the same when a process restarts, so its new producers fence the old ones and
// the broker aborts their open transactions at once. So each App Process has an ID, folded into
// its Workers' transactional IDs (`<applicationId>-<processId>-worker-<n>`):
// - With a state directory, the ID is kept in `<stateDir>/<applicationId>/process-id`: a process
//   restarted on the directory takes its ID again.
// - Without a state directory, a process gets a new random ID on every start. A crashed process's
//   open transaction is then only aborted by the broker when it times out (the commit interval
//   plus 10 s), and the new owners of its Tasks wait for that.
//
// Task locks. The Workers (worker threads) of one App Process share its Task directories. A Task
// moving between them must be closed by its old owner before its new owner opens its State Stores
// and Checkpoint, so its directory has a lock file, `.lock`, while a Worker has the Task open; the
// new owner waits for it. A lock is only ever created where none is (a hard link, which fails if
// the name exists) and removed by its holder. Every Task lock in the directory is this process's:
// those left by a process that is gone are removed when the next one starts, and those of a worker
// thread that was terminated when the Streams App has stopped.

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
import { join } from 'node:path'
import Database from 'better-sqlite3'
import { taskDirectory } from './state-store.js'

/** The ID of this App Process, while it holds it. */
export interface ProcessIdentity {
  readonly id: string
  /** Gives the ID, and the state directory, up. */
  release(): void
}

const PROCESS_ID = /^[0-9a-f]{12}$/
const PROCESS_ID_FILE = 'process-id'
const LOCK_FILE = '.lock'
const LOCK_POLL_MS = 50

/** The state directories this process holds, by path. */
const held = new Set<string>()

/**
 * Takes the state directory `stateDir`, if there is one, for this App Process, and its App
 * Process ID for `applicationId`. Throws if another process, or another Streams App of this one,
 * uses the directory.
 */
export function claimProcessIdentity(
  stateDir: string | undefined,
  applicationId: string,
): ProcessIdentity {
  if (stateDir === undefined) return { id: newProcessId(), release() {} }
  const releaseDirectory = holdStateDirectory(stateDir)
  try {
    const directory = join(stateDir, applicationId)
    mkdirSync(directory, { recursive: true })
    removeTaskLocks(directory)
    return { id: processIdOf(directory), release: releaseDirectory }
  } catch (error) {
    releaseDirectory()
    throw error
  }
}

/** Locks `stateDir` for this process until the returned function is called. */
function holdStateDirectory(stateDir: string): () => void {
  mkdirSync(stateDir, { recursive: true })
  const file = join(stateDir, LOCK_FILE)
  if (held.has(file)) {
    throw new Error(
      `stateDir ${stateDir} is in use by another Streams App of this process; give each its own stateDir`,
    )
  }
  const db = new Database(file, { timeout: 0 })
  try {
    // Exclusive locking mode keeps the lock of the first transaction until the database closes;
    // the journal is kept in memory, as there is nothing to keep.
    db.pragma('journal_mode = MEMORY')
    db.pragma('locking_mode = EXCLUSIVE')
    db.exec('BEGIN EXCLUSIVE; COMMIT')
  } catch (error) {
    db.close()
    if ((error as { code?: unknown }).code === 'SQLITE_BUSY') {
      throw new Error(
        `stateDir ${stateDir} is in use by another process; give each process its own stateDir`,
        { cause: error },
      )
    }
    throw error
  }
  held.add(file)
  let released = false
  return () => {
    if (released) return
    released = true
    held.delete(file)
    db.close()
  }
}

/** The App Process ID kept in `directory`, created if there is none. */
function processIdOf(directory: string): string {
  const file = join(directory, PROCESS_ID_FILE)
  try {
    const id = readFileSync(file, 'utf8').trim()
    if (PROCESS_ID.test(id)) return id
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error
  }
  const id = newProcessId()
  // Written in full under another name first, so a crash never leaves half an ID.
  const draft = `${file}.tmp`
  writeFileSync(draft, id)
  renameSync(draft, file)
  return id
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
  while (!tryLock(lock, holder)) {
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
  const lock = join(taskDirectory(stateDir, applicationId, task), LOCK_FILE)
  if (readContent(lock) === holder) rmSync(lock, { force: true })
}

/**
 * Removes every Task lock of the application, once none of this process's Workers runs: at start,
 * those of a process that is gone, and at stop, those of terminated worker threads.
 */
export function removeTaskLocks(applicationDirectory: string): void {
  let tasks: string[]
  try {
    tasks = readdirSync(applicationDirectory).filter((name) => /^\d+$/.test(name))
  } catch {
    return
  }
  for (const task of tasks) rmSync(join(applicationDirectory, task, LOCK_FILE), { force: true })
}

/** Takes the lock `file` for `holder` unless another holder has it; true if `holder` has it. */
function tryLock(file: string, holder: string): boolean {
  // Written in full under another name first, so nobody ever reads a half-written lock.
  const draft = `${file}.${randomBytes(4).toString('hex')}.tmp`
  writeFileSync(draft, holder)
  try {
    linkSync(draft, file)
    return true
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== 'EEXIST') throw error
  } finally {
    rmSync(draft, { force: true })
  }
  return readContent(file) === holder
}

function readContent(file: string): string | undefined {
  try {
    return readFileSync(file, 'utf8')
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') return undefined
    throw error
  }
}

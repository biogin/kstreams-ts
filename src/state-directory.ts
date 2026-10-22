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
// A lock file names its holder and the process (PID, its PID namespace on Linux, which boot of
// the host, the process's incarnation: a random value of this process, shared by its worker
// threads, and its liveness token, below). A lock is stale, and taken over, when that process is
// gone: the host has rebooted since, or, for a process in this PID namespace, its PID isn't
// running or the PID is this process's but the incarnation isn't (an earlier process that had the
// same PID), or, for a process in another PID namespace (another container, or this container
// before it restarted), its liveness file isn't held. PIDs tell nothing across PID namespaces:
// containers on one host typically all run as PID 1.
//
// Liveness files. An App Process with a state directory holds `processes/<incarnation>.alive`, a
// SQLite database it keeps an exclusive lock on (an OS file lock, which the kernel releases when
// the process ends, whatever its PID namespace), from when it takes its App Process ID until it
// gives it up. Another process tells whether it runs by trying to read the file: the lock makes
// that fail. The file is locked under a private name before it is renamed to its own, so a starting
// process that removes the readable (dead) ones never removes one that is still being taken. This needs a file system whose POSIX advisory locks the processes share (a local
// file system, e.g. one host directory bind-mounted into several containers; not NFS without
// locking). A lock in another PID namespace without a liveness token is never taken over.
//
// The boot is told by the kernel's
// boot ID on Linux, which no wall-clock step changes; elsewhere by when the host booted, derived
// from the wall clock and uptime (macOS adjusts its boot time when the clock steps, so the two
// agree; a step of more than BOOT_TOLERANCE_S where it doesn't could make a live process's lock
// look stale). A lock of this same process is
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
import {
  existsSync,
  linkSync,
  mkdirSync,
  readdirSync,
  readFileSync,
  readlinkSync,
  renameSync,
  rmSync,
  writeFileSync,
} from 'node:fs'
import { uptime } from 'node:os'
import { join } from 'node:path'
import Database from 'better-sqlite3'
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

/** Which boot of the host a process runs in, and in which PID namespace. */
export interface HostBoot {
  /** The kernel's ID of this boot, if it has one (Linux): unlike `booted`, no clock step moves it. */
  readonly bootId?: string
  /** When the host booted, in seconds since the epoch (to a few seconds), from the wall clock. */
  readonly booted: number
  /** The process's PID namespace, if the OS has them (Linux), e.g. `pid:[4026531836]`. */
  readonly pidNamespace?: string
}

/** How far two readings of the boot time may differ for the same boot. */
const BOOT_TOLERANCE_S = 120

function readBootId(): string | undefined {
  try {
    const id = readFileSync('/proc/sys/kernel/random/boot_id', 'utf8').trim()
    return id === '' ? undefined : id
  } catch {
    return undefined
  }
}

function readPidNamespace(): string | undefined {
  try {
    return readlinkSync('/proc/self/ns/pid')
  } catch {
    return undefined
  }
}

/** This process's boot of the host. */
const HOST_BOOT: HostBoot = (() => {
  const bootId = readBootId()
  const pidNamespace = readPidNamespace()
  return {
    booted: Math.round(Date.now() / 1000 - uptime()),
    ...(bootId === undefined ? {} : { bootId }),
    ...(pidNamespace === undefined ? {} : { pidNamespace }),
  }
})()

/** Tells whether processes run, by their liveness files (see above). */
export interface Liveness {
  /** This process's liveness token, if it holds a liveness file. */
  token(): string | undefined
  /** Whether the process with liveness token `token` still runs. */
  alive(token: string): boolean
}

const TOKEN = /^[0-9A-Za-z_-]+$/
const ALIVE_SUFFIX = '.alive'

/** The liveness files in `directory` (an application's `processes` directory). */
export function livenessIn(directory: string): Liveness {
  return {
    token() {
      // The file exists only while this process (its main thread) holds it.
      return existsSync(aliveFile(directory, incarnation)) ? incarnation : undefined
    },
    alive(token) {
      if (!TOKEN.test(token)) return false
      const file = aliveFile(directory, token)
      // This process's own: alive (and not to be opened twice, which could drop its lock).
      if (held.has(file)) return true
      let db: Database.Database | undefined
      try {
        db = new Database(file, { fileMustExist: true, timeout: 0 })
        db.prepare('SELECT count(*) FROM sqlite_master').get()
      } catch (error) {
        // Locked: its process holds it. Anything but a missing file counts as alive, to be safe.
        return (error as { code?: unknown }).code !== 'SQLITE_CANTOPEN'
      } finally {
        db?.close()
      }
      // Readable: its process is gone, and nobody holds that token again. The file is left over.
      rmSync(file, { force: true })
      return false
    },
  }
}

function aliveFile(directory: string, token: string): string {
  return join(directory, `${token}${ALIVE_SUFFIX}`)
}

/** The liveness files this process holds, with how many App Processes use each. */
const held = new Map<string, { db: Database.Database; users: number }>()

/** A step of taking a liveness file. Unit tests run other processes' cleanup between them. */
export type LivenessStep = 'create' | 'lock' | 'publish'

/**
 * Holds this process's liveness file in `directory` until the returned function is called.
 * `preempt` runs before each step.
 */
export function holdLiveness(
  directory: string,
  preempt: (step: LivenessStep) => void = () => {},
): () => void {
  const file = aliveFile(directory, incarnation)
  let entry = held.get(file)
  if (entry === undefined) {
    // Created and locked under a private name that no other process's cleanup looks at, then
    // published under its own name already locked: a readable `.alive` file is always a dead one's.
    const draft = `${file}.${randomBytes(4).toString('hex')}.tmp`
    preempt('create')
    const db = new Database(draft)
    try {
      // Exclusive locking mode keeps the lock of the first transaction until the database closes;
      // the journal is kept in memory, as there is nothing to keep.
      preempt('lock')
      db.pragma('journal_mode = MEMORY')
      db.pragma('locking_mode = EXCLUSIVE')
      db.exec('BEGIN EXCLUSIVE; COMMIT')
      preempt('publish')
      renameSync(draft, file) // atomic; the lock stays with the file
    } catch (error) {
      db.close()
      rmSync(draft, { force: true })
      throw error
    }
    entry = { db, users: 0 }
    held.set(file, entry)
  }
  const current = entry
  current.users++
  let released = false
  return () => {
    if (released) return
    released = true
    if (--current.users > 0) return
    held.delete(file)
    current.db.close()
    rmSync(file, { force: true })
  }
}

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
  const liveness = livenessIn(directory)
  // Removes the liveness files of processes that are gone.
  for (const name of readdirSync(directory)) {
    if (name.endsWith(ALIVE_SUFFIX)) liveness.alive(name.slice(0, -ALIVE_SUFFIX.length))
  }
  const releaseLiveness = holdLiveness(directory)
  const known = readdirSync(directory)
    .filter((name) => PROCESS_ID.test(name))
    .sort()
  try {
    for (;;) {
      const id = known.shift() ?? newProcessId()
      writeFileSync(join(directory, id), '', { flag: 'a' })
      const lock = join(directory, `${id}.lock`)
      if (tryLock(lock, id, false, lockFiles, HOST_BOOT, liveness)) {
        return {
          id,
          release() {
            unlock(lock, id)
            releaseLiveness()
          },
        }
      }
    }
  } catch (error) {
    releaseLiveness()
    throw error
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
  const liveness = livenessIn(join(stateDir, applicationId, 'processes'))
  while (!tryLock(lock, holder, true, lockFiles, HOST_BOOT, liveness)) {
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
  bootId?: string
  incarnation?: string
  pidNamespace?: string
  /** The holder's liveness token. */
  alive?: string
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
  host: HostBoot = HOST_BOOT,
  liveness?: Liveness,
): boolean {
  const token = liveness?.token()
  for (;;) {
    // Written in full under another name first, so nobody ever reads a half-written lock.
    const mine = {
      holder,
      pid: process.pid,
      booted: host.booted,
      ...(host.bootId === undefined ? {} : { bootId: host.bootId }),
      ...(host.pidNamespace === undefined ? {} : { pidNamespace: host.pidNamespace }),
      incarnation,
      ...(token === undefined ? {} : { alive: token }),
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
    if (current !== undefined && !isStale(current, host, liveness)) return false
    // Stale: only the holder of its claim removes it, and only while it is still this very lock.
    const claim = `${file}.${createHash('sha256').update(content).digest('hex').slice(0, 16)}${CLAIM_SUFFIX}`
    // Reentrant: a claim of this very holder can only be left over from a terminated worker thread.
    if (!tryLock(claim, holder, true, files, host, liveness)) return false // another contender takes it over
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
        ...(typeof parsed.bootId === 'string' ? { bootId: parsed.bootId } : {}),
        ...(typeof parsed.incarnation === 'string' ? { incarnation: parsed.incarnation } : {}),
        ...(typeof parsed.pidNamespace === 'string' ? { pidNamespace: parsed.pidNamespace } : {}),
        ...(typeof parsed.alive === 'string' ? { alive: parsed.alive } : {}),
      }
    }
  } catch {
    // Unreadable: stale.
  }
  return undefined
}

/** Whether `lock` was taken by this process (not an earlier one that had its PID). */
function isMine(lock: LockHolder): boolean {
  // The incarnation is random, so no process in another PID namespace has it.
  return lock.pid === process.pid && lock.incarnation === incarnation
}

/** Whether the process that took `lock` is gone. */
function isStale(lock: LockHolder, host: HostBoot, liveness: Liveness | undefined): boolean {
  // Taken in another boot of the host. Boot IDs, where both have one, are immune to clock steps.
  if (lock.bootId !== undefined && host.bootId !== undefined) {
    if (lock.bootId !== host.bootId) return true
  } else if (Math.abs(lock.booted - host.booted) > BOOT_TOLERANCE_S) return true
  // Another PID namespace: its PIDs mean nothing here; only its liveness file tells.
  if (
    lock.pidNamespace !== undefined &&
    host.pidNamespace !== undefined &&
    lock.pidNamespace !== host.pidNamespace
  ) {
    return lock.alive !== undefined && liveness !== undefined && !liveness.alive(lock.alive)
  }
  // A live process's PID is its own, so an earlier process that had this PID is gone.
  if (lock.pid === process.pid) return lock.incarnation !== incarnation
  try {
    process.kill(lock.pid, 0)
    return false
  } catch (error) {
    // EPERM: it runs, as another user.
    return (error as NodeJS.ErrnoException).code === 'ESRCH'
  }
}

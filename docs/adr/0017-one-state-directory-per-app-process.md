# One state directory per App Process

Each App Process has a state directory of its own. On start it takes an exclusive OS file lock on `<stateDir>/.lock` and keeps it until it stops; the kernel releases it when the process dies, however it dies. A process that starts on a state directory another live process holds fails at once: `stateDir <path> is in use by another process; give each process its own stateDir`. The App Process ID is a single ID kept in the state directory and reused on restart. Task directory locks only coordinate the worker threads of one process; any left in the directory when a process starts are leftovers of a dead one and are removed.

We used to let processes on one host share a state directory, which only saved disk and Restore time when they happened to share it. Supporting it needed a cross-process lock protocol: stale-lock takeover with claim files, PID and same-PID-incarnation checks, boot IDs, PID namespaces and liveness files for containers. It took four rounds of review to fix its correctness bugs, each a way to take a live holder's lock or to keep a dead holder's forever, and it rested on heuristics the coding standards forbid (no clock or PID liveness). An OS-held lock, owned by exactly one process, has no such failure modes.

## Consequences

- Processes on one host, or containers sharing a host directory, each need their own state directory. A deployment that shared one fails to start instead of running unsafely.
- Whether a lock's holder lives is decided only by the OS lock: no PID, clock, boot or namespace logic is left. The state directory needs a file system with POSIX advisory locks (a local one; not NFS without locking).
- A Task that moves between processes never shares local files: its new owner restores its State Stores from the Changelog into its own state directory (in full, if it never owned the Task), and exactly-once still relies on fencing and stable offsets, not on local locks.

# Operations

## Broker version support

- **Minimum: Apache Kafka 3.6**, using the classic consumer group protocol. The new KIP-848 consumer group protocol is not supported yet, on Kafka 4.x either; the classic protocol works there.
- **Tested on Kafka 3.9 and 4.x** (single-node KRaft brokers) in CI: the integration suite runs against both, and the crash suite against the latest 4.x.
- Exactly-once needs a broker with transaction support (KIP-447, the transactional offset commit with group metadata), which 3.6 has. With fewer than three brokers lower `transaction.state.log.replication.factor` and `transaction.state.log.min.isr`, and use `changelogReplicationFactor: 1`.

## State directory

`stateDir` is where this App Process keeps:

- the SQLite State Stores and their Checkpoints, in `<stateDir>/<applicationId>/<task>/`;
- its **App Process ID** (`<stateDir>/<applicationId>/processes/`), used in the transactional IDs, and a liveness file (`<incarnation>.alive`) it keeps an OS file lock on while it runs;
- a `.lock` file per Task directory while a Worker has the Task open.

Rules:

- **Keep it across restarts** and give it **its own directory per process**. A restart then takes its old App Process ID, so crashed transactions are fenced at once, and restores only the Changelog after each Checkpoint.
- Processes that share one host may share a state directory; they take different App Process IDs and lock Tasks. Stale locks of dead processes are taken over. Containers on one host may share one too (each typically runs as PID 1 in its own PID namespace): their liveness is told by the liveness files' OS locks, not PIDs, so the directory must be on a local file system with POSIX advisory locks (a bind mount of a host directory; not NFS without locking). Never share a state directory across hosts.
- **Do not copy, restore from a backup, or edit** a state directory by hand. A store whose Checkpoint is missing or unclean is wiped and fully restored, which is safe, but you gain nothing from copies; the Changelog is the source of truth and the only backup you need.
- Size disks for the SQLite files of the Tasks a process may own, including after a rebalance when it takes over Tasks from a failed process.
- Without a `stateDir`, State Stores are in memory: every start is a full Restore, and each start gets a new, random App Process ID, so a crashed process's open transactions are only aborted by their timeout and the new owners of its Tasks wait for that.

## Disposable disks

When the local disk doesn't survive a restart (containers without persistent volumes, Kubernetes pods without a PersistentVolume), **every start is a full Restore** of every store of every owned Task. This is correct, but the time is proportional to the Changelog size. A Task restores when its first record arrives; there is no limit on concurrent Restores, and the app stays in the `restoring` state until all are done (watch `restore-progress` and the `restore-remaining` metric).

Mitigations: use a persistent volume per replica (for example a StatefulSet), keep stores small (windows expire, delete keys with Tombstones), use compacted Changelogs (they are by default), and make readiness probes wait for `running` with a generous startup timeout.

## Deploys and shutdown

On `SIGTERM` call `app.stop()` and give the process at least `shutdownTimeoutMs` (default 30 s) before killing it. A graceful stop commits the current batch, writes Checkpoints, and leaves the consumer group so Tasks move at once. A rolling deploy of an app with persistent volumes restores only a few records per Task.

## Scaling and sizing

See [workers](./workers.md). The useful number of Workers is at most the input partition count. A Task's records are processed one at a time, so per-key order is kept, and one slow Task (or an async Processor) limits that partition's throughput.

## Checklist

- Create input and output topics and (if auto-creation is off) the Internal Topics; input topics have the same partition count.
- `guarantee: 'exactly-once'` for state you cannot afford to double count, with downstream `read_committed`.
- A stable Application ID and explicit store names.
- A persistent state directory per process, `SIGTERM` handling, a readiness probe on `running`, a liveness probe on not `error`.
- Alert on `consumer-lag` growing, on `aborts`, `records-dropped` and `late-records-dropped`.

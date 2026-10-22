# 29: Scale out across processes

**What to build:** Running several processes with the same Application ID spreads Tasks across all their Workers using cooperative-sticky assignment, so rebalances move as few Tasks as possible.

**Blocked by:** 18

**Status:** done (branch `ticket/29`)

- [x] Cooperative-sticky assignor configured
- [x] Integration test: 2 processes × 2 Workers share Tasks, results are exact under exactly-once
- [x] Adding or removing a process moves only the Tasks it has to

Split from the original ticket 18.

Spec: [../spec.md](../spec.md). Glossary: [CONTEXT.md](../../../CONTEXT.md). ADRs: [docs/adr/](../../../docs/adr/)

## Notes from ticket 15

- Transactional IDs are `<applicationId>-worker-<n>`, per Worker number, not per process: two processes running the same exactly-once app would fence each other. Needs a stable instance identity (e.g. persisted in the state directory) folded into the transactional ID.

## Notes from ticket 28

- The client reads committed offsets without `requireStable`. With several exactly-once members, a new owner can start from a stale offset and produce duplicates. Before processing newly assigned partitions, fetch their stable committed offsets.
- Transactional IDs are still per Worker number (see ticket 15 note), so two exactly-once processes fence each other; ticket 28's exactly-once rebalance test uses a raw consumer as the second member for that reason.
- Re-check `src/ownership.ts` under cooperative-sticky: join events can arrive late, and one rebalance can bump the generation twice.

## Notes from ticket 18

- Workers in one process share `stateDir` and nothing locks a Task's directory: a Task moving between Workers (or processes on one host) could be opened by its new owner before the old owner closes it. Add a per-Task lock (e.g. an exclusive lock file) and wait/retry on acquire.
- The stale-committed-offset risk (ticket 28 note) now also applies between Workers inside one process.

## Comments

- 2026-10-22: Done on branch `ticket/29`.
  - **Assignor (`src/task-assignor.ts`):** our own `partitionAssigner` under the client's `cooperative-sticky` protocol name (subscription version 3), so the client still runs the two rounds. It assigns *whole Tasks*: the client's assignor balances topic-partitions and could split a Task's Co-partitioned partitions over two members (the old roundrobin could too, whenever partitions % members ≠ 0). Sticky: owners keep their Tasks up to a fair share. Members that already have the most Tasks get the larger shares, so a joining member takes Tasks only from members over their share. Cooperative: a Task another member still claims is only revoked in round 1, and assigned in round 2. Only claims from the latest generation count; a Task two members claim is contested and owned by neither. Unit tests: `test/task-assignor.test.ts`.
  - **Ownership (`src/ownership.ts`):** it now also follows `consumer:group:rebalance`. In round 1 of a cooperative rebalance the client reduces `assignments` and emits only that event (no `group:join`), and the generation goes up by one per round. A rebalance event with an unchanged assignment (heartbeat REBALANCE_IN_PROGRESS) changes nothing. A step of +1 (or 0, when the client drops the assignment) is continuous; a larger step loses everything, as before. Unit tests: `test/ownership.test.ts`.
  - **App Process ID (`src/state-directory.ts`):** transactional and client IDs are `<applicationId>-<processId>-worker-<n>`. IDs (12 hex chars) live in `<stateDir>/<applicationId>/processes/`: one empty file per ID, plus `<id>.lock` while a process holds it. At start, a process takes the first ID nobody holds, or creates a random one, so a restart takes the same ID and fences its old producers. **Two processes sharing a stateDir** get different IDs (no fencing), and the per-Task locks below coordinate their Task directories; this is covered by integration test 1. Without a stateDir, every start gets a random ID, so a crashed process's open transaction lasts until its timeout (commit interval + 10 s), and new owners wait for it. Locks hold `{holder, pid, booted}`. A lock is stale when its PID isn't running or the host has rebooted since. A lock from this same process is never stale. The Runtime removes its Task locks and releases the ID once all Workers have stopped, which also covers terminated threads. Taking over a stale lock uses rename and then checks the content; a tiny race window remains, and documented.
  - **Task locks:** `<taskDir>/.lock`, taken before a Task opens its stores (with a stateDir and State Stores), released on Task close. The wait polls every 50 ms and gives up when partitions are lost, a stop is requested, or a failure occurs. This way two Workers waiting for each other's Tasks deal with their losses first instead of deadlocking.
  - **Stable committed offsets (`src/stable-offsets.ts`):** under exactly-once, every new Task first reads `listConsumerGroupOffsets({ requireStable: true })` for its partitions. It retries on UNSTABLE_OFFSET_COMMIT until the previous owner's transaction ends (giving up on loss or stop), then raises `#position`, so records below the offset are skipped. Under exactly-once, every new Task now starts between batches (this used to apply only with stores).
  - **Restore (`src/restore.ts`):** before reading, a read waits until the Changelog partition's last stable offset reaches the end it had when the read started. This matters because the previous owner's transaction may still be open, or committed with its markers not yet written. Stable offsets alone don't cover this, since markers reach `__consumer_offsets` and the Changelog independently. It also covers our own abort just before a rewind.
  - **Tests:** `test/integration/scale-out.test.ts`: (1) 2 App Processes × 2 Workers sharing **one** stateDir, 8 partitions, 800 records: exact counts 1..n per key, 4 members with 2 Tasks each, 2 process IDs. (2) Continuous input: P1 alone (4+4 Tasks); P2 joins, and each P1 Worker keeps 2 of its own 4; P2 stops, and P1's Workers keep theirs and take P2's back; then exact counts over everything. (3) A previous owner's transaction holds a pending offset commit (6) when the app takes the Task. Once it commits, the app outputs exactly records 6..9. Mutation checks: transactional IDs without the process ID → test 1 times out (the processes fence each other); roundrobin → test 2 fails on stickiness; without the stable read → test 3 outputs 0..9. `rebalance.test.ts`'s intruder consumer now uses the cooperative protocol (otherwise INCONSISTENT_GROUP_PROTOCOL). Full checks pass: unit tests, the integration suite twice (48 tests), and the crash suite once.
  - **For later tickets:**
    - 19: Task start now waits on the lock, the stable offsets and the Changelog LSO, all in the commit cycle between batches. Non-blocking Restore has to move all three off the cycle. Also, an exactly-once abort still closes and restores *every* Task (from its Checkpoint), including the ones it keeps; per-Task abort would preserve more of the stickiness.
    - 20: crash tests should cover a SIGKILLed process whose Tasks move to *another* App Process: its open transaction then blocks the new owner until it times out, because only the same App Process ID fences it at once. Also cover the stale-lock takeover.
    - 25: rebalance rounds are visible in Ownership (rebalance and join events), which suits a `rebalancing` state; waits on locks, unstable offsets and the LSO would make good metrics or log events (today they wait silently).
    - A zombie Worker (kicked out without knowing it) whose transaction holds only wall-clock-timer Changelog writes and no offsets for the lost Task could still commit them after the new owner restored. This is an edge case and isn't handled.

# 20: Crash suite covers rebalances

**What to build:** The crash test suite also scales Workers and processes up and down mid-run and kills during rebalances, still checking for exact results.

**Blocked by:** 17, 19, 29

**Status:** ready-for-agent

- [x] Scenarios: scale up, scale down, kill during rebalance
- [x] Same exact-comparison oracle as ticket 17

Spec: [../spec.md](../spec.md). Glossary: [CONTEXT.md](../../../CONTEXT.md). ADRs: [docs/adr/](../../../docs/adr/)

## Notes from ticket 29

- Cover a SIGKILLed process whose Tasks move to a different process (the new owner waits out the dead process's transaction timeout), and taking over a `.lock` left behind by a dead process.

## Comments

- 2026-10-22: Done on branch `ticket/20`.
  - **Suite:** new `test/crash/rebalance.test.ts`; the oracle, input and settings moved from `crash.test.ts` to `test/crash/crash-run.ts` (unchanged logic: outputs, Changelogs and SQLite stores against the reference, and Restore's reader against `log-reader.ts`, which is untouched). `crash-app.ts` takes an optional `workers`; `crash-workers-app.ts` is its App Module for worker threads (ADR-0013). `run-app.ts` prints errors with `inspect(..., { depth: 8 })` so nested Kafka error codes show.
  - **Scenarios** (several App Processes, same Application ID, each with its own or a shared state directory; input keeps arriving): `scale-up` (A, then B in its own directory, then C sharing A's); `scale-down` (A, B, C; C SIGKILLed for good, so its Tasks move and A takes over its stale `.lock` files and waits out its transactions; then A or B stopped gracefully); `kill-rebalance` (3 rounds: a process joins or stops in the background, a kill 0–1.5 s later, the victim restarted or left dead); `kill-restore` (6,000 records preloaded; a *gate*, an open transaction of another producer on every `counts` Changelog partition, holds every Restore open so held records overflow `commitMaxRecords`; then a kill of the restoring process, a process joining in the same directory to cancel starts plus a kill, or a kill of the old owner; the gate is aborted 0.5–3 s later, so the oracle ignores its records; then a full Restore into a fresh directory killed 0–600 ms after running); `workers` (2 worker threads per process: kill+restart, a process in a shared directory killed for good, a kill while another joins). At the end every process but a random survivor stops gracefully, the input ends with one record per partition (own keys, so the survivor opens every Task), and the survivor's state directory must hold the exact stores. A process that exits by itself fails the iteration at once; a graceful stop that does not exit 0 is a problem reported with the oracle's (the run goes on, as after a kill).
  - **Settings:** `KSTREAMS_CRASH_SCENARIOS` (comma list; `kill` is `crash.test.ts`, the rest are above; default all), `KSTREAMS_CRASH_REBALANCE_ITERATIONS` (default 2 per scenario), `KSTREAMS_CRASH_SEED` (iteration i of each scenario uses seed + i). A failure prints scenario, seed, journal (timed steps) and the rerun command. Vitest runs the two files in parallel: the whole `npm run test:crash` takes about 4–5 minutes.
  - **Runs at defaults:** 4 full runs; 2 passed (14/14), 2 failed on the engine bugs below; the exactly-once oracle was exact in every iteration that got to it. Sanity check: with `guarantee: 'at-least-once'` in the fixture, `kill-rebalance` fails (duplicate counts and checksums).
  - **Engine bugs found, not fixed (the suite fails on them intermittently, on purpose):**
    1. **A graceful stop during a rebalance crashes the process with an unhandled 'error' event.** The engine attaches no `'error'` listener to its consumers (`src/worker.ts`; `ReadCommittedConsumer`). When `stop()` closes the consumer while the group is rebalancing, the client's heartbeat-triggered rejoin fails and the client emits `'error'` (`NetworkError: Connection closed`, or `MultipleErrors: joinGroup failed 1 times` with `REBALANCE_IN_PROGRESS` + `UserError: Client closed while retrying joinGroup`, from `#retryJoinGroup`, consumer.js L967/L1465), so Node throws and the process exits with code 1 instead of 0. Seen in `scale-up` seed 1268049968 (stop A right after C joined), `kill-restore` seeds 1263592374 and 16853522 (stops right after a process was killed for good, while the group waited for its session to expire). Data stayed exact. Fix direction: an `'error'` listener on every client that routes errors to the Worker's failure path, and ignores them once the Worker is stopping.
    2. **The input consumer dies on `INVALID_FETCH_SESSION_EPOCH` after a rebalance.** `scale-up` seed 16853522 (and once with seed 1515259580): when B and C stopped and A took all Tasks, A's input fetch failed 4 times with error code 71 (`fetch failed 4 times`, each `INVALID_FETCH_SESSION_EPOCH`), so A went to `error` and exited; its last records were never consumed. The client drops the session on that error and retries, yet every retry failed the same way, which suggests concurrent fetches sharing one fetch session (e.g. a Worker's old and new record stream, or a rewind, with a Fetch still in flight); the engine's `readCommittedFetchApi` passes session ID and epoch through unchanged. Needs diagnosis (client fetch sessions vs. how the Worker re-creates its stream on assignment changes).

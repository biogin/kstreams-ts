# 18: Multiple Workers (worker threads)

**What to build:** `workers > 0` runs each worker thread as its own consumer-group member with its own consumer, producer and State Stores (ADR-0007). `workers: 0` still runs in the main thread.

**Blocked by:** 16, 28

**Status:** done

- [x] Worker threads, each a group member, with results exact under exactly-once
- [x] Workers rebuild the app by importing the App Module named in `workers: { count, module }` (ADR-0013); a clear error if `workers > 0` without a module
- [x] Integration test: 1 process × 2 Workers share Tasks

Decided: App Module, see ADR-0013. Split from the original ticket 18; scaling out across processes is ticket 29.

Spec: [../spec.md](../spec.md). Glossary: [CONTEXT.md](../../../CONTEXT.md). ADRs: [docs/adr/](../../../docs/adr/)

## Comments

- **API:** `workers?: number | WorkersConfig`, with `WorkersConfig = { count, module: string | URL }` (exported). `workers: 0` or `{ count: 0, module }` runs one Worker in the main thread as before. `workers: N` (a number above 0) or an object without `module` throws `workers > 0 needs an App Module: use workers: { count: N, module: import.meta.url } ...`; a `module` that isn't a URL throws too.
- **App Module contract:** its default export is the Streams App itself, the same object whose config names the module. Each worker thread imports the module and checks that the default export is a Streams App with the same Application ID. Otherwise it fails with `App Module <url> must export the Streams App as its default export ...` or `... exports Streams App "x", not "y"`.
- **Code:** `src/worker-thread.ts` is the main side: `startWorkerThread` returns a `Worker` handle over a `node:worker_threads` thread, with messages `running` / `stopped` / `failed` and `stop`. `src/worker-thread-entry.ts` is the thread side: it imports the module and calls the internal `startWorkerOf(app, …)` from streams-app.ts, which runs the same `startWorker` as `workers: 0`. The thread entry is `./worker-thread-entry.js` next to the built file, or `.ts` when the sources run directly. Errors cross the thread boundary by structured clone, which keeps message and stack; custom classes like `DeserializationError` arrive as plain `Error`s, and uncloneable errors are copied as `Error`.
- **Lifecycle (`Runtime` in streams-app.ts):** `start()` starts all Workers in parallel and resolves when all are running. If one fails to start, the app goes to `error`, stops the others, and `start()` rejects. A Worker failure later puts the app in `error` and stops the other Workers right away, with no `stop()` call needed; `stop()` then rejects with the first error. `stop()` stops all Workers in parallel. Each one respects `shutdownTimeoutMs`; a thread that hasn't reported `stopped` within `shutdownTimeoutMs + 10 s` is terminated and counts as a failure. `app.state` is still created / running / stopping / stopped / error (no rebalancing/restoring yet; ticket 25).
- **Transactional and client IDs:** `<applicationId>-worker-<n>` per Worker number, n = 0..count−1 (per-process identity is ticket 29).
- **Tests:** `test/lifecycle.test.ts` covers the config errors. `test/integration/workers.test.ts` has three tests:
  1. One App Process × 2 Workers on 4 partitions, exactly-once, SQLite stores in a shared state directory, 300 records produced before start and 300 after. Every key's counts are exactly 1..50 in order, the group has 2 members, and SIGTERM exits 0 with both leaving the group.
  2. In-process, one Worker fails (a marker file makes sure only the first Worker to see the value fails). The app goes to `error` and the group empties without a `stop()` call; checked by mutation: without the auto-stop the healthy Worker stays. `stop()` rejects with the error.
  3. Threads that find no app in the module: `start()` rejects with the App Module error.

  Passed 5 runs in a row on their own, and in two full integration runs.
- **Test infra:** `vitest.integration.config.ts` now preloads `harness/ts-resolve.ts` (`execArgv`), so worker threads started from the test process can run the TS sources; worker threads inherit `--import` from their process. `run-app.ts` accepts an App Module whose default export is the app itself. Such App Modules read their params from `KSTREAMS_APP_PARAMS`, see `fixtures/workers-count-app.ts`.
- **For later tickets:**
  - 29: transactional IDs are per Worker number, so two processes still fence each other. The stale-committed-offset risk from ticket 28's note now applies within one process too: it didn't show up in the runs above, but rebalances between Workers are where it would.
  - 29/19: Workers in one process share `stateDir`, so a Task moving between two Workers of the same process uses the same Task directory, and nothing locks it. Ownership is noticed after the fact (ADR-0006), so the new owner could open the SQLite store and Checkpoint while the old owner still has them open. Consider a per-Task lock or a close-before-open handoff with 19.
  - 25: only running / stopped / failed cross the thread boundary. Aggregated rebalancing/restoring states need Workers to report their state over the same channel.
  - 23: errors from worker threads lose their class (structured clone), so error handlers that dispatch on class must run inside the thread.

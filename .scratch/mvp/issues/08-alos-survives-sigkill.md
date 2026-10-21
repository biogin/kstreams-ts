# 08: At-least-once survives SIGKILL

**What to build:** An app run as a child process can be SIGKILLed mid-stream and restarted, and every input record still shows up in the output. Duplicates are allowed. The child-process harness built here is reused by the crash test suite.

**Blocked by:** 07

**Status:** done

- [x] Reusable harness: run a Streams App in a child process, SIGKILL at a chosen or random time, restart
- [x] Integration test: kill mid-run, restart, assert nothing is missing

Spec: [../spec.md](../spec.md). Glossary: [CONTEXT.md](../../../CONTEXT.md). ADRs: [docs/adr/](../../../docs/adr/)

## Comments

- 2026-10-21: Done on branch `ticket/08`.
  - **Harness** (`test/integration/harness/app-process.ts`): `launchAppModule(appModuleUrl, params, { timeoutMs? })` forks `run-app.ts`, which imports the App Module, calls its default export with the JSON `params` to build the Streams App, `start()`s it and reports `running` over IPC; the promise resolves then (consumer group joined) and rejects with the child's output if it exits first. `AppProcess`: `pid`, `exited`, `log()`, `kill()` (SIGKILL, waits for exit, idempotent), `stop()` (SIGTERM → graceful `app.stop()`, exit 0). `randomBetween(min, max)` for random kill points. The child exits on a fatal error (polls `app.state === 'error'` until events land in ticket 25) and on parent disconnect (no orphan group members).
  - **App Modules** live in `test/integration/fixtures/` (`pass-through-app.ts`): default export `(params) => StreamsApp`, no side effects, `sessionTimeoutMs: 6_000` (broker minimum) so a SIGKILLed Worker's partitions move to the restarted one quickly.
  - **Running TS in the child:** plain Node type stripping plus a `.js` → `.ts` resolve hook (`harness/ts-resolve.ts`, preloaded with `--import`); no build step. Node 26 removed `--experimental-transform-types`, so `tsconfig.json` now sets `erasableSyntaxOnly` and the one parameter property (`SerializedKey`) became an explicit field. `src` must stay erasable-only TypeScript.
  - **Kafka helper:** `kafka.consumeUntil(topic, done(record), { timeoutMs })` reads until `done` returns true and returns all records (duplicates included); `consume` now shares the same reader.
  - **Test** (`test/integration/at-least-once-crash.test.ts`): 3,000 records produced over ~1.5 s to 3 partitions while the app is SIGKILLed 3 times at random points (200–1000 ms) and restarted; every input value arrives, duplicates allowed. Checked by mutation: committing offsets before producing makes it fail. Takes ~20 s, mostly session timeouts.
  - **For later tickets:** crash suites (17, 20) should reuse `launchAppModule` + `consumeUntil` and compare against a reference run; each restart waits up to one session timeout to get its partitions back, so iterations cost ~6 s per kill. With `workers > 0` (ticket 18) the App Module is imported again by each worker thread, so its URL must be importable there too.

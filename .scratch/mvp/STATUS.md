# MVP implementation status

Integration branch: `mvp`. Plan (2026-10-21): finish ALL remaining tickets before any release, then one code review over everything and a fix pass, then release prep (ticket 26) as 0.1 beta. No rush; the earlier M3 stop is dropped.

## Merged (30 of 30)

All 30 tickets (01–30), plus three fixes: aborted-transaction filter, root-cause error reporting, fetch sessions for the restore consumer. `mvp` passes typecheck, lint, 341 unit tests, the integration suite (63 tests; see the scale-out item below) and the crash suite incl. rebalance scenarios.

## Remaining

1. Code review of `mvp` (`/code-review`), one fix agent, final full test run, worktree cleanup.
2. Left for the user: create the remote repository and fill in `repository`, `npm publish`, release tag, check the first CI run on Kafka 3.9.

## Open items for the code review / fix pass

- **Flaky integration tests** under broker load: `at-least-once-crash.test.ts` (once read 0 records), `passthrough.test.ts` "leaves the group on stop and resumes from the committed offsets on restart" (timeout), `scale-out.test.ts` (one 13 s timeout while the crash suite shared the broker), plus unnamed one-off timeouts. Make waits condition-based and robust.
- **Client internals:** `src/read-committed.ts` (`ReadCommittedConsumer`) hooks `kGetApi` and relies on the Fetch API's argument order (isolationLevel is argument 4). Re-check on every `@platformatic/kafka` upgrade.
- Leftover `crash-input-*` topics from diagnosis runs may still be on the local broker (`npm run kafka -- reset` clears everything).
- **Zombie timer writes (from ticket 29):** a zombie Worker whose open transaction holds only timer-driven Changelog writes could still commit them after the new owner has restored. Rare; needs a fix or a documented limitation.

- **Possible exactly-once duplicates (TOP PRIORITY):** `test/integration/scale-out.test.ts` once received 2502 records vs 2480 expected (duplicates) after the fetch-session fix merge; passed on rerun. First check whether the test reads outputs with the stock client consumer (which can deliver aborted records, #467) — if so, switch it to our abort-aware reader and rerun many times; if duplicates persist, it's an engine bug in multi-process rebalances.
- **Silent wrong aggregation (from ticket 26, high priority):** `map` (key change) followed by `groupByKey` doesn't repartition, so records with the same new key can land in different Tasks. Repartition Topics are v1 scope; for 0.1, fail fast when the app is built with a clear message.
- **Pin `@platformatic/kafka` to an exact version** while `src/read-committed.ts` depends on its internals.
- **Unverified doc claims (ticket 26):** transaction timeout wording and timers not persisting across restarts come from source comments; verify.

## Fixed

- READ_COMMITTED filtering (platformatic/kafka#467): `src/read-committed.ts` sends Fetch as read_committed at the protocol level but bypasses the client's filter, and filters each response with Java's stateless `CompletedFetch` algorithm. Both Restore and the Worker's input path use it. Root cause of the crash-suite failures: the client keys aborted ranges by producer ID, and a later abort marker from a restarted producer stretched an old range over 630 committed records.

## Upstream

- platformatic/kafka#467 filed by the user; body corrected in place before anyone responded. Local copy: `.scratch/upstream/platformatic-kafka-read-committed-aborted/issue.md`. Upstream fix in progress: **PR platformatic/kafka#470** (open) implements the stateless per-response filter and also fixes the committed-records drop. Once it ships in a release, switch back to the client's filter and delete `src/read-committed.ts` (and its dependency on client internals); keep our read_committed unit/integration tests and the crash suite as regression guards. An optional follow-up comment for #470 (committed-records symptom + suggested regression test) was drafted but not posted.

## Resuming

- Research notes are in `.scratch/mvp/research/` (note: `kafka-client.md` predates the read_committed findings; see its warning at the top).
- Local broker: `npm run kafka -- start`, tests with `KSTREAMS_KAFKA_BROKERS=localhost:9092 npm run test:integration`, crash suite with `npm run test:crash`, then `npm run kafka -- stop`.
- Shared-broker rule for parallel agents: start if needed, never stop or reset.
- Usage rules: Sonnet for merges and mechanical tickets, Opus for hard ones, at most 2 implementers at a time.
- Old agent worktrees under `.claude/worktrees/agent-*` and `.claude/worktrees/ticket-22`, plus leftover branches `ticket/22` and `ticket/17`, can be removed during cleanup.

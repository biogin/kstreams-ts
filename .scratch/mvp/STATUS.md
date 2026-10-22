# MVP status

**State (2026-10-23): 0.1.0 beta released.** Published to npm as `kstreams-ts@0.1.0` (npm account `modernnn`; dist-tags `beta` and `latest` both 0.1.0, since npm makes the first version `latest`), verified by a clean install from the registry. Git tag `v0.1.0` on 9850cea; GitHub prerelease https://github.com/biogin/kstreams-ts/releases/tag/v0.1.0. Work goes on branches off `main`, merged by PR (CI runs integration on Kafka 3.9.1 and 4.4.0, plus the crash suite nightly). The `mvp` branch is history.

## Releasing (for the next version)

- Bump `version` in package.json and CHANGELOG.md on a PR; merge once CI is green.
- Publish from a real terminal, not through Claude Code (its shell isn't a TTY, so npm hides the 2FA approval link and fails with EOTP): `npm publish --access public` (add `--tag beta` for a prerelease). npm first shows a `0.0.0-stage` placeholder; the version appears after about 2 minutes.
- Then `git tag -a vX.Y.Z <commit> -m ...`, push the tag, `gh release create vX.Y.Z --generate-notes` (add install notes on top).
- The build runs from the `prepare` script, so `npm publish`, `npm pack` and git installs (`npm install github:biogin/kstreams-ts`) all build `dist/`.
- The local `main` in `~/code/kstreams-ts` may be behind: `git pull --ff-only origin main` there.

## Next

- Ticket 31 (split `src/worker.ts`) and ticket 32 (ultrareview finding 10).
- Drop `src/read-committed.ts` once platformatic/kafka#470 ships in a release (see Upstream). Until then `@platformatic/kafka` stays pinned to an exact version (2.14.0): `ReadCommittedConsumer` hooks `kGetApi` and relies on the Fetch API's argument order (isolationLevel is argument 4); re-check on every upgrade.
- Optional upstream PR for the client's fetch-session bug (see Upstream).
- Reopen if they recur: flaky integration timeouts under broker load (`passthrough.test.ts` "leaves the group on stop...", `scale-out.test.ts`); the likely cause, time-based retention deleting test topics, is fixed (`retention.ms=-1` in `test/integration/kafka.ts`).
- Unverified doc claims from ticket 26: the transaction-timeout wording and "timers don't persist across restarts" come from source comments; verify.

## Resolved (kept for context)

- **Kafka 3.9.1 transaction timeout (PR #1, 2026-10-22):** after a timeout abort Kafka 3.x bumps the epoch by one and clears `lastProducerEpoch`, so retaking the transactional ID with the old epoch was fenced. `TransactionalCommitter#begin` retries a fenced retake with old epoch + 1, which 3.x accepts only if nothing but the timeout fence happened. 3.9.1 reproduces locally with `KSTREAMS_CACHE_DIR=<tmp> KAFKA_VERSION=3.9.1 KAFKA_PORT=9094 KAFKA_CONTROLLER_PORT=9095 npm run kafka -- start`.
- **"Exactly-once duplicates" in `scale-out.test.ts` (2502 vs 2480):** the test read outputs with the stock client consumer, which can deliver aborted records (#467). Integration reads now use the abort-aware `ReadCommittedConsumer` (`test/integration/kafka.ts`); no engine bug was found.
- **`map` then `groupByKey` aggregating wrong:** now fails when the app is built (no Repartition Topics in 0.1).
- **Zombie timer-driven writes:** every transaction commits offsets of each Task in it, so the group coordinator fences a Worker that lost the Task (commit 4efdc12).
- **READ_COMMITTED filtering (#467):** `src/read-committed.ts` sends Fetch as read_committed but filters each response with Java's stateless `CompletedFetch` algorithm; Restore and the Worker's input path use it.
- Reviews and their findings: `.scratch/mvp/review-findings-*.md` (Claude two-axis, Codex, ultrareview, Codex on the ultrareview fixes). Older detail is in this file's git history.

## Upstream

- platformatic/kafka#467 (filed; local copy `.scratch/upstream/platformatic-kafka-read-committed-aborted/issue.md`); fix in progress in **PR platformatic/kafka#470**. Once released: switch back to the client's filter, delete `src/read-committed.ts`, keep our read_committed tests and the crash suite as regression guards. A follow-up comment for #470 was drafted, not posted.
- Fetch-session bug, not filed: in 2.14, concurrent `fetch()` calls on one Consumer share one fetch session per node unserialized, so the broker rejects all but one with INVALID_FETCH_SESSION_EPOCH (71) (`consumer.js` L550–555, L665/L684–696, L640–643; likely from platformatic/kafka#315 / PR #321). Worked around with `fetchSessions: false`. Plan if pursued: fork, failing test + fix (serialize fetches per node), run against the local broker, show the user the diff before pushing.

## Resuming

- Research notes: `.scratch/mvp/research/` (`kafka-client.md` predates the read_committed findings; see its warning).
- Local broker: `npm run kafka -- start`; tests with `KSTREAMS_KAFKA_BROKERS=localhost:9092 npm run test:integration`; crash suite `npm run test:crash`; `npm run kafka -- stop` (`reset` clears leftover topics).
- Shared-broker rule for parallel agents: start if needed, never stop or reset.
- Usage rules: Sonnet for merges and mechanical tickets, Opus for hard ones, at most 2 implementers at a time.
- Leftover worktrees: `.claude/worktrees/agent-af7f0d92b0962fd7d` (abandoned, superseded files; `git worktree remove --force`) and harness-locked ones.

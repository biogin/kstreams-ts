# MVP implementation status

Integration branch: `mvp`. Plan (changed 2026-10-21): finish ALL remaining tickets before any release, then one code review over everything and a fix pass, then release prep (ticket 26) as 0.1 beta. The earlier M3 stop is dropped. Order: 24 → (after the aborted-transaction fix merges) 18, 30, 23 → 29 → 19 → 20 → 25 → 26 → code review.

Since the pause: tickets 28 and 17 merged; the crash suite found Changelog reads dropping committed records (client filter keyed by producer ID); a fix agent is replacing the client's READ_COMMITTED filter with our own Java-style per-response filter (branch `fix/aborted-txn-filter`).

## Merged (19 of 30)

01–16, 21, 22, 27. `mvp` passes typecheck, lint, 267 unit tests and the integration suite (with occasional flaky timeouts, see below).

## Remaining for M3

1. **28** Abort on revoke, commit on graceful shutdown: not started (the agent was stopped before saving work).
2. **17** Crash suite v1: started, stopped before committing. Its worktree `.claude/worktrees/agent-af7f0d92b0962fd7d` (branch `ticket/17`) may hold uncommitted partial work; check it or restart fresh from `mvp`.
3. Then: code review of `mvp`, one fix agent, worktree cleanup.

## Open items for the code review / fix pass

- **Flaky integration tests** under broker load: `at-least-once-crash.test.ts` (once read 0 records), `passthrough.test.ts` "leaves the group on stop and resumes from the committed offsets on restart" (timeout), plus unnamed one-off timeouts. Make waits condition-based and robust.
- **Input consumer and platformatic/kafka#467:** `consume()` with read_committed can deliver aborted records. Apply an aborted-transaction filter to the main input path, as Restore does.
- **`src/restore.ts` filter** carries state across fetch responses. Java's `CompletedFetch` is stateless per response (see the Root cause and Suggested fix sections of `.scratch/upstream/platformatic-kafka-read-committed-aborted/issue.md`); re-check and simplify to match.
- Ticket 29 later: transactional IDs are per Worker number, not per process (needs a stable instance identity).

## Not in this run (later session)

18, 19, 20, 23, 24, 25, 26, 29, 30. Ticket files contain notes from earlier tickets.

## Upstream

- platformatic/kafka#467 filed by the user. Its body was corrected before anyone responded: Java's handling is stateless per response, the suggested fix follows that, and two extra defects are listed. The local copy is `.scratch/upstream/platformatic-kafka-read-committed-aborted/issue.md`.

## Resuming

- Research notes the implementers relied on are in `.scratch/mvp/research/`. Point agents there.
- Local broker: `npm run kafka -- start`, tests with `KSTREAMS_KAFKA_BROKERS=localhost:9092 npm run test:integration`, then `npm run kafka -- stop`.
- Shared-broker rule for parallel agents: start if needed, never stop or reset.
- Usage rules: Sonnet for merges and mechanical tickets, Opus for hard ones, at most 2 implementers at a time.
- Old agent worktrees under `.claude/worktrees/agent-*` and `.claude/worktrees/ticket-22` (an empty leftover branch `ticket/22`) are all merged or empty and can be removed during cleanup.

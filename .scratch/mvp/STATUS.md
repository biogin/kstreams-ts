# MVP implementation status (paused 2026-10-21)

Integration branch: `mvp`. Plan for this run: stop at milestone M3, then code review.

## Merged (19 of 30)

01–16, 21, 22, 27. `mvp` passes typecheck, lint, 267 unit tests and the integration suite (with occasional flaky timeouts, see below).

## Remaining for M3

1. **28** Abort on revoke, commit on graceful shutdown: not started (the agent was stopped before saving work).
2. **17** Crash suite v1: started, stopped before committing. Its worktree `.claude/worktrees/agent-af7f0d92b0962fd7d` (branch `ticket/17`) may hold uncommitted partial work; check it or restart fresh from `mvp`.
3. Then: code review of `mvp`, one fix agent, worktree cleanup.

## Open items for the code review / fix pass

- **Flaky integration tests** under broker load: `at-least-once-crash.test.ts` (once read 0 records), `passthrough.test.ts` "leaves the group on stop and resumes from the committed offsets on restart" (timeout), plus unnamed one-off timeouts. Make waits condition-based and robust.
- **Input consumer and platformatic/kafka#467:** `consume()` with read_committed can deliver aborted records. Apply an aborted-transaction filter to the main input path, as Restore does.
- **`src/restore.ts` filter** carries state across fetch responses. Java's `CompletedFetch` is stateless per response (see `.scratch/upstream/.../correction-comment.md`); re-check and simplify to match.
- Ticket 29 later: transactional IDs are per Worker number, not per process (needs a stable instance identity).

## Not in this run (later session)

18, 19, 20, 23, 24, 25, 26, 29, 30. Ticket files contain notes from earlier tickets.

## Upstream

- platformatic/kafka#467 filed by the user. A correction comment (Java's handling is stateless per response; extra defects) is drafted in `.scratch/upstream/platformatic-kafka-read-committed-aborted/correction-comment.md` and **not yet posted**:
  `gh issue comment 467 --repo platformatic/kafka --body-file .scratch/upstream/platformatic-kafka-read-committed-aborted/correction-comment.md`

## Resuming

- Research notes the implementers relied on are in `.scratch/mvp/research/`. Point agents there.
- Local broker: `npm run kafka -- start`, tests with `KSTREAMS_KAFKA_BROKERS=localhost:9092 npm run test:integration`, then `npm run kafka -- stop`.
- Shared-broker rule for parallel agents: start if needed, never stop or reset.
- Usage rules: Sonnet for merges and mechanical tickets, Opus for hard ones, at most 2 implementers at a time.
- Old agent worktrees under `.claude/worktrees/agent-*` and `.claude/worktrees/ticket-22` (an empty leftover branch `ticket/22`) are all merged or empty and can be removed during cleanup.

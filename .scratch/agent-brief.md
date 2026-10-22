# Implementer brief

The checklist for anyone implementing a ticket or fix in kstreams-ts. A task prompt only needs the ticket, the hand-off notes, and anything that overrides this brief.

## Setup

1. Work in your own git worktree, on a new branch created from `mvp` (`git switch -c <branch> mvp`), then `npm ci`.
2. If you can't write to your own worktree (e.g. after a resume), stop and report; write nowhere else.

## While working

- **Test-first at public seams** (Test Driver, real Kafka, State Store contract suite); internals only as ADR-0016 allows. A bug fix starts with a failing test that reproduces it.
- **Broker**: one shared local Kafka (`npm run kafka -- status|start|stop`). Start it only if it isn't running; stop it only if you started it and nobody else is using it; never reset it. Use unique topic names and Application IDs; stateful test apps set `changelogReplicationFactor: 1`; waits are condition-based, never fixed sleeps.
- **TypeScript**: `erasableSyntaxOnly` (no enums, namespaces or parameter properties). Use `CONTEXT.md` terms.
- **Output discipline**: pipe test and build output through `tail` or `grep`; never paste whole logs or files into your context.
- **Commit early**: one commit per logical item, WIP commits during long work, trailer `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- **Two-strikes rule**: if the same approach fails twice, stop and report what you know instead of trying again.
- Stay in scope: no unrelated refactors; note follow-ups for later tickets instead.

## Done means

All of these pass: `npm run typecheck && npm run lint && npm test && npm run build`; `KSTREAMS_KAFKA_BROKERS=localhost:9092 npm run test:integration`; and for engine changes `npm run test:crash` at defaults. A flaky failure is reported by test name and error line, not dismissed.

## Report

At most 10 lines: branch, commit SHAs, what changed per item, test evidence, concerns for later tickets. Tick the ticket's acceptance boxes and append a `## Comments` note. Don't merge or push.

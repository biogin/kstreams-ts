# Review brief: the ultrareview fix commits

Review the kstreams-ts repo **read-only**: no edits, commits or pushes.

## Scope

Only the commits that fixed the ultrareview findings: `git diff 802bf92..HEAD`. Focus on `src/`; read tests only to judge whether they prove a fix.

## Sources

- The findings these commits fix: `.scratch/mvp/review-findings-ultrareview.md` (items 1–9).
- Review rules: `CODING_STANDARDS.md`. Terms: `CONTEXT.md`. Decisions: `docs/adr/`.

## What to check

For each ultrareview item 1–9: is the fix correct and complete, and could it introduce a new bug? Pay special attention to:

- transaction-timeout handling vs genuine zombie fencing (`src/worker.ts`: `INVALID_PRODUCER_EPOCH` handling, the batch time budget);
- process liveness across PID namespaces via `.alive` lock files (`src/state-directory.ts`);
- send splitting and produced-offset bookkeeping (`maxProduceBytes`);
- the store-key round-trip rule, and `clear()` with Checkpoint invalidation.

Don't run integration tests or touch the Kafka broker. Unit tests (`npm test`) are fine if the sandbox allows.

## Output

Your final message is a findings document with these sections:

1. `## Per item`: a verdict per ultrareview item 1–9 (correct / incomplete / introduces bug), with file:line evidence.
2. `## New findings`: most severe first, each with severity (P1/P2/P3), file:line, a concrete triggering scenario and a suggested fix.
3. `## Summary`: one line.

Be specific, and say when something is a guess.

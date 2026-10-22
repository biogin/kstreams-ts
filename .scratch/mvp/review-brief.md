# Code review brief: kstreams-ts `mvp` branch

You are reviewing a TypeScript library for stateful, exactly-once stream processing on Kafka for Node.js, built on `@platformatic/kafka` with SQLite State Stores. Work **read-only** on the code: don't modify source files, don't commit, don't push. Your only output is the findings file described at the end.

## What to review

- Branch `mvp` against fixed point `main`. Diff: `git diff main...mvp`. Commits: `git log main..mvp --oneline` (~100 commits, ~170 files).
- The diff is large. Prioritise `src/` (the engine), then `test/` helpers and the crash suite (`test/crash/`), then `examples/` and `docs/` only for contradictions with the code.

## Sources of truth

- **Spec:** `.scratch/mvp/spec.md` (user stories, implementation decisions, defaults, testing decisions, out of scope).
- **Tickets with acceptance criteria and implementer notes:** `.scratch/mvp/issues/01–30` (the `## Comments` sections describe what was actually built and known gaps).
- **Glossary:** `CONTEXT.md`. Code and docs should use its terms and avoid its `_Avoid_` words.
- **Decisions:** `docs/adr/0001–0015`. Flag code that contradicts an ADR.
- **Current state and known open items:** `.scratch/mvp/STATUS.md`.
- **Tooling-enforced, skip:** Biome lint/format, `tsc` strict mode with `erasableSyntaxOnly` (no enums, namespaces or parameter properties).

## Two separate axes (keep them separate, don't merge or re-rank across them)

### Axis 1: Standards

Report (a) every place the code violates a documented standard (cite the file and rule: glossary, ADR, spec's testing decisions such as "tests drive public seams only" and "no mocking the Kafka client"), and (b) code smells, always as judgement calls: Mysterious Name, Duplicated Code, Feature Envy, Data Clumps, Primitive Obsession, Repeated Switches, Shotgun Surgery, Divergent Change, Speculative Generality, Message Chains, Middle Man, Refused Bequest. `src/worker.ts` has grown large across many tickets; assess Divergent Change there specifically.

### Axis 2: Spec

Report (a) requirements the spec asked for that are missing or partial, (b) behaviour that wasn't asked for, (c) requirements that look implemented but where the implementation looks wrong. Quote the spec or ticket line for each finding.

## Known open items: confirm or refute each, with evidence

1. **Possible exactly-once duplicates (top priority):** `test/integration/scale-out.test.ts` once received 2502 records vs 2480 expected. Check how that test reads outputs. If it uses the stock `@platformatic/kafka` consumer with read_committed, it can see aborted records (upstream bug platformatic/kafka#467), so the "duplicates" may be a test artefact; the project's own abort-aware reader is in `src/read-committed.ts` and the crash suite has an independent one in `test/crash/log-reader.ts`. If the test is sound, look for a real engine path that can commit duplicates during multi-process rebalances (`src/worker.ts`, `src/ownership.ts`, `src/task-assignor.ts`, `src/stable-offsets.ts`).
2. **Silent wrong aggregation:** `map` that changes the key, followed by `groupByKey`, doesn't repartition (Repartition Topics are out of scope for 0.1). Confirm, and say where a build-time error should be raised.
3. **Zombie timer writes:** a zombie Worker whose open transaction holds only timer-driven Changelog writes could commit them after the new owner has restored. Assess whether this is real and how likely.
4. **Unverified doc claims:** the transaction-timeout wording and "timers don't persist across restarts" in `docs/`. Check them against the code.
5. **Dependency on client internals:** `src/read-committed.ts` hooks `kGetApi` and the Fetch API argument order, while `package.json` has `@platformatic/kafka` as a caret range. Assess the risk.
6. **Flaky integration tests** listed in `STATUS.md`: look for fixed sleeps, too-short timeouts, or shared-state assumptions that explain them.

## Running things (optional)

- Unit tests, no broker: `npm ci && npm run typecheck && npm run lint && npm test`.
- Integration tests need a local Kafka (no Docker needed): `npm run kafka -- start`, then `KSTREAMS_KAFKA_BROKERS=localhost:9092 npm run test:integration`, then `npm run kafka -- stop`. Requires Java 17 at `/usr/local/opt/openjdk`; Kafka 4.4.0 is cached in `~/.cache/kstreams-ts/`.
- Don't run `npm run kafka -- reset`.

## Output

Write your findings to `.scratch/mvp/review-findings-<your-model-name>.md` with exactly these sections:

1. `## Standards`: findings, most important first. For each: file and line(s), the standard or smell, a short quote, hard violation vs judgement call, suggested fix.
2. `## Spec`: findings, most important first. For each: the spec or ticket line quoted, file and line(s), what's missing, extra or wrong, suggested fix.
3. `## Open items`: one verdict per item 1–6 above (confirmed / refuted / unclear), with evidence.
4. `## Summary`: one line per axis with the count of findings and the worst issue within that axis.

Be specific and evidence-based: cite file paths and line numbers, and say when something is a guess. Don't pad with style nits that tooling would catch.

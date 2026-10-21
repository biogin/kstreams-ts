# 21: Event Time, Stream Time, timers

**What to build:** Processors can schedule timers on Stream Time or wall-clock time, Stream Time per Task only moves forward, and output records inherit the input record's Event Time. The Test Driver can advance wall-clock time.

**Blocked by:** 06

**Status:** done

- [x] Stream Time per Task. `ctx.schedule` (Stream Time and wall-clock)
- [x] Output timestamp = input Event Time
- [x] Test Driver: pipe with timestamps, advance wall-clock time

Spec: [../spec.md](../spec.md). Glossary: [CONTEXT.md](../../../CONTEXT.md). ADRs: [docs/adr/](../../../docs/adr/)

## Comments

- 2026-10-21: Done on branch `ticket/21`. Public: `ctx.schedule(intervalMs, type, callback)` now returns a `Timer` (`cancel()`); `callback: TimerCallback = (timestamp) => void | Promise<void>` gets the Stream Time (stream-time) or wall-clock time it fired at. New exported types `Timer`, `TimerCallback`. Test Driver: `driver.advanceWallClockTime(ms): Promise<void>` (wall-clock starts at 0, never moves back; records piped without a timestamp take the current wall-clock time). Tests in `test/time.test.ts`.
- Semantics: Stream Time per Task = highest Event Time seen, unknown until the first record, never moves back (out-of-order records don't rewind it). A timer fires every `intervalMs` after it was scheduled; a Stream Time timer scheduled before any record (e.g. in the supplier) starts counting from the first record's Event Time. If time jumps several intervals the timer fires **once** with the current time, and the next due time stays on the original grid (Kafka Streams' punctuation behaviour). Stream Time timers fire after the record that advanced Stream Time is fully processed (async Processors awaited). Timers fire earliest-due first, one after another, each awaited (async callbacks and what they forward included) — the same per-Task serialization as records. A throwing timer fails the Task like a failed record (fail-fast). Forwarded records need an explicit timestamp (usually the callback's `timestamp`). Output records already inherited the input's Event Time (DSL operators and Processors forwarding the record); now covered by a test.
- Runtime seam (internal, for ticket 07): `createTask(graph, collect, wallClockTime = Date.now)`. Stream Time timers need nothing from the runtime: `task.process` advances Stream Time and fires them. Wall-clock timers: the runtime's loop calls `task.fireWallClockTimers()` periodically (e.g. once per poll/commit cycle); it is queued behind records already given to `process`, reads `wallClockTime()` when it runs, and has the same rejection/fail-fast semantics. Test Driver passes `() => wallClockTime` and calls it from `advanceWallClockTime`.
- For later tickets: Stream Time lives only in memory and restarts unknown after a restart/rebalance (Kafka Streams also recomputes it); ticket 22 (windows, Grace Period) will need Stream Time inside its window Processor — expose it internally from the Task rather than adding public API. Ticket 10 also edits `createContext` in `src/task.ts` and the "features that arrive later" test in `test/processor-api.test.ts` (only `ctx.store` remains there) — expect small merge conflicts.

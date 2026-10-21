# 30: Window retention and windows on Kafka

**What to build:** Window state is kept for window size plus Grace Period, then dropped and tombstoned in the Changelog. Windowed counts work end to end on real Kafka, including after Restore.

**Blocked by:** 22, 27

**Status:** ready-for-agent

- [x] Retention = window size + Grace Period, then entries are dropped and tombstoned
- [x] Integration test: windowed `count()` on Kafka, including after a restart

Split from the original ticket 22.

Spec: [../spec.md](../spec.md). Glossary: [CONTEXT.md](../../../CONTEXT.md). ADRs: [docs/adr/](../../../docs/adr/)

## Notes from ticket 22

- Export a windowed key Serde (e.g. `serdes.windowed(inner, size)`) so consumers outside the app can read windowed Table topics; the format is key Serde bytes + 8-byte window start.
- Window store keys are time-first (`[windowStart, key]`), so retention cleanup is one range scan up to the cutoff.

## Comments

- 2026-10-21: Done on branch `ticket/30`. Retention: a window is kept until Stream Time *passes* its end plus the Grace Period (`end + grace < streamTime`; at `end + grace == streamTime` it is closed to late records but still in the store, so the closing record's view still shows it). Then its entries are deleted through the typed store view, which writes their Tombstones to the Changelog; nothing is forwarded downstream (as Kafka Streams). Window store keys are time-first, so one range scan `[undefined, encodeKey([firstKeptWindowStart]))` finds every expired window, however far Stream Time jumped.
- When cleanup runs: in the windowed aggregation Processor, before each record (Tombstones precede that record's own update in the Changelog), but only when Stream Time has made at least one more window expire since the last look, i.e. at most one range scan per window size of Stream Time per Task and store. The first record after a start always looks, so a store restored with stale windows is cleaned then. Restore applies the retention Tombstones like any other. Code: `byWindow` / `TaskKeying.expire` in `src/streams-app.ts`, `firstKeptWindowStart` in `src/windows.ts`, internal `rangeByStoreKey(view, from, to)` in `src/state-store.ts` (raw store-key range on a typed view).
- Public: `serdes.windowed(keySerde, sizeOrWindows)` → `Serde<Windowed<K>>` (key Serde bytes + 8-byte big-endian window start; takes a `Duration` or the `TumblingWindows` themselves), for reading windowed Table topics outside the app.
- Tests: `test/windows.test.ts` (retention in memory and SQLite, jumps, no forwarding on expiry, `serdes.windowed`); one existing test moved its last record from 61m to 60m since `[0, 1h)` now expires at 61m. `test/integration/windows.test.ts`: windowed `count()` on Kafka over three starts with in-memory stores (full Restore), checking output via `serdes.windowed`, store contents, the exact Changelog (Tombstones for expired windows), and that Restore applied them (no Tombstones re-written after the restart).
- Not done: retention is not configurable (spec default only); nothing expires without new records (wall-clock time doesn't advance Stream Time).

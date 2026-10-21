# 22: Tumbling windows in the Test Driver

**What to build:** A user writes `groupByKey().window(tumbling('1m', { grace: '10s' })).count()`. Late records beyond the Grace Period are dropped and counted, and window results carry the window end as their timestamp. Verified through the Test Driver.

**Blocked by:** 12, 21

**Status:** done

- [x] Window store built on range scans
- [x] Grace Period; late records dropped and counted
- [x] Window end used as the result timestamp
- [x] Test Driver tests

Split from the original ticket 22; retention and the Kafka integration test are ticket 30.

Spec: [../spec.md](../spec.md). Glossary: [CONTEXT.md](../../../CONTEXT.md). ADRs: [docs/adr/](../../../docs/adr/)

## Comments

- 2026-10-21: Done on branch `ticket/22-windows`. Public: `tumbling(size, { grace })` (sizes and Grace Periods as ms or `'500ms' | '10s' | '1m' | '2h' | '1d'`; grace defaults to 0; validated when called), `GroupedStream.window(windows)` → `WindowedStream<K, V>` with `count` / `aggregate` / `reduce` returning `Table<Windowed<K>, V>`, where `Windowed<K> = { key, window: { start, end } }` (end excluded). New exported types `Duration`, `TumblingOptions`, `TumblingWindows`, `Window`, `Windowed`, `WindowedStream`, `TestDriverMetrics`. Test Driver: `driver.metrics().lateRecordsDropped`. Tests in `test/windows.test.ts` (in memory, and SQLite via `stateDir`).
- Semantics (as Kafka Streams): window `[start, start + size)` aligned to the epoch; a record is late, dropped and counted when `windowEnd + grace <= Stream Time` (Stream Time includes the record itself). Each windowed aggregation that drops a record counts it once. Window results are forwarded with the window end as their timestamp. Null keys and tombstones are skipped, not counted as late.
- Implementation: windowed and plain aggregations share one Processor (`AggregationNode` plus a `Keying` strategy in `src/streams-app.ts`). Window store keys are time-first, `encodeKey([windowStart, key])` (ADR-0015), behind the existing typed store view through a new internal `KeyEncoding` on `StoreNode` (`keyValueStore(store, serdes, context, encoding?)`). Stream Time and the late counter reach the DSL Processor through an internal `taskInternals(ctx)` WeakMap in `src/task.ts`, not public API. On topics, windowed keys use an internal Serde: key Serde bytes + 8-byte big-endian window start (the window store's `serdes.key` is that Serde too).
- For later tickets: ticket 30 (retention) can drop everything before `streamTime - size - grace` with one range scan `[undefined, encodeKey([cutoff]))` on the window store; nothing deletes window entries yet, so window stores grow until then. Ticket 13 (Changelog) should write window-store keys with the store's `serdes.key` (the windowed Serde), and Restore must re-encode them with `StoreNode.keyEncoding`, not `storeKey`. Ticket 25 should fold `Task.metrics().lateRecordsDropped` into the metrics interface. The windowed key Serde isn't exported, so a consumer outside the app reading a windowed Table's topic decodes it by hand (consider `serdes.windowed(inner, size)`).

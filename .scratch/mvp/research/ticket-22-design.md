# Ticket 22 design plan (from a prior agent's reading; not yet implemented)

- API: `groupByKey().window(tumbling('1m', { grace: '10s' }))` → `WindowedStream` with `count`, `aggregate`, `reduce`, built on the shared aggregation Processor. Results are `Table<Windowed<K>, V>` where `Windowed<K> = { key, window: { start, end } }`. Grace defaults to 0.
- Window store layout: time-first, `encodeKey([windowStart, keyPart])` behind the existing typed store view. Ticket 30's retention cleanup is then one range scan of everything before the cutoff, with no format change; a window update is a direct lookup.
- Stream Time: read via an internal WeakMap on the Processor context (e.g. `taskInternals(ctx)`), not public API.
- Late-record counter: expose through the Test Driver (e.g. `TestDriver.metrics().lateRecordsDropped`) so ticket 25 can fold it into the metrics interface.
- Window results: timestamped with the window end; written with a windowed key Serde (key bytes + 8-byte window start).
- Tests: include a Test Driver test with `stateDir` (SQLite path) for `aggregate`/`reduce`/windowed count.

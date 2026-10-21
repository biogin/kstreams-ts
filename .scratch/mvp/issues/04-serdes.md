# 04: Serdes

**What to build:** A user can use built-in JSON, number and raw-bytes Serdes or plug in their own through the typed Serde interface. Null values are tombstones throughout, and a malformed payload raises a typed deserialization error.

**Blocked by:** 02

**Status:** done

- [x] `serdes.json<T>()`, `serdes.number`, `serdes.bytes` alongside `serdes.string`
- [x] Custom Serde interface is public and documented in the type definitions
- [x] Null value ↔ tombstone round-trips through the Test Driver
- [x] Malformed JSON raises a typed deserialization error. With no handlers yet (ticket 23), the app stops

Spec: [../spec.md](../spec.md). Glossary: [CONTEXT.md](../../../CONTEXT.md). ADRs: [docs/adr/](../../../docs/adr/)

## Comments

- 2026-10-21: Done on branch `ticket/04`. Public: `serdes.json<T>()` (UTF-8 JSON, strict UTF-8 decoding, throws `TypeError` for values JSON can't represent), `serdes.number` (8-byte big-endian double, same wire format as Kafka's DoubleSerde), `serdes.bytes` (identity), `DeserializationError` (`topic`, `part: 'key' | 'value'`, `timestamp`, `cause`), types `SerdeContext`.
  - **Decision: Serdes receive the topic.** `serialize(value, context)` / `deserialize(bytes, context)` with `context: SerdeContext = { topic }`. A context object (not a bare string) so it can grow later (headers, key/value) without breaking implementers, who may ignore it. Needed for Schema Registry subjects. Code that delegates to another Serde passes the context through.
  - **Null handling: the runtime, not the Serde.** A null key or value never reaches a Serde; it's carried as null bytes, and a null value is a tombstone (internal helpers `serializeNullable`/`deserializeNullable` in `src/serde.ts`; `SerializedRecord` is now `StreamRecord<Uint8Array | null, Uint8Array | null>`). The Test Driver's `pipe` takes `K | null, V | null` and `read`/`readAll` return `StreamRecord<K | null, V | null>`. DSL types (`Stream<K, V>`) were deliberately left unchanged: at runtime a tombstone flows through operators as `value: null` even though it's typed `V`. Ticket 03 (operators) and 11 (tables) must decide how tombstones are typed and treated by `filter`/`map`/aggregations.
  - **Deserialization errors stop the app.** `Task.process` wraps any Serde throw in a `DeserializationError`. The Test Driver rethrows it from `pipe` and then refuses every later `pipe` ("stopped after an earlier error", with the original as `cause`). This applies to any error escaping a record, so processing errors already fail fast; ticket 23 hooks handlers into `Task.process`.
  - Test Driver: `input(topic, serdes?)` / `output(topic, serdes?)` take optional per-test Serdes overrides (used to pipe malformed bytes or read raw bytes).

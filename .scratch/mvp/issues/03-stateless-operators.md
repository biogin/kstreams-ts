# 03: Stateless operators with typed chains

**What to build:** A user chains `filter`, `map` and `peek` on a Stream, with key and value types inferred through the whole chain, including `map` changing both, and sees the results in the Test Driver.

**Blocked by:** 02

**Status:** done

- [x] `filter`, `map` (may change key and value types), `peek`
- [x] Type inference tests (type-level assertions) show types flow through the chain with no annotations
- [x] Behavior tested through the Test Driver

Spec: [../spec.md](../spec.md). Glossary: [CONTEXT.md](../../../CONTEXT.md). ADRs: [docs/adr/](../../../docs/adr/)

## Comments

- 2026-10-21: Done on branch `ticket/03`. Public: `stream.filter((key, value) => boolean)` (a type-guard predicate narrows the value type), `stream.map((key, value) => ({ key, value }))` (may change both types; keeps the record's timestamp), `stream.peek((key, value) => void)`. Each compiles to one Processor node in the graph (`src/streams-app.ts`). `filter` and `peek` keep the Stream's inherited Serdes; `map` drops them, so `.to()` after `map` must be given both Serdes, otherwise it throws when the app is defined. That requirement is only checked at runtime: `to()`'s `serdes` argument stays optional at the type level. Type-level tests use Vitest's `expectTypeOf` and `@ts-expect-error`, checked by `npm run typecheck` (`test/stateless-operators.test.ts`). Synchronous-only enforcement for these callbacks (ADR-0012) is left to ticket 05.

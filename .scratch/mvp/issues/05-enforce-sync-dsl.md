# 05: Enforcing synchronous DSL functions

**What to build:** A user who passes an async function to a DSL operator is stopped: at compile time with a message pointing to Processors, when the app is built for `async` functions, and on the first record for any function returning a thenable. See ADR-0012.

**Blocked by:** 03

**Status:** ready-for-agent

- [x] Type-level test: async callback to `filter`/`map`/`peek` fails to compile with the named message
- [x] Defining an app with an `AsyncFunction` throws when the app is built
- [x] A plain function returning a Promise throws `AsyncNotAllowedError` on the first record. No unhandled rejection is left behind
- [x] `AsyncNotAllowedError` is marked to skip error handlers (wiring verified in ticket 23)

Spec: [../spec.md](../spec.md). Glossary: [CONTEXT.md](../../../CONTEXT.md). ADRs: [docs/adr/](../../../docs/adr/)

## Comments

- Done on `ticket/05`. All three layers live in `src/sync-dsl.ts`. `Sync<R>` and `SyncKeyValue<R>` turn a `PromiseLike` return type into a `'DSL functions must be synchronous; use a Processor for async work'` type error. `synchronous(operator, fn)` rejects an `AsyncFunction`/`AsyncGeneratorFunction` when the operator is called (that is, when the app is defined). It also wraps `fn` so that a thenable result gets a no-op `.catch` and then throws `AsyncNotAllowedError`. `StreamNode` wraps each predicate, mapper and action with it. Future DSL operators (`aggregate`, `reduce`, ...) should do the same.
- Signatures changed: `filter`, `map` and `peek` now infer the callback's return type `R`. `map<R>` returns `Stream<KeyOf<R>, ValueOf<R>>` where it used to be `map<KOut, VOut>`. `peek` accepts any synchronous return value, not only `void`.
- `AsyncNotAllowedError` is exported from the package entry point. It is thrown both when the app is defined and per record. The "skips the error handlers" mark is the internal `skipsErrorHandlers(error)` predicate in `src/sync-dsl.ts`. Ticket 23 should route errors through it, and can extend it to produce errors (ADR-0010).
- The type-level test checks the message text for `map` and `peek` with instantiation expressions. For `filter`, the type-guard overload blocks that, so `filter` is covered by `@ts-expect-error` only.

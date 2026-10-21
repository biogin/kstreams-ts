# 05: Stateless DSL with enforced synchronous functions

Status: ready-for-agent
Milestone: M1
Blocked by: 04

`createStreamsApp(config)` and the stateless DSL: `stream`, `filter`, `map`, `peek`, `to`. These compile down to the Processor API (ADR-0001).

## Acceptance

- Key and value types are inferred through the whole chain, and `map` can change both.
- Synchronous enforcement per ADR-0012, with a test for each layer:
  1. A type-level test (e.g. `expectTypeOf` / `@ts-expect-error`) showing that an async callback fails to compile with the named error message.
  2. Defining an app with an `async` function throws when the app is built.
  3. A plain function that returns a Promise throws `AsyncNotAllowedError` on the first record. That error skips the error handlers, and the stray Promise doesn't cause an unhandled rejection.
- All behavior is tested through the Test Driver (04).

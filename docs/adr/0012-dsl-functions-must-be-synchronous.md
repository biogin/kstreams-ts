# DSL functions must be synchronous, enforced at three layers

Functions passed to the DSL (`filter`, `map`, `aggregate`, ...) must be synchronous. A Promise accepted silently would either be treated as a value (`filter` sees a truthy Promise, `map` sends a Promise downstream) or break per-key ordering, and in both cases the data is quietly wrong. Async work belongs in Processors, which are awaited one after another per Task. We enforce this at three layers, because types alone don't protect JavaScript users or `any` casts:

1. **Types**: callback return types are constrained so that a `PromiseLike` result becomes a named error type ("DSL functions must be synchronous; use a Processor for async work") and fails to compile.
2. **When the app is built**: functions declared `async` (`AsyncFunction`/`AsyncGeneratorFunction`) are rejected when the app is defined, before it ever connects to Kafka.
3. **Per record at runtime**: a thenable result throws `AsyncNotAllowedError`. This catches plain functions that return Promises and code transpiled from async. The stray Promise gets a no-op `.catch` so it doesn't also show up as an unhandled rejection.

`AsyncNotAllowedError` **skips the error handlers** (ADR-0010) and always stops the application. It's a code bug, not a bad record. Sending it to a `deadLetter` handler would quietly put every record in the dead-letter topic.

## Consequences

- The runtime check is one `typeof result?.then` test per call, so the cost is negligible.
- Users who need I/O (HTTP enrichment, lookups) must drop down to a Processor, and the error message points them there.

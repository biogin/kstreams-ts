# Type-level tests with TypeScript 7.0 (native) + Vitest 5

Scratch project: `/Users/igorshalygin/.claude/jobs/e36bc646/tmp/explore/ts-testing/` with
`typescript@7.0.2`, `vitest@5.0.5`, `@types/node@24`, and the repo's exact `tsconfig.json`
(NodeNext, strict, exactOptionalPropertyTypes, verbatimModuleSyntax, `.js` import specifiers).
Everything below is **verified** there unless marked [U].

## What TS 7 is (relevant facts)
- `typescript@7.0.2` = the Go-native compiler. The npm package is a thin launcher: `bin/tsc` +
  `lib/tsc.js` picks the platform binary (`@typescript/typescript-darwin-arm64` etc., optional deps).
- **No classic JS compiler API** (`require('typescript')` only exports version info; the programmatic
  API lives under `typescript/unstable/*`). So tools that `import ts from 'typescript'` to typecheck
  in-process (`tsd`, `ts-morph`) won't work [U]. Tools that **spawn the `tsc` CLI** (Vitest typecheck)
  and pure-type libraries (`expectTypeOf`, built on expect-type) do work.
- `tsc --noEmit` on the scratch project: ~0.4 s.

## Options tested
| approach | works with TS 7? | notes |
|---|---|---|
| `expectTypeOf` / `assertType` from `vitest` in `*.test.ts`, checked by `tsc --noEmit` | **yes** | pure types (expect-type); at runtime they're no-ops. Wrong type → TS2344 with readable "Expected…/Actual…" message. |
| `// @ts-expect-error <reason>` checked by `tsc --noEmit` | **yes** | an unused directive → TS2578 "Unused '@ts-expect-error' directive" — i.e. the negative test fails if the code starts compiling. |
| Vitest typecheck mode (`test.typecheck`, `*.test-d.ts`) | **yes** | Vitest spawns `tsc --noEmit --incremental --tsBuildInfoFile …` against a temp tsconfig and maps errors to tests. Prints "Testing types with tsc and vue-tsc is an experimental feature". Both failure kinds above are reported per test (`TypeCheckError`), exit code 1. |
| `tsd` / tools using the TS JS API in-process | no [U, not installed — inferred from the missing API] | |

## Recommended pattern (matches what the repo already does)
The repo already has this: `test/sync-dsl.test.ts` (`@ts-expect-error DSL functions must be synchronous`),
`test/state-stores.test.ts` (`expectTypeOf(...).toEqualTypeOf<...>()`), and `npm run typecheck`
= `tsc --noEmit` whose `include` covers `test/`. Keep it:

1. Write type assertions **next to the runtime tests** in `test/**/*.test.ts`.
2. **`npm run typecheck` is the gate** for type tests (CI: `npm run typecheck && npm test`).
   `vitest run` alone does *not* check types (esbuild/oxc strips them) — a broken type test passes
   `npm test`.
3. Negative tests: `// @ts-expect-error <why>` directly above the offending expression. Always write a
   reason. Prefer putting it on an expression that also runs (as the repo does:
   `expect(() => stream.filter(async () => true)).toThrow(...)`), so the runtime guard is tested too.
4. Positive tests: `expectTypeOf(value).toEqualTypeOf<T>()`, `expectTypeOf(fn).returns...`,
   `expectTypeOf(fn).parameter(0)...`, or the value-free form `expectTypeOf<T>().toEqualTypeOf<U>()`.
   Use `toEqualTypeOf` (exact) over `toMatchTypeOf`/`toExtend` unless you mean subtyping.
5. If a type-only check needs a value you don't want to construct, use the value-free generic form,
   or `declare const x: Foo` **inside a block that never runs**:
   ```ts
   import { assertType, expectTypeOf } from 'vitest'
   declare const s: Stream<string, number>   // erased; `s` is undefined at runtime
   if (false as boolean) {
     expectTypeOf(s.mapValues((v) => String(v))).toEqualTypeOf<Stream<string, string>>()
     // @ts-expect-error DSL functions must be synchronous
     s.mapValues(async (v) => v)
     assertType<Stream<string, number>>(s.filter((v) => v > 1))
   }
   ```
6. Optional (only if per-test reporting of type failures is wanted): dedicated `test/**/*.test-d.ts`
   files + vitest typecheck mode. Verified config:
   ```ts
   // vitest.config.ts
   export default defineConfig({ test: {
     include: ['test/**/*.test.ts'],
     typecheck: { enabled: true, include: ['test/**/*.test-d.ts'], tsconfig: './tsconfig.json' },
   } })
   ```
   `*.test-d.ts` files are never executed, so `declare const` is fine at top level there. It adds
   ~0.2 s and an "experimental" banner; `tsc --noEmit` already covers the same files if they're in
   the tsconfig `include`. Not needed for v1 — plain `tsc --noEmit` is simpler.

## Example failure output (tsc 7)
```
broken/broken.test-d.ts(7,63): error TS2344: Type 'Stream<string, number>' does not satisfy the constraint
  '{ filter: "Expected: function, Actual: function"; mapValues: ... }'.
broken/broken.test-d.ts(10,5): error TS2578: Unused '@ts-expect-error' directive.
```
Note the expect-type message for mismatched *generic interfaces* is vague ("Expected: function, Actual:
function") — when a toEqualTypeOf fails, narrow it (`expectTypeOf(x).toHaveProperty('k')...` or compare
the generic args) to see what differs.

## Gotchas
- Type tests depend on the project's strict flags (`exactOptionalPropertyTypes`,
  `noUncheckedIndexedAccess`) — write them against the repo tsconfig, not a looser one.
- A `@ts-expect-error` swallows *any* error on the next line; keep the expression minimal so it can't
  pass for the wrong reason (e.g. a typo). Pair with `expectTypeOf` positive tests of the valid form.
- Biome lint may flag `if (false as boolean)`/unused vars in type-only blocks — the `.test-d.ts` route
  avoids that.

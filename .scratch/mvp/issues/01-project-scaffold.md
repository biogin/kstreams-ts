# 01: Project scaffold

Status: ready-for-agent
Milestone: M1

Set up the package according to the spec's Platform section.

## Acceptance

- `package.json`: ESM-only (`"type": "module"`), `engines.node >=24`, Apache-2.0, `exports` map with a single entry point.
- `tsconfig.json`: strict, `NodeNext` module resolution, emits declarations.
- Test runner (Vitest) with one passing smoke test. Typecheck, lint and test scripts.
- `LICENSE` (Apache-2.0) and a minimal `README.md` that links to `CONTEXT.md` and the spec.
- `@platformatic/kafka` and `better-sqlite3` are not installed yet. Each is added by the issue that first needs it.

## Comments

- 2026-10-21: Done on branch `issue-01-scaffold`. TypeScript 7.0, Vitest 5, Biome 2.5 (lint + format), `@types/node@24`. The package is `"private": true` until the 0.1 release (M6). `typecheck`, `lint`, `test` and `build` all pass.

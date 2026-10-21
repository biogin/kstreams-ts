# 07: M1 integration test against real Kafka

Status: ready-for-agent
Milestone: M1
Blocked by: 06

The M1 demo, proven end to end.

## Acceptance

- Testcontainers starts a single Kafka broker (the latest 3.9 and 4.x, per the spec's Defaults).
- Test: produce N records to `orders` → a Streams App with `filter`/`map`/`to` → assert that every expected record arrives in the output topic. Duplicates are allowed, because the guarantee is at-least-once.
- Test: kill the app mid-run and restart it → nothing is missing from the output.
- The test is runnable with one script (e.g. `npm run test:integration`) and documented in the README.

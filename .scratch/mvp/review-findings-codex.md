## Standards

1. **Hard violation — revoke handling contradicts ADR-0006.**

   **Rule:** `docs/adr/0006-abort-on-revoke.md:3`: “the Worker aborts the open Kafka transaction and rolls back the matching SQLite transaction, without trying to commit first.”

   **Locations:** `src/worker.ts:1057–1080,1322–1332,1341–1357`.

   Revocations are sampled before awaiting processing. A partition revoked and reassigned during that wait passes the subsequent current-ownership check. Separately, timer-only transactions bypass offset-based ownership checks entirely. Both paths can commit work from a lost ownership period; see Spec findings 1–2.

   **Fix:** Track uninterrupted ownership of every Task contributing to a transaction, including timers, and abort when that ownership ends. Revalidate after asynchronous work before submitting transactional offsets.

2. **Hard violation — ownership tests fabricate Kafka client behavior.**

   **Rule:** `.scratch/mvp/spec.md:186`: “No mocking of the Kafka client anywhere.”

   **Locations:** `test/ownership.test.ts:6–27,37–50`; consumer-shaped interface at `src/ownership.ts:16–21`.

   `FakeMember` manufactures assignments, generations and `consumer:group:join`/`consumer:group:rebalance` events. These tests validate the assumed event protocol rather than the actual client’s behavior.

   **Fix:** Establish ownership guarantees through public Streams App rebalance tests against Kafka. Ticket 29 explicitly mentions these unit tests at `.scratch/mvp/issues/29-scale-out-processes.md:36`; reconcile that implementation choice with the unchanged testing prohibition.

3. **Hard violation — tests depend on internal engine seams and implementation observations.**

   **Rule:** `.scratch/mvp/spec.md:182`: tests “drive[] the system only through public surfaces” and never check “how the engine is structured inside.”

   **Locations and evidence:**

   - `test/checkpoints.test.ts:82–101,162–163`: constructs internal Tasks and manually supplies commits/checkpoints; asserts reader calls.
   - `test/restore.test.ts:12–43,61–64`: constructs internal readers/store nodes and checks `reader.reads`.
   - `test/ownership.test.ts:39–50`: checks internal ownership state and callback counts.
   - `test/task-assignor.test.ts:21–40`: directly exercises internal assignment.
   - `test/read-committed.test.ts:77–89`: checks internal filtered batch structures.
   - `test/state-directory.test.ts:35–55,68–78`: exercises internal locking and rewrites its private representation.
   - `test/store-key.test.ts:6,23–26,82–101`: exercises internal encoding functions.
   - `test/integration/restore.test.ts:243,260,314–332`: counts store-method invocations, including `expect(second.run.writes).toBe(2)`; lines 384–400 directly exercise the internal reader.
   - Internal store/reader imports also appear in `test/integration/read-committed.test.ts:4`, `windows.test.ts:11–12`, `exactly-once-state.test.ts:5–8`, `non-blocking-restore.test.ts:21–22`, `changelog.test.ts:11–12`, and `rebalance.test.ts:16–17`.
   - `test/crash/crash-run.ts:283–305,329`: calls the production Restore reader and internal typed-store wrapper.

   **Fix:** Move behavioral assertions to public outputs, stores, events and metrics. Alternatively, explicitly document exceptions for direct module tests and disk-format inspection. These tests can be useful, but currently contradict the policy. Preserve the crash suite’s independent log oracle.

4. **Judgement call — Divergent Change in `src/worker.ts`.**

   **Locations:** client construction/shutdown at `182–334`; lag sampling at `584–627`; background Task preparation at `875–896`; transaction protocols at `1191–1371`.

   Representative responsibilities include `async #measureLag()`, `async #prepare(...)`, and `class TransactionalCommitter`. Metrics, Restore scheduling, ownership, client configuration and transaction changes all modify the same module, often the same `CommitCycle` class. The concern is responsibility coupling, not merely its size.

   **Fix:** Extract lag reporting, Task-start/Restore management and the existing committers behind narrow interfaces. Keep transaction ordering explicit in one orchestration point.

5. **Judgement call — Divergent Change in `src/streams-app.ts`.**

   **Locations:** configuration at `517–575`; `class Runtime` at `584`, lifecycle at `613–778`; `class StreamNode` at `825`; window retention at `963–994`.

   Worker lifecycle and window aggregation have unrelated reasons to change this module.

   **Fix:** Extract runtime/configuration and window implementation while retaining a cohesive public DSL facade.

6. **Judgement call — Primitive Obsession / Duplicated Code for topic-partition identities.**

   **Locations:** `src/worker.ts:377–378,1141`; `src/ownership.ts:144–170`.

   Both modules encode `"${topic}:${partition}"` and independently parse it, including `Number(key.slice(key.lastIndexOf(':') + 1))`.

   **Fix:** Centralize the representation and parsing in one internal type/helper, or use structured maps. This is a maintenance concern; no current delimiter collision is asserted.

7. **Hard violation, low severity — glossary terminology drifts.**

   **Rules:** `CONTEXT.md:9–15,59–61` discourages “topology” for a Streams App and “Instance” for an App Process/Worker.

   **Locations:** `src/internal-topics.ts:53` (“another instance”); `test/checkpoints.test.ts:155`; `test/integration/rebalance.test.ts:287,289,301,315`; `test/crash/crash.test.ts:25` (“topology sizes”).

   **Fix:** Use Streams App, App Process or Worker as appropriate. Legitimate class instances, graph nodes and Kafka consumers need no mechanical renaming.

## Spec

1. **[P1] A revoked batch can commit after its partition is reassigned to the same Worker.**

   **Requirement:** `.scratch/mvp/spec.md:81`: a rebalance revoking a Task mid-batch must “abort the open transaction safely.”

   **Locations:** `src/worker.ts:1057–1080,1341–1357`; `src/ownership.ts:75–88,97–100`.

   `#commit()` snapshots `takeLost()` before awaiting processing. A slow Processor can remain pending while another App Process acquires, processes and relinquishes the partition. When the original Worker reacquires it, `#withCurrentGroup()` checks only `owns()`, then replaces the record’s metadata with the new generation. Losses accumulated during the wait remain outside the earlier snapshot.

   With separate state directories and the old Processor emitting only after its wait, this permits stale state/output and offsets to commit under valid new group metadata.

   **Fix:** Check continuous ownership, not just present ownership; consume newly recorded losses after waits and validate each record’s ownership epoch before adding offsets. **Evidence:** concrete static interleaving; not reproduced against a broker.

2. **[P1] Timer-only transactions lack ownership fencing.**

   **Requirement:** `.scratch/mvp/spec.md:78`: exactly-once must ensure crashes and rebalances “never produce duplicates or lose updates.”

   **Locations:** `src/worker.ts:1057–1070,1322–1332`; `src/restore.ts:252–263`.

   Timers can produce Changelog records while `lastRead` is empty. The transaction then skips `addConsumer()` and `addOffset()`, so its commit does not validate group membership. The revoke check also considers only input records.

   Restore waits for transactions affecting the Changelog end it sampled. That protects against already-visible pending writes, but cannot prevent a zombie from appending timer writes after the sampled boundary.

   **Fix:** Include an ownership-fenced offset operation for every Task contributing timer effects, even without new input, and reject effects from an interrupted ownership period. **Evidence:** confirmed missing fencing path; occurrence frequency is unmeasured.

3. **[P1] A handled key-serialization failure leaves a local mutation without a Changelog entry.**

   **Requirement:** `.scratch/mvp/spec.md:74`: “every State Store write recorded to a compacted Changelog”; ADR-0002 identifies that Changelog as the source of truth.

   **Locations:** `src/state-store.ts:203–211`; `src/task.ts:374–379`.

   The store is changed before its Changelog key is serialized. Native string store keys bypass the key Serde during local encoding, so a custom serializer can throw only after the mutation. A processing handler returning `continue`, including dead-letter routing, permits that unlogged mutation to survive.

   An in-memory reproduction committed value `42` after `put('x',42)` threw, with **zero Changelog records**.

   **Fix:** Prepare local and Changelog key representations before modifying the store. Any failure after mutation that prevents logging must fail the transaction rather than continue.

4. **[P1] Key-changing `map()` followed by `groupByKey()` silently aggregates separately per Task.**

   **Requirement:** `.scratch/mvp/spec.md:57` requests running totals per key. `.scratch/mvp/STATUS.md:23` explicitly requires: “for 0.1, fail fast when the app is built with a clear message.”

   **Locations:** `src/streams-app.ts:849–864,898–904`; `src/processor-graph.ts:87–92`.

   `map()` changes the forwarded key without changing the Task. Supplying explicit Serdes makes the subsequent `groupByKey()` pass its only validation. Equal new keys originating in different input partitions therefore produce independent aggregates.

   **Fix:** Track whether partitioning remains valid through the graph. Raise the build-time error in `StreamNode.groupByKey()` before constructing `GroupedStreamNode`, preserving that metadata through intervening operators and Processor connections. Arbitrary `map()` functions cannot reliably be proven key-preserving by inspection.

5. **[P1] Stale-lock reclamation can admit simultaneous owners.**

   **Requirement:** `.scratch/mvp/issues/29-scale-out-processes.md:29` requires a per-Task lock to prevent a new owner opening a Task before the previous owner closes it.

   **Location:** `src/state-directory.ts:159–182`.

   A contender reads a stale lock, then unconditionally renames the current pathname. Another contender may already have replaced it with a live lock. While that live lock is displaced, a third contender can acquire the pathname. Restoring the displaced live lock then fails with `EEXIST`, which is swallowed.

   Two callers can consequently believe they hold the lock, threatening exclusive Task-store access. The same reclamation helper also protects App Process IDs. The source acknowledges a race at lines 25–27.

   **Fix:** Use a locking primitive or reclamation protocol that preserves exclusivity throughout takeover; checking the displaced contents afterward is insufficient. **Evidence:** static concurrent interleaving, not a stress-test reproduction.

6. **[P2] In-memory State Stores alias Node Buffer inputs and outputs.**

   **Requirement:** `.scratch/mvp/issues/10-count-in-memory-store.md:19`: “Stores copy bytes in and out.”

   **Locations:** `src/state-store.ts:255–263,275–281`.

   `Buffer` is accepted as `Uint8Array`, but its `.slice()` shares memory. The implementation uses `.slice()` for storage and retrieval. In an executed reproduction, modifying the input changed stored `"one"` to `"xne"`; modifying `get()`’s result changed it again to `"yne"`.

   These mutations bypass store operations, undo bookkeeping and Changelog writes. `all()` and `range()` expose the same problem.

   **Fix:** Make explicit byte copies, such as `Uint8Array.from(...)`, on ingress and egress. Extend the public store contract suite to cover Buffers and mutation during transactions.

7. **[P2] Skipped processing errors advance Stream Time and fire timers.**

   **Requirement:** `.scratch/mvp/issues/25-observability.md:22`: “Records skipped by a handler do not advance Stream Time”; repeated in `docs/windows.md:39`.

   **Location:** `src/task.ts:373–389`.

   Stream Time advances before processing. The continuing error path preserves it, then fires Stream Time timers. A reproduction using the actual Task processed timestamps `1`, skipped a failing record at `5000`, then processed `2`: observed Stream Times were `[1,5000]`, and a timer fired at `5000`.

   A rejected far-future record can therefore make subsequent valid window records late. Deserialization skips correctly avoid advancement.

   **Fix:** Preserve committed Stream Time and timer scheduling when processing is skipped, consistently handling any window effects already produced.

8. **[P2] The scale-out integration test cannot reliably establish exactly-once output.**

   **Requirement:** `.scratch/mvp/issues/29-scale-out-processes.md:10`: “results are exact under exactly-once.”

   **Locations:** `test/integration/scale-out.test.ts:123–129`; `test/integration/kafka.ts:268–275`.

   `expectExactCounts()` uses the stock consumer with `READ_COMMITTED`. The installed client explicitly ignores an aborted range when its marker is absent from that fetch response: `node_modules/@platformatic/kafka/dist/clients/consumer/consumer.js:1909–1927`. It can also discard committed records when aborted ranges are combined incorrectly.

   Thus an incorrect observed count can reflect the reader rather than the engine.

   **Fix:** Use the abort-aware consumer for integration reads, or drain completed output with the independent crash oracle at `test/crash/log-reader.ts:51–141`. Then repeat the rebalance scenarios. The existing 2502-versus-2480 observation remains inconclusive.

9. **[P2] The client dependency remains unpinned despite reliance on its internals.**

   **Requirement:** `.scratch/mvp/issues/26-release-0-1.md:35` flags exact pinning; `.scratch/mvp/STATUS.md:24` explicitly requests it.

   **Locations:** `package.json:37`; `src/read-committed.ts:144–150,224–246`.

   `"^2.14.0"` permits upgrades while the workaround overrides `kGetApi`, forces positional Fetch arguments and disables the client’s own filtering. A compatible-range release could break these assumptions. The repository lockfile does not pin dependency resolution for downstream package consumers.

   **Fix:** Pin the validated client version exactly until the workaround is removed, and require its broker regression tests for upgrades. This is a compatibility risk, not an observed failure with installed version 2.14.0.

## Open items

1. **Possible exactly-once duplicates — unclear cause; defective test reader confirmed.**

   The scale-out path reaches stock `Consumer` at `test/integration/kafka.ts:268`, with the filtering defect visible in the installed client at `consumer.js:1909–1927`. Therefore 2502 versus 2480 does not prove committed duplicates. The independent crash reader resolves commit/abort markers at `test/crash/log-reader.ts:109–140`. Separately, Spec findings 1–2 identify real engine fencing gaps, but neither has been tied to that historical failure.

2. **Silent wrong aggregation — confirmed.**

   `src/streams-app.ts:849–864` changes keys; `898–904` only checks Serdes before grouping. Add partitioning validation at `groupByKey()` construction, with graph metadata propagated from earlier operators. Explicit Serdes must not bypass it.

3. **Zombie timer writes — confirmed fencing gap.**

   `src/worker.ts:1322–1332` omits group-offset fencing when no input was read. The Restore boundary at `src/restore.ts:252–263` only covers writes already included in its sampled end. The dangerous timing requires ownership loss plus delayed/new timer writes; long async callbacks or delayed membership detection make it plausible. Calling it “rare” is not supported by measured evidence.

4. **Unverified documentation claims — substantially confirmed; suspicion of a basic contradiction refuted.**

   - **Transaction timeout:** `src/worker.ts:165–166,219–221` configures `commitIntervalMs + 10_000`. The installed producer passes its `timeout` to `InitProducerId` at `node_modules/@platformatic/kafka/dist/clients/producer/producer.js:550`, supporting `docs/delivery-guarantees.md:29,39`. This establishes the configured timeout, not an exact wall-clock deadline for abort completion.
   - **Restarted timers:** timers and due times live only in memory at `src/task.ts:194–218`; suppliers are invoked when Tasks are created at `300–308`. Restart non-persistence is correct.
   - **Qualification:** `docs/processor-api.md:66` and `docs/limitations.md:15` overgeneralize rebalances. Cooperative rebalances retaining a Task also retain its timers; only recreated Tasks reschedule them (`src/worker.ts:1113,1135–1138`).

5. **Dependency on client internals — confirmed.**

   `src/read-committed.ts:144–150,232–246` depends on the Fetch calling convention and `kGetApi`; `package.json:37` permits compatible-range upgrades. Pinning is warranted. No upstream release status was assumed or independently verified.

6. **Flaky integration tests — confirmed timing weaknesses; historical causes unclear.**

   Evidence includes:

   - `test/integration/kafka.ts:100,285–300`: a default 20-second read budget and fixed post-count settling window. Settling does not prove that every partition has been drained.
   - `test/integration/passthrough.test.ts:114–132`: two application starts, two output reads and a stop share the default 60-second test budget from `vitest.integration.config.ts:14`; group emptiness is asserted immediately at line 124.
   - `test/integration/at-least-once-crash.test.ts:84–102,129–152`: randomized kills occur after “running,” which the harness defines as having joined the group (`test/integration/harness/app-process.ts:49–52`), without establishing processing progress. Random kill delays are intentional, but do not prove that a kill landed mid-processing.
   - `test/integration/scale-out.test.ts:105–118,214–218` already uses condition-based waits and generous deadlines. Its reported 13-second failure cannot be attributed to a particular timeout from inspection alone.
   - Topic names are isolated by `uniqueName()` at `test/integration/kafka.ts:22–24`; shared broker capacity remains a plausible source of interference.

   **Fix:** Establish progress before randomized interruption, poll lifecycle conditions, and drain output to known partition boundaries. Align overall test/hook deadlines with their constituent operations and include child-process diagnostics on failure.

## Summary

- **Standards: 7 findings** — 4 hard violations and 3 judgement calls; worst: revoke handling contradicts ADR-0006’s abort requirement.
- **Spec: 9 findings** — worst: a batch from revoked ownership can commit after reassignment using the new group generation.

Reviewed `main` (`1d26022`) through `mvp` (`0150f80`). No files were changed, and no integration/crash tests or Kafka operations were run.

Unit validation was environment-limited: `npm test` failed creating Vite’s temporary config file. Retrying with native config loading and caching disabled produced 33 passing tests, but most suites could not load because the sandbox denies `os.uptime()`. Three targeted, entirely in-memory reproductions confirmed the serialization, Buffer-aliasing and skipped-Stream-Time findings.
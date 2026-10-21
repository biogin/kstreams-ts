> **Warning (added later):** the claims below that read_committed hides aborted records are WRONG for 2.14.0. The client's filter misses aborted transactions whose marker is in a later fetch response, and can drop committed records when a restarted producer reuses its producer ID (platformatic/kafka#467). kstreams-ts uses its own filter in `src/read-committed.ts`; don't rely on the client's.

# @platformatic/kafka 2.14.0 — notes for kstreams-ts

Version checked: **2.14.0** (npm latest, released 2026-10-07). Pure ESM (`"type": "module"`). Pure JS
except an *optional* napi dep `@node-rs/crc32` (falls back to WASM `@platformatic/wasm-utils`).
README says supported brokers are **Kafka 3.5.0–4.2.0**; everything below was verified against a
local **Kafka 4.4.0** KRaft broker (see `local-broker.md`) and worked.

Sources (read in a scratch install, `node_modules/@platformatic/kafka/dist/...`):
`clients/consumer/{consumer,messages-stream,types}.{js,d.ts}`, `clients/producer/{producer,transaction}.js`,
`clients/admin/{admin,types}.d.ts`, `apis/enumerations.js`, `protocol/records.d.ts`;
docs: github.com/platformatic/kafka/tree/v2.14.0/docs (`consumer.md`, `transactions.md`, `admin.md`).
Experiments: `/Users/igorshalygin/.claude/jobs/e36bc646/tmp/explore/kafka-client/scripts/exp{1,2,3,4}.mjs`.
Legend: **[V]** verified by experiment, **[S]** read in source, **[U]** unverified.

## Constructors (all clients extend `Base`)
Common `BaseOptions`: `clientId` (req), `bootstrapBrokers: string[]` (req), `timeout`, `retries`
(number | true=infinite, default 3), `retryDelay` (ms or fn, default 250), `metadataMaxAge`,
`autocreateTopics`, `strict`, `metrics`, SASL/TLS. All methods are callback **or** Promise.
`client.metadata({ topics, forceUpdate })` → `ClusterMetadata` (`topics.get(t).partitionsCount`,
`.id` (topicId), `.partitions[i].leader/leaderEpoch`). `close()` everywhere.

```ts
import { Admin, Producer, Consumer, FetchIsolationLevels, ListOffsetTimestamps,
  COOPERATIVE_STICKY_ASSIGNOR, IS_CONTROL, stringSerializers, stringDeserializers } from '@platformatic/kafka'

const producer = new Producer({ clientId, bootstrapBrokers, idempotent: true,
  transactionalId: `app-${workerId}`, producerId: -1n, producerEpoch: -1, // see restart gotcha below
  serializers: stringSerializers /* default: Buffer passthrough */ })
const consumer = new Consumer({ groupId, clientId, bootstrapBrokers,
  autocommit: false,                                   // default true!
  isolationLevel: FetchIsolationLevels.READ_COMMITTED, // 1 — this is ALREADY the default [S]
  protocols: [{ name: COOPERATIVE_STICKY_ASSIGNOR, version: 3 }], // default: roundrobin (eager)
  sessionTimeout: 10_000, heartbeatInterval: 1_000, rebalanceTimeout: 10_000, maxWaitTime: 500,
  highWaterMark: 1024 /* msgs buffered in the Readable */ })
```
Generic params `<Key, Value, HeaderKey, HeaderValue>` come from (de)serializers; default `Buffer`.

## Consuming — `consumer.consume(opts)` → `MessagesStream` (a Node `Readable`, objectMode)
- `consume()` is what joins the group (constructor does nothing). [S][V]
- opts: `topics`, `mode` (`'latest'` default!, `'earliest'`, `'committed'`, `'manual'` + `offsets`),
  `fallbackMode` (`'latest'` default, `'earliest'`, `'fail'`), plus any consumer option override
  (`autocommit`, `isolationLevel`, `maxWaitTime`, `highWaterMark`, ...). For an app: `mode: 'committed',
  fallbackMode: 'earliest'`.
- Per-message only. No batch API by design (docs suggest `hwp` `batchIterator`). Use
  `for await (const m of stream)` or `'data'`. Backpressure = Readable `highWaterMark`; fetching
  stops while the buffer is full. `stream.pause()/resume()` pause **the whole stream**. [S]
- `Message`: `topic`, `partition`, `offset: bigint`, `timestamp: bigint` (ms), `key`, `value`,
  `headers: Map`, `headerEntries`, `leaderEpoch`, `metadata.consumer = { groupId, memberId,
  generationId, coordinatorId }` (captured **at fetch time**), `commit()` (only when autocommit=false). [V]
- Control batches (txn markers) are skipped by the stream; aborted records filtered under read_committed. [S][V]
- Errors: `'error'` on the stream → stream dead, consumer still usable; call `consume()` again.

## Manual commit (at-least-once path)
```ts
await consumer.commit({ offsets: [{ topic, partition, offset: last.offset + 1n, leaderEpoch: last.leaderEpoch }] })
await consumer.listCommittedOffsets({ topics: [{ topic, partitions: [0, 1] }] }) // Map<topic, bigint[]>, -1n = none
```

## Transactions (EOS path) [V]
```ts
const tx = await producer.beginTransaction()   // throws unless idempotent:true; one active txn per producer
await tx.addConsumer(consumer)                 // AddOffsetsToTxn for consumer.groupId (once per txn)
await tx.send({ messages: [{ topic, key, value, headers, partition? }] })
await tx.addOffset(lastMsgOfPartition)         // TxnOffsetCommit, commits msg.offset + 1n
await tx.commit()                              // or tx.abort(); tx.cancel() = local only
```
- **BLOCKER-LEVEL GOTCHA [V exp6/exp7]: restarting with an existing `transactionalId` fails.**
  The client sends InitProducerId with `producerId=0n, producerEpoch=0` by default
  (`producer.js` ~line 550: `options.producerId ?? this[kOptions].producerId ?? 0n`) instead of `-1/-1`.
  A *new Producer instance* reusing a transactionalId that already has broker state (clean previous
  shutdown OR crash with open txn) gets **`PRODUCER_FENCED`** on `beginTransaction()`. Fix: pass
  **`producerId: -1n, producerEpoch: -1` in the Producer constructor**. Verified with that: first init
  OK; restart after a crash-with-open-txn inits in ~300 ms (broker aborts the open txn, epoch bumped),
  the zombie's later produce/commit is fenced, and read_committed sees only committed data. No upstream
  issue found (searched 2026-10-21) — worth reporting. Add a crash-suite test for it (ADR-0011).
  ```ts
  new Producer({ ..., idempotent: true, transactionalId, producerId: -1n, producerEpoch: -1 })
  ```
- **Empty transactions**: `beginTransaction()` then `commit()` (or `abort()`) with nothing sent →
  `INVALID_TXN_STATE` from EndTxn [V]. Only begin a txn when there is something to send/commit, or
  track "touched" and skip EndTxn (use `tx.cancel()` for the local cleanup). [U: cancel then reuse]
- `transactionalId` must be stable across restarts (one per Worker, ADR-0008); if omitted a random UUID
  is used. First `beginTransaction()` does InitProducerId (fences older producers with same id).
  `producer.close()` with an open txn just closes (no abort sent) [V].
- `addOffset(message)` reads `message.metadata.consumer.{groupId,generationId,memberId,coordinatorId}`
  and sends one TxnOffsetCommit per call → call once per partition (last record). [S]
- **Fencing works**: after a rebalance, `addOffset` with a message fetched in the old generation fails
  with `AggregateError` ("Specified group generation id is not valid", ILLEGAL_GENERATION). `tx.abort()`
  then succeeds and the producer can begin new txns. [V exp2]
- **Gotcha (cooperative-sticky)**: every rebalance bumps the generation for *all* members, so
  messages already fetched for partitions you *keep* also carry the stale generation. To commit those,
  pass a synthetic object with the **current** group metadata — accepted by the client [V exp4]:
  ```ts
  await tx.addOffset({ topic, partition, offset /* last processed */, metadata: { consumer: {
    groupId: c.groupId, memberId: c.memberId, generationId: c.generationId, coordinatorId: c.coordinatorId } } } as Message)
  ```
  Only do this for partitions still in `consumer.assignments` and whose processing was not interrupted;
  for revoked partitions, abort (ADR-0006). Whether to always use the synthetic form (= Java's
  `groupMetadata()` at commit time) is a design choice — it's what Kafka Streams does.
- Committed txn offsets become visible to `listCommittedOffsets` asynchronously (markers): right after
  `commit()` it returned `-1n`; 500 ms later `1n`. Use `admin.listConsumerGroupOffsets({ groups,
  requireStable: true })` or poll in tests. [V]
- Read-committed consumer sees only committed records; aborted ones never delivered. [V]

## End offsets / standalone restore consumer (no group)
- End offset: `consumer.listOffsets({ topics: [cl], isolationLevel: READ_COMMITTED })` → `Map<topic,
  bigint[]>` indexed by partition; with READ_COMMITTED this is the **LSO**. `timestamp:
  ListOffsetTimestamps.EARLIEST` (-2n) for log start. (`admin.listOffsets` also exists.) [V]
- There is **no assign()/seek()** API and no per-partition pause on `MessagesStream`. [S]
  `consume({ mode: 'manual', offsets })` still joins a group. For a restore consumer use the
  low-level **`consumer.fetch()`** (no group join; `groupId` is still a required ctor option, any
  string). It filters aborted txns under READ_COMMITTED but returns control batches. [V exp1]
```ts
const md = await restore.metadata({ topics: [cl] }); const t = md.topics.get(cl)!
const end = (await restore.listOffsets({ topics: [cl], isolationLevel: 1 })).get(cl)![p]
let pos = checkpointOffset
while (pos < end) {
  const r = await restore.fetch({ node: t.partitions[p].leader, isolationLevel: 1, maxWaitTime: 500,
    topics: [{ topicId: t.id, partitions: [{ partition: p, fetchOffset: pos,
      currentLeaderEpoch: -1, lastFetchedEpoch: -1, partitionMaxBytes: 1 << 20 }] }] })
  const part = r.responses[0].partitions[0]  // errorCode, highWatermark, lastStableOffset, records
  for (const b of part.records ?? []) {
    for (const rec of b.records) {
      const off = b.firstOffset + BigInt(rec.offsetDelta)
      if (off < pos || (b.attributes & IS_CONTROL)) continue  // batch may start before pos
      apply(rec.key, rec.value /* null = tombstone */, off)
    }
    pos = b.firstOffset + BigInt(b.lastOffsetDelta) + 1n
  }
}
```
Notes: loop on `pos < end`, **not** "last record offset == end-1": txn markers occupy offsets (in exp1
the last data record was 6 and end was 8) and compaction leaves gaps. Raw records: `key/value: Buffer|null`,
`headers: [Buffer,Buffer][]`, timestamp = `b.firstTimestamp + rec.timestampDelta`. Check `part.errorCode`
(e.g. OFFSET_OUT_OF_RANGE → restart from log start). All partitions in one fetch must live on `node`.

**Per-Task pause (ADR-0009) has no client support** — options: hold records of restoring
partitions in an in-memory queue, or pause the whole stream (affects all Tasks). [S]

## Admin [V]
```ts
const admin = new Admin({ clientId, bootstrapBrokers })
await admin.createTopics({ topics: [name], partitions: 4, replicas: 1,
  configs: [{ name: 'cleanup.policy', value: 'compact' }] })  // → [{ id, name, partitions, replicas, configuration }]
await admin.listTopics()                         // string[] (includeInternals?: boolean)
(await admin.metadata({ topics: [name] })).topics.get(name)!.partitionsCount
await admin.describeConfigs({ resources: [{ resourceType: 2 /*TOPIC*/, resourceName: name, configurationKeys: ['cleanup.policy'] }] })
await admin.deleteTopics({ topics: [name] })
```
`configs`/`partitions`/`replicas` apply to all `topics` in the call; per-topic object form
`{ topic, partitions, replicas, assignments }` has no configs. Creating an existing topic throws
`AggregateError("Creating topics failed.")` whose `errors[0]` is a ResponseError (`code:
'PLT_KFK_RESPONSE'`) with Kafka errorCode **36** (TOPIC_ALREADY_EXISTS) in its `response` [V]. Also: `listGroups`, `describeGroups`,
`deleteGroups`, `listConsumerGroupOffsets`, `alterConsumerGroupOffsets`, `createPartitions`, `deleteRecords`.

**Gotcha [V exp3]**: a *non-idempotent* producer sending right after `createTopics` produced
**duplicates** (end offsets `3,2,1,1` for 4 single sends) — retry after partial leader-not-available
resends the whole batch. Fixes: use `idempotent: true` (no dups), and/or wait until
`metadata({topics, forceUpdate:true})` shows every partition with `leader >= 0`. Do both in test helpers.

## Rebalance / assignment visibility [V exp2]
- `consumer.assignments: { topic, partitions: number[] }[] | null`, `generationId`, `memberId`.
- Events (plain EventEmitter, **synchronous, cannot block or delay the rebalance**; no
  onPartitionsRevoked/Assigned callbacks):
  - `consumer:group:join` `{ groupId, memberId, generationId, isLeader, assignments }` — after each
    completed SyncGroup, full current assignment. Diff against the previous one to derive added/revoked.
  - `consumer:group:rebalance` `{ groupId }` — fired (a) on heartbeat REBALANCE_IN_PROGRESS before rejoin
    (assignments still old), and (b) in the cooperative revoke round **after** `consumer.assignments`
    has already been updated to the reduced set.
  - `consumer:group:rejoin`, `consumer:group:leave`, `consumer:heartbeat:*`.
- Cooperative revoke round (source `#completeClassicSyncGroup`): pause all streams → autocommit (only if
  autocommit enabled) → set assignments → resume → emit rebalance → rejoin → `join` event.
  Observed: A [0..3] gen1 → B joins → A rebalance(revoke) → A [2,3] gen3, B [0,1] gen3; B leaves → A [0..3] gen4.
- **Records already buffered in the stream are not purged on revoke** [S, not exercised]: drop records
  whose partition is no longer assigned or whose `metadata.consumer.generationId` is older and which
  belong to revoked partitions.
- Custom assignor: `partitionAssigner(current, members, topics, metadata)` + `protocolsMetadata`
  callback (per-member user data) — for the v1.x state-aware assignor.
- KIP-848 `groupProtocol: 'consumer'` exists (server-side assignors `uniform`/`range`); not tested.

## Graceful close [V]
`await stream.close(); await consumer.close()` → sends LeaveGroup (event `consumer:group:leave`),
~20 ms. `consumer.close()` without `force` errors if a stream is still open; `close(true)` closes
streams. `consumer.leaveGroup(force?)` exists too. Without LeaveGroup (crash), the group waits
`sessionTimeout` (default **60 s**) before reassigning — use small `sessionTimeout` in tests.
`producer.close()`; abort any open txn first [U: behaviour with open txn].

## worker_threads [V]
A consumer group member inside a `Worker` joined, got partitions and left cleanly (exp2). No native
addon required (`@node-rs/crc32` is optional napi-rs, context-aware). Caveats: `bigint` offsets
and `Buffer`s survive `postMessage` (structured clone; Buffers arrive as `Uint8Array`) [U for Buffer
subclass]; clients are not transferable — create them inside the worker. CPU-bound processing blocks
heartbeats only for that worker's event loop (heartbeat timers run on it).

// Helpers for integration tests against a real broker (seam 2). The broker comes from the
// KSTREAMS_KAFKA_BROKERS environment variable (comma-separated host:port list); without it the
// integration tests are skipped. Start a local broker with `npm run kafka -- start`.

import { randomUUID } from 'node:crypto'
import {
  Admin,
  Consumer,
  FetchIsolationLevels,
  IS_CONTROL,
  ListOffsetTimestamps,
  Producer,
} from '@platformatic/kafka'
import { describe } from 'vitest'
import type { StateStore } from '../../src/index.js'
import { ReadCommittedConsumer } from '../support/internals.js'

export const BROKERS_ENV = 'KSTREAMS_KAFKA_BROKERS'

export const brokers: string[] = (process.env[BROKERS_ENV] ?? '')
  .split(',')
  .map((broker) => broker.trim())
  .filter((broker) => broker !== '')

/** `describe` that skips its tests when no broker is configured. */
export function describeWithKafka(name: string, tests: () => void): void {
  describe.skipIf(brokers.length === 0)(name, tests)
}

/** A name no other test (or earlier run) uses, for topics and Application IDs. */
export function uniqueName(prefix: string): string {
  return `${prefix}-${randomUUID().slice(0, 8)}`
}

export interface TestRecord {
  key: string | null
  value: string | null
  timestamp: number
  partition?: number
}

/** Admin access for one test. Close it in `afterAll`/`afterEach`. */
export function createKafka() {
  const admin = new Admin({ clientId: uniqueName('test-admin'), bootstrapBrokers: brokers })
  const producer = new Producer({
    clientId: uniqueName('test-producer'),
    bootstrapBrokers: brokers,
    // A non-idempotent producer can write duplicates right after topic creation.
    idempotent: true,
  })
  const topics: string[] = []

  return {
    admin,

    /**
     * Creates a topic with a unique name and waits until every partition has a leader. `configs`
     * are topic configs, e.g. `{ 'max.message.bytes': '200' }`.
     */
    async createTopic(
      prefix: string,
      partitions = 1,
      configs: Record<string, string> = {},
    ): Promise<string> {
      const topic = uniqueName(prefix)
      // Test records carry fixed timestamps from 2023, so the broker's time-based retention
      // (checked every 5 minutes) would delete them mid-test: keep them for good.
      const all = { 'retention.ms': '-1', ...configs }
      await admin.createTopics({
        topics: [topic],
        partitions,
        replicas: 1,
        configs: Object.entries(all).map(([name, value]) => ({ name, value })),
      })
      topics.push(topic)
      for (let attempt = 0; ; attempt++) {
        const metadata = await admin.metadata({ topics: [topic], forceUpdate: true })
        const found = metadata.topics.get(topic)
        if (
          found?.partitionsCount === partitions &&
          found.partitions.every((partition) => partition.leader >= 0)
        ) {
          return topic
        }
        if (attempt === 50) throw new Error(`Topic "${topic}" has no leaders after 5 s`)
        await delay(100)
      }
    },

    /** Writes string records to `topic` and waits for the broker to acknowledge them. */
    async produce(topic: string, records: TestRecord[]): Promise<void> {
      await producer.send({
        messages: records.map(({ key, value, timestamp, partition }) => ({
          topic,
          ...(key === null ? {} : { key: Buffer.from(key) }),
          value: value === null ? (null as unknown as Buffer) : Buffer.from(value),
          timestamp: BigInt(timestamp),
          ...(partition === undefined ? {} : { partition }),
        })),
      })
    },

    /**
     * Reads `topic` from the beginning until `count` records arrived (right away for 0), then
     * keeps reading for `settleMs` to catch any extra (duplicate) records. Rejects after
     * `timeoutMs`. Reads only committed records unless `uncommitted` is set.
     */
    async consume(
      topic: string,
      count: number,
      { timeoutMs = 20_000, settleMs = 500, uncommitted = false } = {},
    ): Promise<TestRecord[]> {
      let seen = 0
      return readTopic(topic, () => ++seen === count, {
        timeoutMs,
        settleMs,
        uncommitted,
        // Nothing to wait for: only listen for `settleMs`.
        startSettled: count === 0,
      })
    },

    /**
     * Reads `topic` from the beginning (read_committed), calling `done` with each record, until it
     * returns true. Resolves with every record read (duplicates included); rejects after
     * `timeoutMs`. Use it when the number of output records is not known, e.g. at-least-once
     * after a crash.
     */
    async consumeUntil(
      topic: string,
      done: (record: TestRecord) => boolean,
      { timeoutMs = 20_000 } = {},
    ): Promise<TestRecord[]> {
      return readTopic(topic, done, { timeoutMs, settleMs: 0 })
    },

    /**
     * Reads `topic` from the beginning (read_committed) as raw bytes until `count` records arrived,
     * then for `settleMs` more. For topics whose keys or values aren't strings.
     */
    async consumeBytes(
      topic: string,
      count: number,
      { timeoutMs = 20_000, settleMs = 500 } = {},
    ): Promise<BytesRecord[]> {
      let seen = 0
      return readMessages(topic, toBytesRecord, () => ++seen === count, { timeoutMs, settleMs })
    },

    /**
     * Reads `topic` from the beginning as string records with their headers (as strings), until
     * `count` records arrived, then for `settleMs` more.
     */
    async consumeWithHeaders(
      topic: string,
      count: number,
      { timeoutMs = 20_000, settleMs = 500 } = {},
    ): Promise<HeadersRecord[]> {
      let seen = 0
      return readMessages(topic, toHeadersRecord, () => ++seen === count, { timeoutMs, settleMs })
    },

    /** Deletes `topic` on `close`, e.g. an Internal Topic the app under test created. */
    deleteOnClose(topic: string): void {
      topics.push(topic)
    },

    /** The committed offset of every partition of `topic` for consumer group `groupId`. */
    async committedOffsets(groupId: string, topic: string): Promise<Map<number, bigint>> {
      // `requireStable` waits for offsets committed in a transaction to be written.
      const [group] = await admin.listConsumerGroupOffsets({
        groups: [groupId],
        requireStable: true,
      })
      const offsets = new Map<number, bigint>()
      for (const partition of group?.topics.find(({ name }) => name === topic)?.partitions ?? []) {
        offsets.set(partition.partitionIndex, partition.committedOffset)
      }
      return offsets
    },

    /** The sum of the committed offsets of `topic` for consumer group `groupId`. */
    async consumed(groupId: string, topic: string): Promise<bigint> {
      const committed = await this.committedOffsets(groupId, topic)
      return [...committed.values()].reduce((sum, offset) => sum + offset, 0n)
    },

    /** The members of consumer group `groupId`. */
    async groupMembers(groupId: string): Promise<number> {
      const groups = await admin.describeGroups({ groups: [groupId] })
      return groups.get(groupId)?.members.size ?? 0
    },

    /**
     * The state of consumer group `groupId` (e.g. `STABLE`), and the partitions of `topic`
     * assigned to each member, by the member's client ID.
     */
    async groupAssignments(
      groupId: string,
      topic: string,
    ): Promise<{ state: string; assigned: Map<string, number[]> }> {
      const group = (await admin.describeGroups({ groups: [groupId] })).get(groupId)
      const assigned = new Map<string, number[]>()
      for (const member of group?.members.values() ?? []) {
        const partitions = member.assignments?.get(topic)?.partitions ?? []
        assigned.set(
          member.clientId,
          [...partitions].sort((a, b) => a - b),
        )
      }
      return { state: String(group?.state ?? '').toUpperCase(), assigned }
    },

    /**
     * Deletes the topics this helper created (unless KSTREAMS_KEEP_TOPICS is set, to inspect them
     * after a failure) and closes its clients.
     */
    async close(): Promise<void> {
      try {
        if (topics.length > 0 && !process.env.KSTREAMS_KEEP_TOPICS) {
          await admin.deleteTopics({ topics })
        }
      } finally {
        await producer.close()
        await admin.close()
      }
    },
  }
}

/** A record read as bytes; a missing key or a tombstone is null. */
export interface BytesRecord {
  key: Buffer | null
  value: Buffer | null
  timestamp: number
  partition: number
}

function toBytesRecord(record: FetchedRecord): BytesRecord {
  return {
    key: record.key,
    value: record.value,
    timestamp: record.timestamp,
    partition: record.partition,
  }
}

/** A string record with its headers. */
export interface HeadersRecord extends TestRecord {
  headers: Record<string, string>
}

function toHeadersRecord(record: FetchedRecord): HeadersRecord {
  return {
    ...toTestRecord(record),
    headers: Object.fromEntries(
      record.headers.map(([name, value]) => [String(name ?? ''), String(value ?? '')]),
    ),
  }
}

function toTestRecord(record: FetchedRecord): TestRecord {
  return {
    key: record.key === null ? null : record.key.toString(),
    value: record.value === null ? null : record.value.toString(),
    timestamp: record.timestamp,
    partition: record.partition,
  }
}

function readTopic(
  topic: string,
  done: (record: TestRecord) => boolean,
  options: ReadOptions,
): Promise<TestRecord[]> {
  return readMessages(topic, toTestRecord, done, options)
}

/** A data record as fetched. */
interface FetchedRecord {
  key: Buffer | null
  value: Buffer | null
  headers: [Buffer | null, Buffer | null][]
  timestamp: number
  partition: number
}

interface ReadOptions {
  timeoutMs: number
  settleMs: number
  uncommitted?: boolean
  startSettled?: boolean
}

/** Fetch errors that new metadata (another leader, a newer leader epoch) fixes. */
const RETRIABLE_FETCH_ERRORS = new Set([3, 6, 74, 75])

/**
 * Reads every partition of `topic` from its start, without joining a consumer group: it fetches
 * the partitions itself, from positions it keeps. Read_committed reads go through the engine's
 * abort-aware `ReadCommittedConsumer`, because the client's own read_committed filter can deliver
 * aborted records and drop committed ones (platformatic/kafka#467). Calls `done` with each record
 * until it returns true, then keeps reading for `settleMs` to catch extra (duplicate) records.
 * Rejects after `timeoutMs` unless `done` has returned true by then.
 */
async function readMessages<R>(
  topic: string,
  convert: (record: FetchedRecord) => R,
  done: (record: R) => boolean,
  { timeoutMs, settleMs, uncommitted = false, startSettled = false }: ReadOptions,
): Promise<R[]> {
  const clientId = uniqueName('test-reader')
  const options = {
    clientId,
    // Required by the client, but `fetch()` never joins the group.
    groupId: `${clientId}-unused`,
    bootstrapBrokers: brokers,
  }
  const isolationLevel = uncommitted
    ? FetchIsolationLevels.READ_UNCOMMITTED
    : FetchIsolationLevels.READ_COMMITTED
  const consumer = uncommitted
    ? new Consumer(options)
    : new ReadCommittedConsumer({ ...options, fetchSessions: false })
  const records: R[] = []
  const deadline = Date.now() + timeoutMs
  let settleUntil = startSettled ? Date.now() + settleMs : undefined
  try {
    const positions = new Map<number, bigint>()
    const earliest = await consumer.listOffsets({
      topics: [topic],
      timestamp: ListOffsetTimestamps.EARLIEST,
      isolationLevel,
    })
    for (const [partition, offset] of (earliest.get(topic) ?? []).entries()) {
      positions.set(partition, offset)
    }
    let leaders = await partitionLeaders(consumer, topic, false)
    while (settleUntil === undefined ? Date.now() <= deadline : Date.now() < settleUntil) {
      let retry = false
      for (const [node, partitions] of leaders.byNode) {
        const response = await consumer.fetch({
          node,
          isolationLevel,
          maxWaitTime: 100,
          topics: [
            {
              topicId: leaders.topicId,
              partitions: partitions.map((partition) => ({
                partition,
                fetchOffset: positions.get(partition) ?? 0n,
                currentLeaderEpoch: -1,
                lastFetchedEpoch: -1,
                partitionMaxBytes: 1 << 20,
              })),
            },
          ],
        })
        for (const fetched of response.responses[0]?.partitions ?? []) {
          const partition = fetched.partitionIndex
          if (fetched.errorCode !== 0) {
            if (!RETRIABLE_FETCH_ERRORS.has(fetched.errorCode)) {
              throw new Error(
                `Fetching "${topic}" partition ${partition} failed with error code ${fetched.errorCode}`,
              )
            }
            retry = true
            continue
          }
          let position = positions.get(partition) ?? 0n
          for (const batch of fetched.records ?? []) {
            const last = batch.firstOffset + BigInt(batch.lastOffsetDelta)
            if (last < position) continue
            // Control batches hold markers, and aborted batches come without records.
            if (!(batch.attributes & IS_CONTROL)) {
              for (const record of batch.records) {
                if (batch.firstOffset + BigInt(record.offsetDelta) < position) continue
                const converted = convert({
                  key: record.key ?? null,
                  value: record.value ?? null,
                  headers: record.headers,
                  timestamp: Number(batch.firstTimestamp + record.timestampDelta),
                  partition,
                })
                records.push(converted)
                if (settleUntil === undefined && done(converted)) {
                  settleUntil = Date.now() + settleMs
                }
              }
            }
            position = last + 1n
          }
          positions.set(partition, position)
        }
      }
      if (retry) {
        await delay(100)
        leaders = await partitionLeaders(consumer, topic, true)
      }
    }
    if (settleUntil === undefined) {
      // Tells a stalled read (positions short of the end) from records missing on the topic.
      const latest = await consumer.listOffsets({
        topics: [topic],
        timestamp: ListOffsetTimestamps.LATEST,
        isolationLevel,
      })
      const ends = (latest.get(topic) ?? []).map(
        (end, partition) => `${partition}: ${positions.get(partition) ?? 0n}/${end}`,
      )
      throw new Error(
        `Read only ${records.length} records from "${topic}" in time (position/end ${ends.join(', ')})`,
      )
    }
  } finally {
    await consumer.close(true)
  }
  return records
}

/** The topic ID of `topic`, and its partitions by the node that leads them. */
async function partitionLeaders(
  consumer: Consumer,
  topic: string,
  forceUpdate: boolean,
): Promise<{ topicId: string; byNode: Map<number, number[]> }> {
  const metadata = await consumer.metadata({ topics: [topic], forceUpdate })
  const found = metadata.topics.get(topic)
  if (found === undefined) throw new Error(`No metadata for topic "${topic}"`)
  const byNode = new Map<number, number[]>()
  for (const [partition, { leader }] of found.partitions.entries()) {
    if (leader < 0) continue
    byNode.set(leader, [...(byNode.get(leader) ?? []), partition])
  }
  return { topicId: found.id, byNode }
}

export function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

/** Polls `check` until it returns true; rejects after `timeoutMs`. */
export async function eventually(
  check: () => boolean | Promise<boolean>,
  { timeoutMs = 10_000, message = 'condition' } = {},
): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (!(await check())) {
    if (Date.now() > deadline) throw new Error(`Timed out waiting for ${message}`)
    await delay(50)
  }
}

/**
 * `store`, kept open when the Streams App closes it (on stop), so that a test can read it once
 * the app has stopped.
 */
export function keptOpen(store: StateStore): StateStore {
  return {
    get: (key) => store.get(key),
    put: (key, value) => store.put(key, value),
    delete: (key) => store.delete(key),
    clear: () => store.clear(),
    all: () => store.all(),
    range: (from, to) => store.range(from, to),
    begin: () => store.begin(),
    commit: () => store.commit(),
    rollback: () => store.rollback(),
    close() {},
  }
}

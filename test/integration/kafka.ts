// Helpers for integration tests against a real broker (seam 2). The broker comes from the
// KSTREAMS_KAFKA_BROKERS environment variable (comma-separated host:port list); without it the
// integration tests are skipped. Start a local broker with `npm run kafka -- start`.

import { randomUUID } from 'node:crypto'
import { Admin, Consumer, FetchIsolationLevels, type Message, Producer } from '@platformatic/kafka'
import { describe } from 'vitest'

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

    /** Creates a topic with a unique name and waits until every partition has a leader. */
    async createTopic(prefix: string, partitions = 1): Promise<string> {
      const topic = uniqueName(prefix)
      await admin.createTopics({ topics: [topic], partitions, replicas: 1 })
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
     * Reads `topic` from the beginning (read_committed) until `count` records arrived, then keeps
     * reading for `settleMs` to catch any extra (duplicate) records. Rejects after `timeoutMs`.
     */
    async consume(
      topic: string,
      count: number,
      { timeoutMs = 20_000, settleMs = 500 } = {},
    ): Promise<TestRecord[]> {
      let seen = 0
      return readTopic(topic, () => ++seen === count, { timeoutMs, settleMs })
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

    /** The committed offset of every partition of `topic` for consumer group `groupId`. */
    async committedOffsets(groupId: string, topic: string): Promise<Map<number, bigint>> {
      const [group] = await admin.listConsumerGroupOffsets({ groups: [groupId] })
      const offsets = new Map<number, bigint>()
      for (const partition of group?.topics.find(({ name }) => name === topic)?.partitions ?? []) {
        offsets.set(partition.partitionIndex, partition.committedOffset)
      }
      return offsets
    },

    /** The members of consumer group `groupId`. */
    async groupMembers(groupId: string): Promise<number> {
      const groups = await admin.describeGroups({ groups: [groupId] })
      return groups.get(groupId)?.members.size ?? 0
    },

    /** Deletes the topics this helper created and closes its clients. */
    async close(): Promise<void> {
      try {
        if (topics.length > 0) await admin.deleteTopics({ topics })
      } finally {
        await producer.close()
        await admin.close()
      }
    },
  }
}

async function readTopic(
  topic: string,
  done: (record: TestRecord) => boolean,
  { timeoutMs, settleMs }: { timeoutMs: number; settleMs: number },
): Promise<TestRecord[]> {
  const consumer = new Consumer({
    clientId: uniqueName('test-consumer'),
    groupId: uniqueName('test-group'),
    bootstrapBrokers: brokers,
    autocommit: false,
    isolationLevel: FetchIsolationLevels.READ_COMMITTED,
    sessionTimeout: 10_000,
    heartbeatInterval: 1_000,
    maxWaitTime: 100,
  })
  const records: TestRecord[] = []
  try {
    const stream = await consumer.consume({ topics: [topic], mode: 'earliest' })
    await new Promise<void>((resolve, reject) => {
      let settle: NodeJS.Timeout | undefined
      const timeout = setTimeout(
        () => reject(new Error(`Read only ${records.length} records from "${topic}" in time`)),
        timeoutMs,
      )
      stream.on('error', reject)
      stream.on('data', (message: Message) => {
        const record: TestRecord = {
          key: message.key === undefined ? null : message.key.toString(),
          value: message.value === undefined ? null : message.value.toString(),
          timestamp: Number(message.timestamp),
          partition: message.partition,
        }
        records.push(record)
        if (settle === undefined && done(record)) {
          clearTimeout(timeout)
          settle = setTimeout(resolve, settleMs)
        }
      })
    })
    await stream.close()
  } finally {
    await consumer.close(true)
  }
  return records
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

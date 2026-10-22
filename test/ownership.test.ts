import { EventEmitter } from 'node:events'
import { describe, expect, it } from 'vitest'
import { type GroupMember, Ownership } from '../src/ownership.js'

/** A consumer-group member as the client reports it: an assignment, a generation and events. */
class FakeMember extends EventEmitter implements GroupMember {
  generationId: number
  assignments: { topic: string; partitions: number[] }[]

  constructor(generationId: number, partitions: number[]) {
    super()
    this.generationId = generationId
    this.assignments = [{ topic: 't', partitions }]
  }

  /** A completed rebalance round, reported with `consumer:group:join`. */
  join(generationId: number, partitions: number[]): void {
    this.generationId = generationId
    this.assignments = [{ topic: 't', partitions }]
    this.emit('consumer:group:join')
  }

  /** The first round of a cooperative rebalance that takes partitions away. */
  revoke(generationId: number, partitions: number[]): void {
    this.generationId = generationId
    this.assignments = [{ topic: 't', partitions }]
    this.emit('consumer:group:rebalance')
  }
}

function record(partition: number, generationId: number) {
  return { topic: 't', partition, metadata: { consumer: { generationId } } }
}

describe('Ownership under the cooperative protocol', () => {
  it('keeps the partitions a two-round rebalance leaves, and loses the revoked ones', () => {
    const member = new FakeMember(5, [0, 1, 2, 3])
    let lostCalls = 0
    const ownership = new Ownership(member, () => lostCalls++)

    member.revoke(6, [0, 1])
    expect(lostCalls).toBe(1)
    expect([...ownership.takeLost()].sort()).toEqual([2, 3])
    expect(ownership.owns('t', 2)).toBe(false)

    member.join(7, [0, 1])
    expect(ownership.lostPending).toBe(false)
    // Kept partitions are owned since generation 5: records fetched before still count.
    expect(ownership.current(record(0, 5))).toBe(true)
    expect(ownership.current(record(2, 6))).toBe(false)
  })

  it('takes new partitions from the second round on', () => {
    const member = new FakeMember(6, [])
    const ownership = new Ownership(member, () => {})
    // The heartbeat reports a rebalance in progress: nothing changed yet.
    member.emit('consumer:group:rebalance')
    member.join(7, [2, 3])
    expect(ownership.lostPending).toBe(false)
    expect(ownership.current(record(2, 7))).toBe(true)
    expect(ownership.current(record(2, 6))).toBe(false)
  })

  it('loses everything after a missed generation', () => {
    const member = new FakeMember(5, [0, 1])
    const ownership = new Ownership(member, () => {})
    member.join(8, [0, 1])
    expect([...ownership.takeLost()].sort()).toEqual([0, 1])
    expect(ownership.current(record(0, 7))).toBe(false)
    expect(ownership.current(record(0, 8))).toBe(true)
  })
})

// A batch may only commit if every Task with a part in it was owned throughout (ADR-0006). These
// interleavings are driven deterministically through the fake member's events.
describe('ownership epochs', () => {
  it('keeps the epoch of a partition a rebalance leaves with the member', () => {
    const member = new FakeMember(5, [0, 1, 2, 3])
    const ownership = new Ownership(member, () => {})
    const batch = new Map([[0, ownership.epoch(0)]])

    member.revoke(6, [0, 1])
    member.join(7, [0, 1])
    expect(ownership.ownedThroughout(batch)).toBe(true)
  })

  it('changes the epoch of a partition lost and assigned again during the batch', () => {
    const member = new FakeMember(5, [0, 1])
    const ownership = new Ownership(member, () => {})
    // The Task of partition 1 processes a record (or fires a timer) under the current epoch.
    const batch = new Map([[1, ownership.epoch(1)]])

    // While its async Processor runs, partition 1 is revoked and assigned back.
    member.revoke(6, [0])
    member.join(7, [0, 1])
    // It is owned again, and its records are current, but its ownership was interrupted.
    expect(ownership.owns('t', 1)).toBe(true)
    expect(ownership.current(record(1, 7))).toBe(true)
    expect(ownership.ownedThroughout(batch)).toBe(false)
    // A Task created now works under the new epoch.
    expect(ownership.ownedThroughout(new Map([[1, ownership.epoch(1)]]))).toBe(true)
  })

  it('changes the epoch even when the loss was already taken by an earlier commit', () => {
    const member = new FakeMember(5, [0])
    const ownership = new Ownership(member, () => {})
    const batch = new Map([[0, ownership.epoch(0)]])
    member.revoke(6, [])
    ownership.takeLost()
    member.join(7, [0])
    expect(ownership.lostPending).toBe(false)
    expect(ownership.ownedThroughout(batch)).toBe(false)
  })

  it('changes every epoch after a missed generation', () => {
    const member = new FakeMember(5, [0, 1])
    const ownership = new Ownership(member, () => {})
    const batch = new Map([
      [0, ownership.epoch(0)],
      [1, ownership.epoch(1)],
    ])
    member.join(8, [0, 1])
    expect(ownership.ownedThroughout(new Map([[0, batch.get(0) ?? 0]]))).toBe(false)
    expect(ownership.ownedThroughout(new Map([[1, batch.get(1) ?? 0]]))).toBe(false)
  })
})

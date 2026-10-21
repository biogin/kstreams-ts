import { describe, expect, it } from 'vitest'
import { assignTasks, type TaskMember } from '../src/task-assignor.js'

const TOPICS = ['orders', 'customers']

function metadata(partitions: number) {
  return { topics: new Map(TOPICS.map((topic) => [topic, { partitionsCount: partitions }])) }
}

/** A member subscribed to both topics that owned `tasks` in `generation`. */
function member(tasks: number[] = [], generation = tasks.length > 0 ? 5 : -1): TaskMember {
  return {
    topics: TOPICS,
    ownedPartitions:
      tasks.length === 0 ? [] : TOPICS.map((topic) => ({ topic, partitions: tasks })),
    generationId: generation,
  }
}

/** The Tasks each member gets, checking that every member gets each Task in both topics. */
function assign(members: Record<string, TaskMember>, partitions: number): Record<string, number[]> {
  const result = assignTasks('a', new Map(Object.entries(members)), new Set(TOPICS), {
    ...metadata(partitions),
  })
  const tasks: Record<string, number[]> = {}
  for (const { memberId, assignments } of result) {
    const orders = assignments.get('orders')?.partitions ?? []
    const customers = assignments.get('customers')?.partitions ?? []
    expect(customers, `${memberId}: Co-partitioned`).toEqual(orders)
    tasks[memberId] = orders
  }
  return tasks
}

describe('the Task assignor', () => {
  it('assigns whole Tasks, balanced, to new members', () => {
    expect(assign({ a: member(), b: member(), c: member() }, 8)).toEqual({
      a: [0, 1, 2],
      b: [3, 4, 5],
      c: [6, 7],
    })
  })

  it('keeps Tasks where they are when nothing changed', () => {
    const tasks = assign({ a: member([1, 4, 6]), b: member([0, 2, 7]), c: member([3, 5]) }, 8)
    expect(tasks).toEqual({ a: [1, 4, 6], b: [0, 2, 7], c: [3, 5] })
  })

  it('only revokes in the first round when a member joins, then assigns in the second', () => {
    // a and b own 4 Tasks each; c joins: each of a and b gives up its highest Task beyond 3.
    const first = assign({ a: member([0, 1, 2, 3]), b: member([4, 5, 6, 7]), c: member() }, 8)
    expect(first).toEqual({ a: [0, 1, 2], b: [4, 5, 6], c: [] })
    // a and b gave them up and joined again: now c gets them.
    const second = assign(
      { a: member([0, 1, 2], 6), b: member([4, 5, 6], 6), c: member([], -1) },
      8,
    )
    expect(second).toEqual({ a: [0, 1, 2], b: [4, 5, 6], c: [3, 7] })
  })

  it('moves only the Tasks of a member that left', () => {
    const tasks = assign({ a: member([0, 1, 2]), b: member([3, 4, 5]) }, 8)
    // c (Tasks 6 and 7) left: a and b keep theirs and share c's.
    expect(tasks).toEqual({ a: [0, 1, 2, 6], b: [3, 4, 5, 7] })
  })

  it('gives a new member a share even when nobody is over the larger share', () => {
    // 9 Tasks over 4 members: shares 3, 2, 2, 2, though a, b and c each have just 3.
    const owners = { a: member([0, 1, 2]), b: member([3, 4, 5]), c: member([6, 7, 8]) }
    const first = assign({ ...owners, d: member() }, 9)
    expect(first).toEqual({ a: [0, 1, 2], b: [3, 4], c: [6, 7], d: [] })
    const second = assign(
      { a: member([0, 1, 2], 6), b: member([3, 4], 6), c: member([6, 7], 6), d: member() },
      9,
    )
    expect(second).toEqual({ a: [0, 1, 2], b: [3, 4], c: [6, 7], d: [5, 8] })
  })

  it('ignores claims from an older generation', () => {
    // b missed a generation: its claim of Task 0 doesn't count against a's.
    const tasks = assign({ a: member([0, 1], 7), b: member([0, 2], 6) }, 4)
    expect(tasks).toEqual({ a: [0, 1], b: [2, 3] })
  })

  it('assigns a Task two members claim to nobody until one of them gave it up', () => {
    const tasks = assign({ a: member([0, 1]), b: member([1, 2]) }, 4)
    // Task 1 is contested: it is revoked from both in this round.
    expect(tasks.a).not.toContain(1)
    expect(tasks.b).not.toContain(1)
  })

  it('assigns only the partitions of topics a member subscribes to', () => {
    const result = assignTasks(
      'a',
      new Map([['a', { topics: ['orders'], ownedPartitions: [], generationId: -1 }]]),
      new Set(TOPICS),
      metadata(2),
    )
    expect([...(result[0]?.assignments.keys() ?? [])]).toEqual(['orders'])
  })
})

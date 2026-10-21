// Internal: the partition assignor of a Streams App's consumer group. It assigns whole Tasks (one
// partition number across all the Co-partitioned input topics), and it is cooperative-sticky:
//
// - Sticky: a Task stays with the member that owned it in the previous generation, unless the
//   member has more Tasks than its fair share. Shares differ by at most one Task; the members that
//   already have the most Tasks get the larger shares, so a rebalance moves as few Tasks as
//   possible.
// - Cooperative (KIP-429): a Task that moves is only taken away in the first rebalance. Its old
//   owner sees it revoked, gives it up and joins again, which starts a second rebalance; only that
//   one assigns the Task to its new owner. So no Task is ever assigned to two members at once.
//
// The client's own cooperative-sticky assignor balances topic-partitions, not Tasks: it could give
// partition 3 of one input topic to one member and partition 3 of another to a second member,
// which would then both run Task 3. This one runs under the client's `cooperative-sticky`
// protocol name, so the client still does the two rounds above.
//
// Which member owned what comes from each member's subscription: the partitions it owns and the
// generation it owned them in. Like Kafka's assignor, only claims from the latest generation count,
// and a Task claimed by two members counts as owned by neither.

import type {
  ExtendedGroupProtocolSubscription,
  GroupAssignment,
  GroupPartitionsAssignments,
} from '@platformatic/kafka'

/** What the assignor reads from a member's subscription. */
export type TaskMember = Pick<
  ExtendedGroupProtocolSubscription,
  'topics' | 'ownedPartitions' | 'generationId'
>

/** The assignor, as the client's `partitionAssigner` option takes it. */
export function assignTasks(
  _leader: string,
  members: ReadonlyMap<string, TaskMember>,
  topics: ReadonlySet<string>,
  metadata: { topics: ReadonlyMap<string, { partitionsCount: number }> },
): GroupPartitionsAssignments[] {
  const partitionCounts = new Map<string, number>()
  for (const topic of [...topics].sort()) {
    const count = metadata.topics.get(topic)?.partitionsCount
    if (count !== undefined) partitionCounts.set(topic, count)
  }
  const taskCount = Math.max(0, ...partitionCounts.values())
  const memberIds = [...members.keys()].sort()
  const { owners, claimants } = previousOwners(members)

  // Each member keeps the Tasks it owned (up to its share, below).
  const assigned = new Map<string, number[]>(memberIds.map((id) => [id, []]))
  for (let task = 0; task < taskCount; task++) {
    const owner = owners.get(task)
    if (owner !== undefined) assigned.get(owner)?.push(task)
  }

  // Fair shares: `base` Tasks each, one more for the `extra` members that have the most already.
  const base = memberIds.length === 0 ? 0 : Math.floor(taskCount / memberIds.length)
  const extra = memberIds.length === 0 ? 0 : taskCount % memberIds.length
  const byLoad = [...memberIds].sort(
    (a, b) => (assigned.get(b)?.length ?? 0) - (assigned.get(a)?.length ?? 0) || compare(a, b),
  )
  const share = new Map(byLoad.map((id, i) => [id, base + (i < extra ? 1 : 0)]))

  // Members over their share give up their highest Tasks; they and the unowned Tasks go to the
  // members under their share, lowest Task first.
  const free: number[] = []
  for (const id of memberIds) {
    const tasks = assigned.get(id) ?? []
    const limit = share.get(id) ?? 0
    if (tasks.length > limit) free.push(...tasks.splice(limit))
  }
  for (let task = 0; task < taskCount; task++) {
    if (!owners.has(task)) free.push(task)
  }
  free.sort((a, b) => a - b)
  for (const id of memberIds) {
    const tasks = assigned.get(id) ?? []
    const limit = share.get(id) ?? 0
    while (tasks.length < limit && free.length > 0) tasks.push(free.shift() as number)
  }

  // Cooperative: a Task another member still claims is only revoked in this rebalance; it is
  // assigned to its new member in the next one.
  for (const id of memberIds) {
    const tasks = assigned.get(id) ?? []
    const kept = tasks.filter(
      (task) => owners.get(task) === id || ![...(claimants.get(task) ?? [])].some((m) => m !== id),
    )
    assigned.set(id, kept)
  }

  return memberIds.map((memberId) => {
    const subscribed = new Set(members.get(memberId)?.topics ?? [])
    const tasks = (assigned.get(memberId) ?? []).sort((a, b) => a - b)
    const assignments = new Map<string, GroupAssignment>()
    for (const [topic, count] of partitionCounts) {
      if (!subscribed.has(topic)) continue
      const partitions = tasks.filter((task) => task < count)
      if (partitions.length > 0) assignments.set(topic, { topic, partitions })
    }
    return { memberId, assignments }
  })
}

/**
 * The previous owner of each Task, from the members' claims of the latest generation: a Task
 * claimed by exactly one member is owned by it. Also every member that claims each Task.
 */
function previousOwners(members: ReadonlyMap<string, TaskMember>): {
  owners: Map<number, string>
  claimants: Map<number, Set<string>>
} {
  const latest = Math.max(-1, ...[...members.values()].map((m) => m.generationId ?? -1))
  const claimants = new Map<number, Set<string>>()
  for (const [memberId, member] of members) {
    if ((member.generationId ?? -1) < latest) continue
    for (const { partitions } of member.ownedPartitions ?? []) {
      for (const task of partitions) {
        let claiming = claimants.get(task)
        if (claiming === undefined) {
          claiming = new Set()
          claimants.set(task, claiming)
        }
        claiming.add(memberId)
      }
    }
  }
  const owners = new Map<number, string>()
  for (const [task, claiming] of claimants) {
    if (claiming.size === 1) owners.set(task, [...claiming][0] as string)
  }
  return { owners, claimants }
}

function compare(a: string, b: string): number {
  return a < b ? -1 : a > b ? 1 : 0
}

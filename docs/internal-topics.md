# Internal Topics

An **Internal Topic** is a topic the library creates and owns for an application. In 0.1 these are **Changelogs**, one per State Store, named `<applicationId>-<store>-changelog` (Repartition Topics arrive with `groupBy` on a new key, which is not in 0.1). Each Changelog is compacted (`cleanup.policy=compact`) and has the same partition count as the app's input topics.

Because the name derives from the Application ID and the store name, renaming either points the app at a new, empty Changelog. Name your stores explicitly ([DSL](./dsl.md#aggregations)).

## Auto-creation

By default (`autoCreateInternalTopics: true`) the app creates missing Changelogs when it starts, with `changelogReplicationFactor` (default 3, use 1 on a single broker). An existing Internal Topic with the **wrong partition count is always a fatal error**; so is an input topic set that isn't Co-partitioned.

## Locked-down clusters

If your platform team owns topic creation, set `autoCreateInternalTopics: false`. The app then only checks the topics: `start()` fails with one error listing every missing Internal Topic, with its required partition count and `cleanup.policy=compact`, so you know exactly what to ask for. Create them with the same partition count as the input topics, a replication factor appropriate to your cluster, and `cleanup.policy=compact`.

## Other topics

Input and output topics, and dead-letter topics, are never created by the library. Create them yourself, with partition counts that match across the input topics.

# No wire or format compatibility with Java Kafka Streams

We don't match Java Kafka Streams' internal topic naming, changelog record formats, or checkpoint files, so a TypeScript app can't take over state from a Java app or the other way around. Matching would tie us to undocumented Java internals that change between releases, and that kind of migration is not a use case for our audience. Internal topics use our own documented scheme (e.g. `<appId>-<storeName>-changelog`).

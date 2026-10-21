# Local Kafka broker for tests (no Docker) — Kafka 4.4.0 KRaft on Java 17

Status: **verified working** on this machine (macOS arm64, 2026-10-21).
- Kafka **4.4.0** (`kafka_2.13-4.4.0.tgz`, latest 4.x on downloads.apache.org; also 4.3.2, 4.2.2 there)
  runs fine on the existing **OpenJDK 17.0.2** at `/usr/local/opt/openjdk/libexec/openjdk.jdk/Contents/Home`.
  (Kafka 4.x brokers require Java 17+.) Don't use the old 3.1.0 install.
- Downloaded + sha512-verified + extracted into `~/.cache/kstreams-ts/kafka_2.13-4.4.0/`.
  The `.sha512` file is in `gpg --print-md` format (uppercase hex in space-separated groups over
  several lines, prefixed `kafka_2.13-4.4.0.tgz:`), so normalise before comparing (script below does).
- Cold start ~17 s (incl. first format), warm restart ~7 s. Data dir ~20 MB.
- `@platformatic/kafka` README lists brokers 3.5.0–4.2.0 as CI-tested; all experiments in
  `kafka-client.md` (produce, group consume, cooperative rebalance, transactions, read_committed,
  compacted changelog, admin) passed against 4.4.0. If something odd shows up, try
  `KAFKA_VERSION=4.2.2 scripts/kafka-local.sh start` (needs `reset` first: different data dir format is
  not guaranteed compatible).

## Test-relevant broker settings (in the generated `server.properties`)
| setting | why |
|---|---|
| `process.roles=broker,controller`, `node.id=1`, `controller.quorum.bootstrap.servers=localhost:9093` | single-node KRaft combined mode |
| `listeners=PLAINTEXT://localhost:9092,CONTROLLER://localhost:9093` | bind loopback only |
| `offsets.topic.replication.factor=1`, `transaction.state.log.replication.factor=1`, `transaction.state.log.min.isr=1`, `share.coordinator.state.topic.*=1` | internal topics on 1 broker (txns fail otherwise) |
| `offsets.topic.num.partitions=1`, `transaction.state.log.num.partitions=1` | faster first group/txn use |
| `group.initial.rebalance.delay.ms=0` | no 3 s wait on first join |
| `auto.create.topics.enable=false` | tests must create topics explicitly (library does via Admin) |
| `default.replication.factor=1`, `min.insync.replicas=1`, `num.partitions=1` | sane defaults |
| `transaction.abort.timed.out.transaction.cleanup.interval.ms=5000`, `log.cleaner.backoff.ms=1000` | quicker housekeeping |
| `group.consumer.*` (session 10 s, heartbeat 1 s) | only matters for KIP-848 `groupProtocol: 'consumer'` |

Classic-protocol session timeout is a **client** option (`sessionTimeout`, default 60 s in
@platformatic/kafka) — tests should set e.g. `sessionTimeout: 10_000, heartbeatInterval: 1_000`,
otherwise a crashed member blocks reassignment for a minute.

## Commands (what the script does)
```sh
export JAVA_HOME=/usr/local/opt/openjdk/libexec/openjdk.jdk/Contents/Home
K=~/.cache/kstreams-ts/kafka_2.13-4.4.0
$K/bin/kafka-storage.sh format --standalone -t "$($K/bin/kafka-storage.sh random-uuid)" -c ~/.cache/kstreams-ts/run/server.properties
LOG_DIR=~/.cache/kstreams-ts/run/logs nohup $K/bin/kafka-server-start.sh ~/.cache/kstreams-ts/run/server.properties > ~/.cache/kstreams-ts/run/kafka.log 2>&1 &
$K/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092   # health check (exit 0 = up)
$K/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic smoke --partitions 3
kill <pid>    # SIGTERM = controlled shutdown (kafka-server-start.sh execs java, so $! is the JVM)
```
Verified: `start` (cold + warm), idempotent second `start`, `status` (exit 0 running / 1 stopped),
`stop`, topic create/describe via CLI, then Node produce/consume/transactions (kafka-client.md, exp1–7).

## Draft `scripts/kafka-local.sh` (POSIX sh; tested)
Env overrides: `KAFKA_VERSION`, `KSTREAMS_CACHE_DIR`, `KAFKA_PORT`, `KAFKA_CONTROLLER_PORT`, `JAVA_HOME`,
`KAFKA_HEAP_OPTS`. Subcommands `start|stop|status|reset` (`reset` = stop + wipe data dir).
Note: the health check spawns a JVM (~1–2 s each); fine for start-up, too slow for per-test checks
(tests can instead try `admin.listTopics()` with short timeout).

```sh
#!/bin/sh
# Single-node KRaft Kafka broker for local tests (no Docker).
# Usage: scripts/kafka-local.sh start|stop|status|reset
set -eu

KAFKA_VERSION="${KAFKA_VERSION:-4.4.0}"
SCALA_VERSION="2.13"
CACHE_DIR="${KSTREAMS_CACHE_DIR:-$HOME/.cache/kstreams-ts}"
KAFKA_NAME="kafka_${SCALA_VERSION}-${KAFKA_VERSION}"
KAFKA_HOME="$CACHE_DIR/$KAFKA_NAME"
RUN_DIR="$CACHE_DIR/run"
DATA_DIR="$RUN_DIR/data"
CONFIG="$RUN_DIR/server.properties"
LOG_FILE="$RUN_DIR/kafka.log"
PID_FILE="$RUN_DIR/kafka.pid"
PORT="${KAFKA_PORT:-9092}"
CONTROLLER_PORT="${KAFKA_CONTROLLER_PORT:-9093}"

# Kafka 4.x needs Java 17+. Override JAVA_HOME if needed.
if [ -z "${JAVA_HOME:-}" ] && [ -d /usr/local/opt/openjdk/libexec/openjdk.jdk/Contents/Home ]; then
  JAVA_HOME=/usr/local/opt/openjdk/libexec/openjdk.jdk/Contents/Home
fi
export JAVA_HOME

download() {
  [ -x "$KAFKA_HOME/bin/kafka-server-start.sh" ] && return 0
  mkdir -p "$CACHE_DIR"
  tgz="$CACHE_DIR/$KAFKA_NAME.tgz"
  if [ ! -f "$tgz" ]; then
    base="https://downloads.apache.org/kafka/$KAFKA_VERSION"
    curl -fsSL -o "$tgz.part" "$base/$KAFKA_NAME.tgz" \
      || curl -fsSL -o "$tgz.part" "https://archive.apache.org/dist/kafka/$KAFKA_VERSION/$KAFKA_NAME.tgz"
    curl -fsSL -o "$tgz.sha512" "$base/$KAFKA_NAME.tgz.sha512" \
      || curl -fsSL -o "$tgz.sha512" "https://archive.apache.org/dist/kafka/$KAFKA_VERSION/$KAFKA_NAME.tgz.sha512"
    # The .sha512 file is in gpg --print-md format (uppercase, grouped, multi-line).
    expected=$(sed 's/.*://' "$tgz.sha512" | tr -d ' \n\r' | tr 'A-F' 'a-f')
    actual=$(shasum -a 512 "$tgz.part" | cut -d' ' -f1)
    if [ "$expected" != "$actual" ]; then
      echo "sha512 mismatch for $KAFKA_NAME.tgz" >&2
      rm -f "$tgz.part"
      exit 1
    fi
    mv "$tgz.part" "$tgz"
  fi
  tar -xzf "$tgz" -C "$CACHE_DIR"
}

write_config() {
  mkdir -p "$RUN_DIR"
  cat > "$CONFIG" <<EOF
process.roles=broker,controller
node.id=1
controller.quorum.bootstrap.servers=localhost:$CONTROLLER_PORT
listeners=PLAINTEXT://localhost:$PORT,CONTROLLER://localhost:$CONTROLLER_PORT
advertised.listeners=PLAINTEXT://localhost:$PORT
inter.broker.listener.name=PLAINTEXT
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
log.dirs=$DATA_DIR
num.partitions=1
default.replication.factor=1
min.insync.replicas=1
offsets.topic.replication.factor=1
offsets.topic.num.partitions=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
transaction.state.log.num.partitions=1
share.coordinator.state.topic.replication.factor=1
share.coordinator.state.topic.min.isr=1
group.initial.rebalance.delay.ms=0
auto.create.topics.enable=false
delete.topic.enable=true
# Faster tests: shorter waits for txn/group housekeeping.
transaction.abort.timed.out.transaction.cleanup.interval.ms=5000
group.consumer.session.timeout.ms=10000
group.consumer.min.session.timeout.ms=5000
group.consumer.heartbeat.interval.ms=1000
group.consumer.min.heartbeat.interval.ms=1000
log.cleaner.backoff.ms=1000
EOF
}

is_running() {
  [ -f "$PID_FILE" ] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null
}

healthy() {
  "$KAFKA_HOME/bin/kafka-broker-api-versions.sh" --bootstrap-server "localhost:$PORT" >/dev/null 2>&1
}

start() {
  if is_running; then
    echo "kafka already running (pid $(cat "$PID_FILE")) on localhost:$PORT"
    return 0
  fi
  download
  write_config
  if [ ! -f "$DATA_DIR/meta.properties" ]; then
    cluster_id=$("$KAFKA_HOME/bin/kafka-storage.sh" random-uuid)
    "$KAFKA_HOME/bin/kafka-storage.sh" format --standalone -t "$cluster_id" -c "$CONFIG" >/dev/null
  fi
  # nohup + background; the JVM logs go to LOG_FILE (log4j also writes $KAFKA_HOME/logs).
  LOG_DIR="$RUN_DIR/logs" KAFKA_HEAP_OPTS="${KAFKA_HEAP_OPTS:--Xms256m -Xmx512m}" \
    nohup "$KAFKA_HOME/bin/kafka-server-start.sh" "$CONFIG" > "$LOG_FILE" 2>&1 &
  echo $! > "$PID_FILE"
  i=0
  until healthy; do
    i=$((i + 1))
    if ! is_running; then
      echo "kafka exited during startup; see $LOG_FILE" >&2
      tail -n 30 "$LOG_FILE" >&2
      exit 1
    fi
    if [ "$i" -ge 60 ]; then
      echo "kafka not healthy after 60s; see $LOG_FILE" >&2
      exit 1
    fi
    sleep 1
  done
  echo "kafka $KAFKA_VERSION running (pid $(cat "$PID_FILE")) on localhost:$PORT"
}

stop() {
  if ! is_running; then
    echo "kafka not running"
    rm -f "$PID_FILE"
    return 0
  fi
  pid=$(cat "$PID_FILE")
  # kafka-server-start.sh execs java, so $pid is the JVM.
  kill "$pid"
  i=0
  while kill -0 "$pid" 2>/dev/null; do
    i=$((i + 1))
    if [ "$i" -ge 30 ]; then kill -9 "$pid" 2>/dev/null || true; break; fi
    sleep 1
  done
  rm -f "$PID_FILE"
  echo "kafka stopped"
}

status() {
  if is_running && healthy; then
    echo "running (pid $(cat "$PID_FILE")) on localhost:$PORT"
  elif is_running; then
    echo "process running (pid $(cat "$PID_FILE")) but not healthy"; exit 1
  else
    echo "stopped"; exit 1
  fi
}

case "${1:-}" in
  start) start ;;
  stop) stop ;;
  status) status ;;
  reset) stop; rm -rf "$DATA_DIR" ;;
  *) echo "usage: $0 start|stop|status|reset" >&2; exit 2 ;;
esac
```

## Suggested test wiring
- Vitest `globalSetup` can run `scripts/kafka-local.sh start` (idempotent) — or require devs to run it
  and have integration tests skip with a clear message when `localhost:9092` is unreachable.
- Use unique topic/group/transactionalId names per test (`${name}-${randomUUID()}`); delete topics in
  teardown (`delete.topic.enable=true`).
- After `admin.createTopics`, wait for partition leaders and use idempotent producers in helpers
  (see the duplicate-on-retry gotcha in `kafka-client.md`).
- Raw copy of the script: `/Users/igorshalygin/.claude/jobs/e36bc646/tmp/explore/kafka-local/kafka-local.sh`.

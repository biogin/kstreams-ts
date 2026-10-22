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
    if ! "$KAFKA_HOME/bin/kafka-storage.sh" format --standalone -t "$cluster_id" -c "$CONFIG" >/dev/null 2>&1; then
      # Brokers without --standalone (before KIP-853) take a static single-voter quorum instead.
      sed "s/^controller.quorum.bootstrap.servers=.*/controller.quorum.voters=1@localhost:$CONTROLLER_PORT/" \
        "$CONFIG" > "$CONFIG.static" && mv "$CONFIG.static" "$CONFIG"
      "$KAFKA_HOME/bin/kafka-storage.sh" format -t "$cluster_id" -c "$CONFIG" >/dev/null
    fi
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

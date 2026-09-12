#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP_SERVERS="kafka-1:19092,kafka-2:19092,kafka-3:19092"
KAFKA_TOPICS="/opt/kafka/bin/kafka-topics.sh"

create_topic() {
  local topic="$1"
  local partitions="$2"
  local replication_factor="$3"
  local min_insync_replicas="$4"

  "$KAFKA_TOPICS" \
    --bootstrap-server "$BOOTSTRAP_SERVERS" \
    --create \
    --if-not-exists \
    --topic "$topic" \
    --partitions "$partitions" \
    --replication-factor "$replication_factor" \
    --config retention.ms=604800000 \
    --config "min.insync.replicas=$min_insync_replicas"

  "$KAFKA_TOPICS" \
    --bootstrap-server "$BOOTSTRAP_SERVERS" \
    --describe \
    --topic "$topic"
}

create_topic "coinbase.curated.trades" 3 3 2
create_topic "coinbase.invalid.trade_batches" 2 2 2

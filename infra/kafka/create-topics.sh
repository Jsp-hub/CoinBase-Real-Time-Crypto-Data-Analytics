#!/bin/sh
set -eu

BOOTSTRAP_SERVERS="kafka-1:19092,kafka-2:19092,kafka-3:19092"
TOPIC_NAME="coinbase.raw.trade_batches"

/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "$BOOTSTRAP_SERVERS" \
  --create \
  --if-not-exists \
  --topic "$TOPIC_NAME" \
  --partitions 3 \
  --replication-factor 3 \
  --config retention.ms=604800000 \
  --config min.insync.replicas=2

echo "Created or confirmed topic: $TOPIC_NAME"

/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "$BOOTSTRAP_SERVERS" \
  --describe \
  --topic "$TOPIC_NAME"

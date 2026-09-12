# Spark Trade Curator

`trade_curator.py` converts raw Coinbase batches into validated, one-trade Kafka records.

```text
coinbase.raw.trade_batches
  -> Spark curator
  -> coinbase.curated.trades
  -> coinbase.invalid.trade_batches
```

## Run

From the project root:

```powershell
docker compose -f infra/docker/docker-compose.yml -f infra/docker/docker-compose.spark.yml up -d --build
```

## Verify

```powershell
docker compose -f infra/docker/docker-compose.yml -f infra/docker/docker-compose.spark.yml ps
```

Expected: Kafka brokers are healthy; Spark master, worker, and curator are running; topic initializer services exit with code `0`.

Consume newly curated trades:

```powershell
docker exec -it coinbase-kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka-1:19092 --topic coinbase.curated.trades --property print.key=true --property print.partition=true --property print.offset=true
```

## Behavior

- Valid trade output includes `trade_time`, decimal `price` and `size`, `notional_usd`, and raw Kafka provenance.
- Invalid records go to `coinbase.invalid.trade_batches`.
- Use `trade_time` for financial ordering. Spark output order is not guaranteed to match raw Kafka offsets after flattening and deduplication.
- Spark runs a 10-second micro-batch trigger, so expected curation latency is roughly 0–10 seconds.

## Stop

```powershell
docker compose -f infra/docker/docker-compose.yml -f infra/docker/docker-compose.spark.yml down
```

This preserves Kafka records and Spark checkpoints. Do not add `--volumes` unless a full data reset is intended.

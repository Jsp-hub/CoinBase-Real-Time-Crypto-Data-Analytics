# Flink Candle Analytics

Creates corrected UTC one-minute candles from `coinbase.curated.trades`.

```text
coinbase.curated.trades
  -> Flink event-time windows
  -> coinbase.analytics.candles_1m
```

Configuration: 30-second watermark delay, 30-second allowed lateness, one JobManager, one TaskManager, and two task slots.

## Run

Start Kafka first so its shared Docker network exists:

```powershell
docker compose -f infra/docker/docker-compose.yml up -d
```

Then start Spark and Flink:

```powershell
docker compose -f infra/docker/docker-compose.yml -f infra/docker/docker-compose.spark.yml -f infra/docker/docker-compose.flink.yml up -d --build
```

## Verify

```powershell
docker exec -it coinbase-kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka-1:19092 --topic coinbase.analytics.candles_1m --property print.partition=true --property print.offset=true
```

Late records beyond the window expiry are written to `coinbase.analytics.late_trades`.

## Stop

```powershell
docker compose -f infra/docker/docker-compose.yml -f infra/docker/docker-compose.spark.yml -f infra/docker/docker-compose.flink.yml stop
```

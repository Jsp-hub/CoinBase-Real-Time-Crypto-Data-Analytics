# Coinbase trade listener

This service connects to Coinbase's public Advanced Trade WebSocket and
publishes untouched `market_trades` payloads to Kafka. It does not flatten,
validate, or otherwise transform the Coinbase JSON; Spark will perform those
steps later.

Each running listener accepts one configured product and publishes one Kafka
record per Coinbase WebSocket batch:

```text
Kafka topic: coinbase.raw.trade_batches
Kafka key: configured product ID
Kafka value: untouched Coinbase JSON payload
```

## Run locally

From the project root:

```powershell
python -m pip install -r services/ingestion/requirements.txt
python services/ingestion/coinbase_trade_listener.py --product BTC-USD --print-raw
```

Run one listener process for each product. Use a separate terminal for each
command:

```powershell
python services/ingestion/coinbase_trade_listener.py --product BTC-USD
python services/ingestion/coinbase_trade_listener.py --product ETH-USD
python services/ingestion/coinbase_trade_listener.py --product SOL-USD
```

Stop the listener with `Ctrl+C`.

## Kafka prerequisites

Start the local Kafka cluster before running a listener:

```powershell
docker compose -f infra/docker/docker-compose.yml up -d
```

The default bootstrap servers are:

```text
localhost:29092,localhost:39092,localhost:49092
```

To use another Kafka cluster or topic, pass explicit arguments:

```powershell
python services/ingestion/coinbase_trade_listener.py --product ETH-USD --kafka-bootstrap-servers broker-a:9092,broker-b:9092 --kafka-topic coinbase.raw.trade_batches
```

## Verify raw batches in Kafka

Run a read-only console consumer from the first broker:

```powershell
docker compose -f infra/docker/docker-compose.yml exec kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka-1:19092,kafka-2:19092,kafka-3:19092 --topic coinbase.raw.trade_batches --from-beginning --property print.key=true --property key.separator=" | "
```

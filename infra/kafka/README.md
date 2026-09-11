# Kafka foundation

This Docker Compose configuration creates a local Apache Kafka KRaft cluster.
Each of the three containers is both a broker and a controller, which gives the
project three brokers without requiring ZooKeeper.

## Cluster design

| Component | Configuration |
|---|---|
| Kafka image | `apache/kafka:4.3.1` |
| Brokers | `kafka-1`, `kafka-2`, `kafka-3` |
| Host bootstrap servers | `localhost:29092,localhost:39092,localhost:49092` |
| Internal bootstrap servers | `kafka-1:19092,kafka-2:19092,kafka-3:19092` |
| Storage | One named Docker volume per broker |
| Volume preparation | One-time containers set the Kafka runtime user as volume owner |
| Topic | `coinbase.raw.trade_batches` |
| Partitions | 3 |
| Replication factor | 3 |
| Minimum in-sync replicas | 2 |
| Retention | 7 days |

The topic initializer runs after all three brokers are healthy. It creates the
raw batch topic if needed, then prints the topic description and exits.

## Start the cluster

Run these commands manually from the project root:

```powershell
docker compose -f infra/docker/docker-compose.yml up -d
docker compose -f infra/docker/docker-compose.yml ps
docker compose -f infra/docker/docker-compose.yml logs kafka-topic-initializer
```

The topic description should show three partitions and three replicas for each
partition.

## Verify the topic manually

```powershell
docker compose -f infra/docker/docker-compose.yml exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:19092,kafka-2:19092,kafka-3:19092 --describe --topic coinbase.raw.trade_batches
```

## Stop the cluster

```powershell
docker compose -f infra/docker/docker-compose.yml down
```

`down` stops containers but keeps the named Kafka volumes. Do not use
`docker compose down --volumes` unless you intentionally want to delete all
locally retained Kafka data.

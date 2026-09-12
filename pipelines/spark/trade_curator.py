"""Convert raw Coinbase trade batches into validated, trade-level Kafka events."""

from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import ArrayType, DecimalType, LongType, StringType, StructField, StructType


RAW_TOPIC = os.getenv("RAW_TOPIC", "coinbase.raw.trade_batches")
CURATED_TOPIC = os.getenv("CURATED_TOPIC", "coinbase.curated.trades")
INVALID_TOPIC = os.getenv("INVALID_TOPIC", "coinbase.invalid.trade_batches")
BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka-1:19092,kafka-2:19092,kafka-3:19092",
)
CHECKPOINT_ROOT = os.getenv("CHECKPOINT_ROOT", "/opt/spark/checkpoints/trade-curator")

TRADE_SCHEMA = StructType(
    [
        StructField("product_id", StringType(), True),
        StructField("trade_id", StringType(), True),
        StructField("price", StringType(), True),
        StructField("size", StringType(), True),
        StructField("time", StringType(), True),
        StructField("side", StringType(), True),
    ]
)

RAW_BATCH_SCHEMA = StructType(
    [
        StructField("channel", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("sequence_num", LongType(), True),
        StructField(
            "events",
            ArrayType(
                StructType(
                    [
                        StructField("type", StringType(), True),
                        StructField("trades", ArrayType(TRADE_SCHEMA), True),
                    ]
                )
            ),
            True,
        ),
    ]
)


def read_raw_batches(spark: SparkSession) -> DataFrame:
    """Read raw producer messages and preserve their Kafka provenance."""
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", RAW_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
        .select(
            F.col("key").cast("string").alias("kafka_key"),
            F.col("value").cast("string").alias("raw_payload"),
            F.col("topic").alias("kafka_topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_received_at"),
            F.from_json(F.col("value").cast("string"), RAW_BATCH_SCHEMA).alias("batch"),
        )
    )


def normalize_trades(raw_batches: DataFrame) -> DataFrame:
    """Flatten source batches and identify records that cannot be curated safely."""
    trades = (
        raw_batches.select(
            "kafka_key",
            "raw_payload",
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_received_at",
            "batch",
            F.explode_outer("batch.events").alias("event"),
        )
        .select(
            "kafka_key",
            "raw_payload",
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_received_at",
            F.col("batch.channel").alias("channel"),
            F.col("batch.timestamp").alias("batch_timestamp"),
            F.col("batch.sequence_num").alias("sequence_num"),
            F.col("event.type").alias("event_type"),
            F.explode_outer("event.trades").alias("trade"),
        )
        .select(
            "kafka_key",
            "raw_payload",
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_received_at",
            "channel",
            "batch_timestamp",
            "sequence_num",
            "event_type",
            F.col("trade.product_id").alias("product_id"),
            F.col("trade.trade_id").alias("trade_id"),
            F.col("trade.price").alias("price_raw"),
            F.col("trade.size").alias("size_raw"),
            F.col("trade.time").alias("trade_time_raw"),
            F.col("trade.side").alias("side"),
        )
        .withColumn("price", F.expr("try_cast(price_raw AS DECIMAL(20, 8))"))
        .withColumn("size", F.expr("try_cast(size_raw AS DECIMAL(20, 8))"))
        .withColumn("trade_time", F.to_timestamp("trade_time_raw"))
        .withColumn(
            "notional_usd",
            (F.col("price") * F.col("size")).cast(DecimalType(28, 8)),
        )
        .withColumn("curated_at", F.current_timestamp())
    )

    return trades.withColumn(
        "invalid_reason",
        F.concat_ws(
            "; ",
            F.when(F.col("channel") != "market_trades", "unsupported_or_missing_channel"),
            F.when(F.col("product_id").isNull() | (F.length("product_id") == 0), "missing_product_id"),
            F.when(F.col("trade_id").isNull() | (F.length("trade_id") == 0), "missing_trade_id"),
            F.when(F.col("price").isNull() | (F.col("price") <= 0), "invalid_price"),
            F.when(F.col("size").isNull() | (F.col("size") <= 0), "invalid_size"),
            F.when(F.col("trade_time").isNull(), "invalid_trade_time"),
            F.when(~F.col("side").isin("BUY", "SELL"), "invalid_side"),
            F.when(
                F.col("kafka_key").isNotNull() & (F.col("kafka_key") != F.col("product_id")),
                "product_key_mismatch",
            ),
        ),
    )


def kafka_payload(frame: DataFrame, fields: list[str], key_column: str) -> DataFrame:
    """Encode selected columns as a keyed Kafka record."""
    return frame.select(
        F.col(key_column).cast("string").alias("key"),
        F.to_json(F.struct(*(F.col(field) for field in fields))).alias("value"),
    )


def main() -> None:
    spark = SparkSession.builder.appName("coinbase-trade-curator").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    normalized = normalize_trades(read_raw_batches(spark))

    valid_trades = (
        normalized.filter(F.col("invalid_reason") == "")
        .withWatermark("trade_time", "30 minutes")
        .dropDuplicates(["product_id", "trade_id"])
    )
    invalid_trades = normalized.filter(F.col("invalid_reason") != "")

    curated_fields = [
        "product_id",
        "trade_id",
        "trade_time",
        "price",
        "size",
        "notional_usd",
        "side",
        "batch_timestamp",
        "sequence_num",
        "event_type",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_received_at",
        "curated_at",
    ]
    invalid_fields = [
        "invalid_reason",
        "raw_payload",
        "kafka_key",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_received_at",
        "batch_timestamp",
        "sequence_num",
        "event_type",
        "product_id",
        "trade_id",
        "price_raw",
        "size_raw",
        "trade_time_raw",
        "side",
        "curated_at",
    ]

    curated_query = (
        kafka_payload(valid_trades, curated_fields, "product_id")
        .writeStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("topic", CURATED_TOPIC)
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/curated")
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .start()
    )
    invalid_query = (
        kafka_payload(invalid_trades, invalid_fields, "kafka_key")
        .writeStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("topic", INVALID_TOPIC)
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/invalid")
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .start()
    )

    spark.streams.awaitAnyTermination()
    curated_query.awaitTermination()
    invalid_query.awaitTermination()


if __name__ == "__main__":
    main()

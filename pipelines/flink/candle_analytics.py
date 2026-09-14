"""Create event-time one-minute Coinbase market candles with PyFlink."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from pyflink.common import Duration, Time, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.checkpoint_storage import FileSystemCheckpointStorage
from pyflink.datastream.connectors.kafka import (
    DeliveryGuarantee,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
    KafkaOffsetsInitializer,
)
from pyflink.datastream.functions import AggregateFunction, KeyedProcessFunction, ProcessWindowFunction
from pyflink.datastream.output_tag import OutputTag
from pyflink.datastream.state import StateTtlConfig, ValueStateDescriptor
from pyflink.datastream.window import TumblingEventTimeWindows


UTC = timezone.utc


BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka-1:19092,kafka-2:19092,kafka-3:19092",
)
INPUT_TOPIC = os.getenv("CURATED_TOPIC", "coinbase.curated.trades")
CANDLE_TOPIC = os.getenv("CANDLE_TOPIC", "coinbase.analytics.candles_1m")
LATE_TOPIC = os.getenv("LATE_TOPIC", "coinbase.analytics.late_trades")
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "file:///opt/flink/checkpoints/candle-analytics")
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "flink-candle-analytics-v1")

WATERMARK_DELAY_SECONDS = 30
ALLOWED_LATENESS_SECONDS = 30
IDLE_PARTITION_SECONDS = 60


def parse_timestamp(value: str) -> tuple[datetime, int]:
    """Return a UTC datetime and epoch milliseconds from an ISO-8601 timestamp."""
    timestamp = datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
    return timestamp, int(timestamp.timestamp() * 1000)


def parse_trade(raw_message: str) -> dict[str, Any]:
    """Parse a curated Kafka value while retaining exact decimal values."""
    payload = json.loads(raw_message)
    trade_time, trade_time_ms = parse_timestamp(payload["trade_time"])
    return {
        "product_id": str(payload["product_id"]),
        "trade_id": str(payload["trade_id"]),
        "trade_time": trade_time,
        "trade_time_ms": trade_time_ms,
        "price": Decimal(str(payload["price"])),
        "size": Decimal(str(payload["size"])),
        "notional_usd": Decimal(str(payload["notional_usd"])),
        "side": str(payload["side"]),
    }


def format_timestamp(epoch_millis: int) -> str:
    """Format a UTC epoch-millisecond value for an analytics event."""
    return datetime.fromtimestamp(epoch_millis / 1000, tz=UTC).isoformat().replace("+00:00", "Z")


class DeduplicateTrade(KeyedProcessFunction):
    """Pass a product/trade ID only once within a bounded state TTL."""

    def open(self, runtime_context) -> None:
        descriptor = ValueStateDescriptor("seen_trade", Types.BOOLEAN())
        ttl = (
            StateTtlConfig.new_builder(Time.hours(2))
            .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .set_state_visibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .build()
        )
        descriptor.enable_time_to_live(ttl)
        self.seen_trade = runtime_context.get_state(descriptor)

    def process_element(self, value: dict[str, Any], context):
        if not self.seen_trade.value():
            self.seen_trade.update(True)
            yield value


class CandleAggregate(AggregateFunction):
    """Maintain OHLC, count, and volume state for one product/window."""

    @staticmethod
    def _empty() -> dict[str, Any]:
        return {
            "open_price": None,
            "open_marker": None,
            "high_price": None,
            "low_price": None,
            "close_price": None,
            "close_marker": None,
            "trade_count": 0,
            "base_volume": Decimal("0"),
            "notional_volume_usd": Decimal("0"),
        }

    def create_accumulator(self) -> dict[str, Any]:
        return self._empty()

    def add(self, value: dict[str, Any], accumulator: dict[str, Any]) -> dict[str, Any]:
        marker = (value["trade_time_ms"], value["trade_id"])
        price = value["price"]

        if accumulator["open_marker"] is None or marker < accumulator["open_marker"]:
            accumulator["open_marker"] = marker
            accumulator["open_price"] = price
        if accumulator["close_marker"] is None or marker > accumulator["close_marker"]:
            accumulator["close_marker"] = marker
            accumulator["close_price"] = price
        if accumulator["high_price"] is None or price > accumulator["high_price"]:
            accumulator["high_price"] = price
        if accumulator["low_price"] is None or price < accumulator["low_price"]:
            accumulator["low_price"] = price

        accumulator["trade_count"] += 1
        accumulator["base_volume"] += value["size"]
        accumulator["notional_volume_usd"] += value["notional_usd"]
        return accumulator

    def get_result(self, accumulator: dict[str, Any]) -> dict[str, Any]:
        return accumulator

    def merge(self, first: dict[str, Any], second: dict[str, Any]) -> dict[str, Any]:
        merged = self._empty()
        for candidate in (first, second):
            if candidate["open_marker"] is not None and (
                merged["open_marker"] is None or candidate["open_marker"] < merged["open_marker"]
            ):
                merged["open_marker"] = candidate["open_marker"]
                merged["open_price"] = candidate["open_price"]
            if candidate["close_marker"] is not None and (
                merged["close_marker"] is None or candidate["close_marker"] > merged["close_marker"]
            ):
                merged["close_marker"] = candidate["close_marker"]
                merged["close_price"] = candidate["close_price"]
            if candidate["high_price"] is not None and (
                merged["high_price"] is None or candidate["high_price"] > merged["high_price"]
            ):
                merged["high_price"] = candidate["high_price"]
            if candidate["low_price"] is not None and (
                merged["low_price"] is None or candidate["low_price"] < merged["low_price"]
            ):
                merged["low_price"] = candidate["low_price"]
            merged["trade_count"] += candidate["trade_count"]
            merged["base_volume"] += candidate["base_volume"]
            merged["notional_volume_usd"] += candidate["notional_volume_usd"]
        return merged


class FormatCandle(ProcessWindowFunction):
    """Add the product and one-minute window boundaries to an aggregate."""

    def process(self, key: str, context, aggregates):
        aggregate = next(iter(aggregates))
        base_volume = aggregate["base_volume"]
        notional_volume = aggregate["notional_volume_usd"]
        window_start = context.window().start
        window_end = context.window().end

        yield {
            "candle_key": f"{key}|{format_timestamp(window_start)}",
            "product_id": key,
            "window_start": format_timestamp(window_start),
            "window_end": format_timestamp(window_end),
            "open_price": str(aggregate["open_price"]),
            "high_price": str(aggregate["high_price"]),
            "low_price": str(aggregate["low_price"]),
            "close_price": str(aggregate["close_price"]),
            "trade_count": aggregate["trade_count"],
            "base_volume": str(base_volume),
            "notional_volume_usd": str(notional_volume),
            "vwap": str(notional_volume / base_volume),
            "emitted_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        }


def json_message(value: dict[str, Any]) -> str:
    """Serialize Decimal-containing records without losing precision."""
    return json.dumps(value, default=str, separators=(",", ":"))


def kafka_sink(topic: str) -> KafkaSink:
    """Build an at-least-once Kafka sink for JSON analytics events."""
    serializer = (
        KafkaRecordSerializationSchema.builder()
        .set_topic(topic)
        .set_value_serialization_schema(SimpleStringSchema())
        .build()
    )
    return (
        KafkaSink.builder()
        .set_bootstrap_servers(BOOTSTRAP_SERVERS)
        .set_record_serializer(serializer)
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )


def main() -> None:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(2)
    env.enable_checkpointing(30_000)
    env.get_checkpoint_config().set_checkpoint_storage(FileSystemCheckpointStorage(CHECKPOINT_DIR))

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(BOOTSTRAP_SERVERS)
        .set_topics(INPUT_TOPIC)
        .set_group_id(CONSUMER_GROUP)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )
    watermarks = (
        WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(WATERMARK_DELAY_SECONDS))
        .with_timestamp_assigner(lambda trade, _: trade["trade_time_ms"])
        .with_idleness(Duration.of_seconds(IDLE_PARTITION_SECONDS))
    )
    late_trades = OutputTag("late-trades", Types.PICKLED_BYTE_ARRAY())

    trades = env.from_source(source, WatermarkStrategy.no_watermarks(), "curated-trades-source").map(
        parse_trade,
        output_type=Types.PICKLED_BYTE_ARRAY(),
    )
    unique_trades = trades.key_by(
        lambda trade: f"{trade['product_id']}|{trade['trade_id']}"
    ).process(DeduplicateTrade(), output_type=Types.PICKLED_BYTE_ARRAY())
    timestamped_trades = unique_trades.assign_timestamps_and_watermarks(watermarks)

    candles = (
        timestamped_trades.key_by(lambda trade: trade["product_id"])
        .window(TumblingEventTimeWindows.of(Time.minutes(1)))
        .allowed_lateness(ALLOWED_LATENESS_SECONDS * 1000)
        .side_output_late_data(late_trades)
        .aggregate(CandleAggregate(), FormatCandle(), output_type=Types.PICKLED_BYTE_ARRAY())
    )

    candles.map(json_message, output_type=Types.STRING()).sink_to(kafka_sink(CANDLE_TOPIC))
    candles.get_side_output(late_trades).map(
        json_message,
        output_type=Types.STRING(),
    ).sink_to(kafka_sink(LATE_TOPIC))

    env.execute("flink-candle-analytics")


if __name__ == "__main__":
    main()

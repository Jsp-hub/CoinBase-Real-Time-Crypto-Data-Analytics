"""Persist Flink one-minute candles from Kafka into the local MySQL serving table."""

from __future__ import annotations

import json
import logging
import os
import signal
import time
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

import mysql.connector
from confluent_kafka import Consumer, KafkaError, Message
from mysql.connector import Error as MySQLError


LOGGER = logging.getLogger(__name__)
UTC = timezone.utc
RETRY_DELAY_SECONDS = 5

UPSERT_CANDLE = """
INSERT INTO market_candles_1m (
    product_id,
    window_start,
    window_end,
    open_price,
    high_price,
    low_price,
    close_price,
    trade_count,
    base_volume,
    notional_volume_usd,
    vwap,
    emitted_at,
    candle_status,
    last_updated_at
) VALUES (
    %(product_id)s,
    %(window_start)s,
    %(window_end)s,
    %(open_price)s,
    %(high_price)s,
    %(low_price)s,
    %(close_price)s,
    %(trade_count)s,
    %(base_volume)s,
    %(notional_volume_usd)s,
    %(vwap)s,
    %(emitted_at)s,
    %(candle_status)s,
    %(last_updated_at)s
)
ON DUPLICATE KEY UPDATE
    window_end = IF(VALUES(candle_status) = 'FORMING' AND candle_status = 'FINAL', window_end, VALUES(window_end)),
    open_price = IF(VALUES(candle_status) = 'FORMING' AND candle_status = 'FINAL', open_price, VALUES(open_price)),
    high_price = IF(VALUES(candle_status) = 'FORMING' AND candle_status = 'FINAL', high_price, VALUES(high_price)),
    low_price = IF(VALUES(candle_status) = 'FORMING' AND candle_status = 'FINAL', low_price, VALUES(low_price)),
    close_price = IF(VALUES(candle_status) = 'FORMING' AND candle_status = 'FINAL', close_price, VALUES(close_price)),
    trade_count = IF(VALUES(candle_status) = 'FORMING' AND candle_status = 'FINAL', trade_count, VALUES(trade_count)),
    base_volume = IF(VALUES(candle_status) = 'FORMING' AND candle_status = 'FINAL', base_volume, VALUES(base_volume)),
    notional_volume_usd = IF(VALUES(candle_status) = 'FORMING' AND candle_status = 'FINAL', notional_volume_usd, VALUES(notional_volume_usd)),
    vwap = IF(VALUES(candle_status) = 'FORMING' AND candle_status = 'FINAL', vwap, VALUES(vwap)),
    emitted_at = IF(VALUES(candle_status) = 'FORMING' AND candle_status = 'FINAL', emitted_at, VALUES(emitted_at)),
    last_updated_at = IF(VALUES(candle_status) = 'FORMING' AND candle_status = 'FINAL', last_updated_at, VALUES(last_updated_at)),
    candle_status = IF(candle_status = 'FINAL' AND VALUES(candle_status) = 'FORMING', 'FINAL', VALUES(candle_status))
"""


def environment(name: str, default: str | None = None) -> str:
    """Return a required environment setting, optionally with a local default."""
    value = os.getenv(name, default)
    if value is None or not value.strip():
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def parse_utc_timestamp(value: object, field_name: str) -> datetime:
    """Parse an ISO-8601 timestamp and return a naive UTC value for MySQL."""
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be an ISO-8601 string")

    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError(f"{field_name} is not a valid ISO-8601 timestamp") from error

    if parsed.tzinfo is None:
        raise ValueError(f"{field_name} must include a timezone")

    return parsed.astimezone(UTC).replace(tzinfo=None)


def parse_decimal(payload: dict[str, object], field_name: str) -> Decimal:
    """Return a finite decimal field from the candle payload."""
    value = payload.get(field_name)
    try:
        decimal_value = Decimal(str(value))
    except (InvalidOperation, ValueError) as error:
        raise ValueError(f"{field_name} must be a decimal value") from error

    if not decimal_value.is_finite():
        raise ValueError(f"{field_name} must be finite")
    return decimal_value


def parse_candle(message: Message) -> dict[str, object]:
    """Validate and normalize one Flink candle message for the MySQL schema."""
    try:
        payload = json.loads(message.value().decode("utf-8"))
    except (AttributeError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("Kafka value is not valid UTF-8 JSON") from error

    if not isinstance(payload, dict):
        raise ValueError("Candle payload must be a JSON object")

    product_id = payload.get("product_id")
    if not isinstance(product_id, str) or not product_id.strip():
        raise ValueError("product_id must be a non-empty string")
    if len(product_id) > 20:
        raise ValueError("product_id exceeds the MySQL column length")

    trade_count = payload.get("trade_count")
    if not isinstance(trade_count, int) or isinstance(trade_count, bool) or trade_count < 1:
        raise ValueError("trade_count must be a positive integer")

    candle_status = payload.get("candle_status")
    if candle_status not in ("FORMING", "FINAL"):
        raise ValueError("candle_status must be FORMING or FINAL")

    candle = {
        "product_id": product_id,
        "window_start": parse_utc_timestamp(payload.get("window_start"), "window_start"),
        "window_end": parse_utc_timestamp(payload.get("window_end"), "window_end"),
        "open_price": parse_decimal(payload, "open_price"),
        "high_price": parse_decimal(payload, "high_price"),
        "low_price": parse_decimal(payload, "low_price"),
        "close_price": parse_decimal(payload, "close_price"),
        "trade_count": trade_count,
        "base_volume": parse_decimal(payload, "base_volume"),
        "notional_volume_usd": parse_decimal(payload, "notional_volume_usd"),
        "vwap": parse_decimal(payload, "vwap"),
        "emitted_at": parse_utc_timestamp(payload.get("emitted_at"), "emitted_at"),
        "candle_status": candle_status,
        "last_updated_at": parse_utc_timestamp(
            payload.get("last_updated_at"),
            "last_updated_at",
        ),
    }

    if candle["window_end"] <= candle["window_start"]:
        raise ValueError("window_end must be later than window_start")
    if candle["base_volume"] <= 0:
        raise ValueError("base_volume must be positive")

    return candle


def database_connection() -> mysql.connector.MySQLConnection:
    """Open a non-autocommit MySQL connection using container configuration."""
    return mysql.connector.connect(
        host=environment("MYSQL_HOST"),
        port=int(environment("MYSQL_PORT", "3306")),
        database=environment("MYSQL_DATABASE"),
        user=environment("MYSQL_USER"),
        password=environment("MYSQL_PASSWORD"),
        autocommit=False,
    )


def write_candle_with_retry(candle: dict[str, object]) -> None:
    """Upsert a candle, retrying database outages before the Kafka offset commits."""
    while True:
        connection: mysql.connector.MySQLConnection | None = None
        cursor = None
        try:
            connection = database_connection()
            cursor = connection.cursor()
            cursor.execute(UPSERT_CANDLE, candle)
            connection.commit()
            return
        except MySQLError:
            LOGGER.exception(
                "MySQL write failed; retrying the same Kafka message in %s seconds.",
                RETRY_DELAY_SECONDS,
            )
            if connection is not None:
                connection.rollback()
            time.sleep(RETRY_DELAY_SECONDS)
        finally:
            if cursor is not None:
                cursor.close()
            if connection is not None and connection.is_connected():
                connection.close()


def consumer_configuration() -> dict[str, object]:
    """Create the non-auto-commit Kafka consumer configuration."""
    return {
        "bootstrap.servers": environment("KAFKA_BOOTSTRAP_SERVERS"),
        "group.id": environment("KAFKA_GROUP_ID"),
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "isolation.level": "read_committed",
    }


def main() -> None:
    """Read candle events, upsert them, then commit their Kafka offsets."""
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    consumer = Consumer(consumer_configuration())
    topics = (
        environment("KAFKA_TOPIC"),
        environment("FORMING_CANDLE_TOPIC"),
    )
    stop_requested = False

    def request_stop(*_: object) -> None:
        nonlocal stop_requested
        LOGGER.info("Stop requested; no additional Kafka messages will be read.")
        stop_requested = True

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)
    consumer.subscribe(list(topics))
    LOGGER.info("Consuming %s into MySQL.", ", ".join(topics))

    try:
        while not stop_requested:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError(f"Kafka consumer error: {message.error()}")

            try:
                candle = parse_candle(message)
            except ValueError:
                LOGGER.exception(
                    "Refusing to commit malformed candle at %s[%s] offset %s.",
                    message.topic(),
                    message.partition(),
                    message.offset(),
                )
                raise

            write_candle_with_retry(candle)
            consumer.commit(message=message, asynchronous=False)
            LOGGER.info(
                "Upserted %s candle beginning %s from %s[%s] offset %s.",
                candle["product_id"],
                candle["window_start"],
                message.topic(),
                message.partition(),
                message.offset(),
            )
    finally:
        consumer.close()
        LOGGER.info("MySQL candle writer stopped.")


if __name__ == "__main__":
    main()

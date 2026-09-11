"""Listen to Coinbase public market trades and print each raw payload.

This is the first, deliberately small ingestion milestone. It has no Kafka
dependency yet: its purpose is to inspect Coinbase's source schema before we
define, transform, or publish our own event contract.
"""

from __future__ import annotations

import argparse
import json
import logging
import signal
import threading

import websocket

from kafka_producer import RawBatchProducer


WEBSOCKET_URL = "wss://advanced-trade-ws.coinbase.com"
RECONNECT_DELAY_SECONDS = 5

LOGGER = logging.getLogger(__name__)


def subscriptions(product: str) -> tuple[dict[str, object], dict[str, object]]:
    """Create the separate public subscriptions required by Coinbase."""
    return (
        {
            "type": "subscribe",
            "channel": "market_trades",
            "product_ids": [product],
        },
        {
            "type": "subscribe",
            "channel": "heartbeats",
        },
    )


def handle_message(
    _: websocket.WebSocketApp,
    message: str,
    producer: RawBatchProducer,
    product: str,
    print_raw: bool,
) -> None:
    """Publish an untouched Coinbase market-trades message to Kafka."""
    try:
        payload = json.loads(message)
    except json.JSONDecodeError:
        LOGGER.warning("Ignoring invalid JSON message: %s", message[:200])
        return

    if payload.get("channel") != "market_trades":
        return

    producer.publish(product, message)
    if print_raw:
        print(message, flush=True)


def run_listener(product: str, producer: RawBatchProducer, print_raw: bool) -> None:
    """Listen to one product indefinitely, reconnecting after network failures."""
    trade_subscription, heartbeat_subscription = subscriptions(product)
    stop_requested = threading.Event()
    active_socket: websocket.WebSocketApp | None = None

    def request_stop(_: int, __: object) -> None:
        """Close the active connection so the reconnect loop cannot restart it."""
        nonlocal active_socket
        LOGGER.info("Stop requested. Closing the listener after this connection ends.")
        stop_requested.set()
        if active_socket is not None:
            active_socket.close()

    signal.signal(signal.SIGINT, request_stop)

    def on_open(socket: websocket.WebSocketApp) -> None:
        LOGGER.info("Connected. Subscribing to market trades for %s", product)
        socket.send(json.dumps(trade_subscription))
        socket.send(json.dumps(heartbeat_subscription))

    def on_message(socket: websocket.WebSocketApp, message: str) -> None:
        handle_message(socket, message, producer, product, print_raw)

    def on_error(_: websocket.WebSocketApp, error: Exception) -> None:
        LOGGER.warning(
            "WebSocket error (%s): %r",
            type(error).__name__,
            error,
        )

    def on_close(_: websocket.WebSocketApp, status_code: int | None, message: str | None) -> None:
        LOGGER.warning("WebSocket closed (status=%s, message=%s)", status_code, message)

    while not stop_requested.is_set():
        active_socket = websocket.WebSocketApp(
            WEBSOCKET_URL,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
        )
        try:
            active_socket.run_forever(ping_interval=30, ping_timeout=10)
        except KeyboardInterrupt:
            stop_requested.set()
        finally:
            active_socket.close()
            active_socket = None

        if stop_requested.is_set():
            break

        LOGGER.info("Reconnecting in %s seconds...", RECONNECT_DELAY_SECONDS)
        stop_requested.wait(RECONNECT_DELAY_SECONDS)

    LOGGER.info("Listener stopped by user.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Print public Coinbase market trades.")
    parser.add_argument(
        "--product",
        required=True,
        type=str.upper,
        help="One Coinbase product ID, such as BTC-USD.",
    )
    parser.add_argument(
        "--log-level",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
        default="INFO",
        help="Logging verbosity (default: INFO).",
    )
    parser.add_argument(
        "--kafka-bootstrap-servers",
        default="localhost:29092,localhost:39092,localhost:49092",
        help="Comma-separated Kafka bootstrap servers for the local cluster.",
    )
    parser.add_argument(
        "--kafka-topic",
        default="coinbase.raw.trade_batches",
        help="Kafka topic that receives untouched Coinbase batches.",
    )
    parser.add_argument(
        "--print-raw",
        action="store_true",
        help="Also print each raw Coinbase market-trades batch after publishing it.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    producer = RawBatchProducer(args.kafka_bootstrap_servers, args.kafka_topic)
    try:
        run_listener(args.product, producer, args.print_raw)
    finally:
        producer.flush()


if __name__ == "__main__":
    main()

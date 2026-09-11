"""Kafka producer for untouched Coinbase market-trade batches."""

from __future__ import annotations

import logging

from confluent_kafka import Message, Producer


LOGGER = logging.getLogger(__name__)
INGESTION_VERSION = "v1"


class RawBatchProducer:
    """Publish raw Coinbase batches with delivery guarantees and trace metadata."""

    def __init__(self, bootstrap_servers: str, topic: str) -> None:
        self._topic = topic
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "client.id": "coinbase-raw-ingestion",
                "acks": "all",
                "enable.idempotence": True,
                "retries": 5,
                "retry.backoff.ms": 200,
                "linger.ms": 10,
            }
        )

    def publish(self, product_id: str, raw_message: str) -> None:
        """Queue one unchanged Coinbase batch for asynchronous Kafka delivery."""
        headers = {
            "source": "coinbase",
            "connection_product_id": product_id,
            "ingestion_version": INGESTION_VERSION,
        }

        while True:
            try:
                self._producer.produce(
                    topic=self._topic,
                    key=product_id,
                    value=raw_message,
                    headers=headers,
                    on_delivery=self._on_delivery,
                )
                break
            except BufferError:
                LOGGER.warning(
                    "Kafka producer buffer is full. Waiting for delivery acknowledgments."
                )
                self._producer.poll(1.0)

        self._producer.poll(0)

    def flush(self, timeout_seconds: float = 30.0) -> None:
        """Wait for pending records before the ingestion service exits."""
        undelivered_records = self._producer.flush(timeout_seconds)
        if undelivered_records:
            LOGGER.error(
                "Kafka producer stopped with %s undelivered record(s).",
                undelivered_records,
            )
        else:
            LOGGER.info("Kafka producer flushed successfully.")

    @staticmethod
    def _on_delivery(error: Exception | None, message: Message) -> None:
        if error is not None:
            LOGGER.error("Kafka delivery failed: %s", error)
            return

        LOGGER.debug(
            "Kafka delivery confirmed: topic=%s partition=%s offset=%s",
            message.topic(),
            message.partition(),
            message.offset(),
        )

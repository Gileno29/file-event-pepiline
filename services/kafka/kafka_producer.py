
import logging
import os

from confluent_kafka import Producer


logger = logging.getLogger(__name__)


class KafkaProducer:

    def __init__(
        self,
        bootstrap_servers=None,
        client_id="file-event-pipeline"
    ):

        self.topic = os.getenv(
            "KAFKA_TOPIC",
            "files-processing"
        )

        self.producer = Producer({
            "bootstrap.servers": (
                bootstrap_servers
                or os.getenv(
                    "KAFKA_BOOTSTRAP_SERVERS",
                    "localhost:19092"
                )
            ),
            "client.id": client_id,
            "acks": "all",
            "enable.idempotence": True,
            "retries": 10
        })

        logger.info(
            "Kafka Producer initialized"
        )

    def publish(
        self,
        key,
        value,
        headers=None,
        topic=None
    ):
        """
        Publish a message to Kafka.

        key: Message identifier.
        value: Message payload (bytes).
        headers: Optional Kafka headers.
        topic: Optional topic override.
        """

        target_topic = topic or self.topic

        delivery_result = {}

        def delivery_callback(error, message):

            if error:
                delivery_result["error"] = error

                logger.error(
                    "Kafka delivery failed: %s",
                    error
                )

            else:
                delivery_result["message"] = message

                logger.info(
                    "Message delivered: topic=%s partition=%s offset=%s",
                    message.topic(),
                    message.partition(),
                    message.offset()
                )

        self.producer.produce(
            topic=target_topic,
            key=key if isinstance(key, bytes) else str(key).encode("utf-8"),
            value=value,
            headers=headers,
            callback=delivery_callback
        )

        remaining = self.producer.flush(timeout=30)

        if remaining > 0:
            raise TimeoutError(
                "Kafka delivery timed out"
            )

        if delivery_result.get("error"):
            raise RuntimeError(
                str(delivery_result["error"])
            )

        return delivery_result["message"]

    def close(self):
        """
        Flush pending messages before closing.
        """

        remaining = self.producer.flush(timeout=30)

        if remaining > 0:
            raise TimeoutError(
                f"{remaining} Kafka messages were not delivered"
            )

        logger.info("Kafka Producer closed")

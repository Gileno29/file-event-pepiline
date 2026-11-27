
import os
import logging

from pathlib import Path, PurePosixPath

from confluent_kafka import Consumer


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

logger = logging.getLogger(__name__)


# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:19092"
)

KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC",
    "files-processing"
)

KAFKA_GROUP_ID = os.getenv(
    "KAFKA_GROUP_ID",
    "file-processing-group"
)

OUTPUT_DIR = Path(
    os.getenv("OUTPUT_DIR", "processed")
)


consumer = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "group.id": KAFKA_GROUP_ID,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False
})


def get_headers(message):
    """
    Convert Kafka headers into a dictionary.
    """

    headers = message.headers() or []

    return {
        key: value.decode("utf-8") if isinstance(value, bytes) else value
        for key, value in headers
    }


def save_file(headers, content):
    """
    Save received file locally.
    """

    bucket_name = headers["bucket"]
    object_name = headers["object"]

    relative_path = PurePosixPath(object_name)

    if relative_path.is_absolute() or ".." in relative_path.parts:
        raise ValueError("Invalid object path")

    destination = (
        OUTPUT_DIR
        / bucket_name
        / Path(*relative_path.parts)
    )

    destination.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    destination.write_bytes(content)

    logger.info(
        "File saved successfully: %s (%s bytes)",
        destination,
        len(content)
    )


def process_message(message):
    """
    Process Kafka message.
    """

    headers = get_headers(message)

    content = message.value()

    event_id = headers.get("event_id")

    logger.info(
        "Received Kafka message: %s",
        event_id
    )

    save_file(
        headers,
        content
    )


def run():

    consumer.subscribe([KAFKA_TOPIC])

    logger.info("Listening to Kafka topic: %s", KAFKA_TOPIC)

    try:
        while True:

            message = consumer.poll(timeout=1.0)

            if message is None:
                continue

            if message.error():
                logger.error("Kafka error: %s", message.error())
                continue

            try:
                process_message(message)

                consumer.commit(
                    message=message,
                    asynchronous=False
                )

            except Exception:
                logger.exception("Error processing Kafka message")

    finally:
        consumer.close()


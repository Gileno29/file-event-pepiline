
import logging
import threading
import signal
import sys

from dotenv import load_dotenv

from services.kafka.kafka_consumer import run as run_kafka
from google.cloud import storage
from services.pubsub.pubsub_consumer import PubsubConsumer
from google.auth.credentials import AnonymousCredentials
from services.kafka.kafka_producer import KafkaProducer
from shared.constants import *

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)

logger = logging.getLogger("application")


def start_service(name, service):

    logger.info("Starting service: %s", name)

    try:
        service()

    except Exception:
        logger.exception(
            "Service stopped unexpectedly: %s",
            name
        )


def main():
    producer= KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    storage_client = storage.Client(
        project=PROJECT_ID,
        credentials=AnonymousCredentials(),
        client_options={
            "api_endpoint": GCS_ENDPOINT
        }
    )
    
    run_pubsub= PubsubConsumer(kafka_producer=producer, storage_client=storage_client).run

    logger.info("Starting File Event Pipeline")

    services = [
        ("Kafka Consumer", run_kafka),
        ("Pub/Sub Consumer", run_pubsub)
    ]

    threads = []

    for name, service in services:

        thread = threading.Thread(
            target=start_service,
            args=(name, service),
            name=name,
            daemon=True
        )

        thread.start()
        threads.append(thread)

    try:

        for thread in threads:
            thread.join()

    except KeyboardInterrupt:

        logger.info("Shutting down File Event Pipeline")

        sys.exit(0)


if __name__ == "__main__":
    main()

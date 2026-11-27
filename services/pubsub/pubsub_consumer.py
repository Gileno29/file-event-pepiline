
import os
import json
import logging

from google.cloud import pubsub_v1

from confluent_kafka import Producer

from kafka.kafka_producer import KafkaProducer


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

logger = logging.getLogger(__name__)




# GCS client



# Kafka producer
producer = Producer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "client.id": "pubsub-consumer"
})


class PubsubConsumer():
    def __init__(
            self,
            kafka_producer=None,
            storage_client=None,
        ):

        self.producer= kafka_producer
        self.storage_client=storage_client

    def download_file(self, bucket_name, object_name, generation=None):
        """
        Download a file from the GCS emulator.
        """

        logger.info(
            "Downloading file: gs://%s/%s",
            bucket_name,
            object_name
        )

        bucket = storage_client.bucket(bucket_name)

        blob = bucket.blob(
            object_name,
            generation=int(generation) if generation else None
        )

        content = blob.download_as_bytes()

        logger.info(
            "File downloaded successfully. Size: %s bytes",
            len(content)
        )

        return content, blob


    def publish_to_kafka(self, event, content, blob):
        """
        Publish file bytes to Kafka and wait for broker confirmation.
        """

        bucket_name = event["bucket"]
        object_name = event["object"]

        generation = event.get("generation") or blob.generation

        event_id = (
            f"{bucket_name}/{object_name}#{generation}"
        )

        headers = [
            ("event_id", str(event_id)),
            ("bucket", bucket_name),
            ("object", object_name),
            ("generation", str(generation)),
            ("content_type", blob.content_type or "application/octet-stream"),
        ]

        return self.producer.publish(topic=KAFKA_TOPIC, key=event_id.encode("utf-8"), value=content, headers=headers)


    def process_message(self, message):
        """
        Process Pub/Sub message.
        """

        event = json.loads(message.data.decode("utf-8"))

        bucket_name = event["bucket"]
        object_name = event["object"]

        logger.info(
            "Received file event: gs://%s/%s",
            bucket_name,
            object_name
        )

        content, blob = download_file(
            bucket_name,
            object_name,
            event.get("generation")
        )

        self.publish_to_kafka(
            event,
            content,
            blob
        )


    def run(self):

        subscriber = pubsub_v1.SubscriberClient()

        subscription_path = subscriber.subscription_path(
            PROJECT_ID,
            SUBSCRIPTION_NAME
        )

        def callback(message):
            try:
                self.process_message(message)
                message.ack()

                logger.info("Pub/Sub message acknowledged")

            except Exception:
                logger.exception("Error processing Pub/Sub message")
                message.nack()

        logger.info("Listening to: %s", subscription_path)

        streaming_pull_future = subscriber.subscribe(
            subscription_path,
            callback=callback
        )

        try:
            streaming_pull_future.result()

        finally:
            streaming_pull_future.cancel()
            subscriber.close()

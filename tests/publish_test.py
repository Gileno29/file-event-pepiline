
import os
import json

from google.cloud import pubsub_v1


project_id = os.getenv("GCP_PROJECT_ID", "local-project")
topic_name = os.getenv("PUBSUB_TOPIC", "file-uploaded")

publisher = pubsub_v1.PublisherClient()

topic_path = publisher.topic_path(
    project_id,
    topic_name
)

event = {
    "event_type": "OBJECT_FINALIZE",
    "bucket": "incoming",
    "object": "transactions.csv",
    "generation": None
}

future = publisher.publish(
    topic_path,
    json.dumps(event).encode("utf-8")
)

print("Published message:", future.result())

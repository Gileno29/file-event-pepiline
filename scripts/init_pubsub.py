
import os

from google.cloud import pubsub_v1
from google.api_core.exceptions import AlreadyExists


PROJECT_ID = os.getenv("GCP_PROJECT_ID", "local-project")
TOPIC_NAME = os.getenv("PUBSUB_TOPIC", "file-uploaded")
SUBSCRIPTION_NAME = os.getenv(
    "PUBSUB_SUBSCRIPTION",
    "file-uploaded-sub"
)


publisher = pubsub_v1.PublisherClient()
subscriber = pubsub_v1.SubscriberClient()

topic_path = publisher.topic_path(
    PROJECT_ID,
    TOPIC_NAME
)

subscription_path = subscriber.subscription_path(
    PROJECT_ID,
    SUBSCRIPTION_NAME
)

try:
    publisher.create_topic(request={"name": topic_path})
    print(f"Topic created: {topic_path}")

except AlreadyExists:
    print(f"Topic already exists: {topic_path}")


try:
    subscriber.create_subscription(
        request={
            "name": subscription_path,
            "topic": topic_path
        }
    )

    print(f"Subscription created: {subscription_path}")

except AlreadyExists:
    print(f"Subscription already exists: {subscription_path}")


subscriber.close()

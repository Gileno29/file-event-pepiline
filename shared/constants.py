import os

# Configuration
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "local-project")

SUBSCRIPTION_NAME = os.getenv(
    "PUBSUB_SUBSCRIPTION",
    "file-uploaded-sub"
)

KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC",
    "files-processing"
)

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:19092"
)

GCS_ENDPOINT = os.getenv(
    "STORAGE_EMULATOR_HOST",
    "http://localhost:8080"
)

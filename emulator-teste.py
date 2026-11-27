from google.auth.credentials import AnonymousCredentials
from google.cloud import storage



BUCKET = "test-bucket"

client = storage.Client(
    credentials=AnonymousCredentials(),
    project="test",
)
bucket = client.bucket(BUCKET)
bucket = client.create_bucket(bucket)
blob = bucket.blob("blob1")
blob.upload_from_string("test1")
blob = bucket.blob("blob2")
blob.upload_from_string("test2")
for blob in bucket.list_blobs():
    content = blob.download_as_bytes()
    print("Blob [{}]: {}".format(blob.name, content))


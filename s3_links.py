import boto3
from datetime import timedelta

BUCKET = "ev-charging-app-csvs"

s3 = boto3.client("s3")

def generate_presigned_url(key, expiry_seconds=3600):
    return s3.generate_presigned_url(
        ClientMethod="get_object",
        Params={"Bucket": BUCKET, "Key": key},
        ExpiresIn=expiry_seconds
    )

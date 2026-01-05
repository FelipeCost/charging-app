import boto3
import pandas as pd
import io
import os
from botocore.exceptions import ClientError


S3_BUCKET = os.environ.get("S3_BUCKET")
AWS_REGION = os.environ.get("AWS_REGION", "eu-west-2")

s3 = boto3.client("s3", region_name=AWS_REGION)

def read_csv_s3(key, columns=None):
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        body = obj["Body"].read()
        if len(body) == 0:
            return pd.DataFrame(columns=columns)
        return pd.read_csv(io.BytesIO(body))
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            return pd.DataFrame(columns=columns)
        raise

def write_csv_s3(df, key):
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=csv_buffer.getvalue())


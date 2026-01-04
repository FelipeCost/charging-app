import os
import boto3
import pandas as pd
from io import StringIO

AWS_REGION = os.getenv("AWS_REGION")
S3_BUCKET = os.getenv("S3_BUCKET_NAME")

s3 = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=AWS_REGION,
)

def s3_file_exists(key):
    try:
        s3.head_object(Bucket=S3_BUCKET, Key=key)
        return True
    except:
        return False

def read_csv_s3(key, columns=None):
    if not s3_file_exists(key):
        if columns:
            return pd.DataFrame(columns=columns)
        return pd.DataFrame()

    obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
    return pd.read_csv(obj["Body"])

def write_csv_s3(df, key):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=csv_buffer.getvalue())

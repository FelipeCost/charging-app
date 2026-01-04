import os
import boto3
import pandas as pd
from io import StringIO

S3_BUCKET = os.environ.get("S3_BUCKET")
AWS_REGION = os.environ.get("AWS_REGION", "eu-west-2")

s3 = boto3.client("s3", region_name=AWS_REGION)

def read_csv_s3(key, columns=None):
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        df = pd.read_csv(obj["Body"])
        if columns:
            for c in columns:
                if c not in df.columns:
                    df[c] = None
        return df
    except s3.exceptions.NoSuchKey:
        if columns:
            return pd.DataFrame(columns=columns)
        return pd.DataFrame()
    except Exception:
        if columns:
            return pd.DataFrame(columns=columns)
        return pd.DataFrame()

def write_csv_s3(df, key):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=csv_buffer.getvalue())

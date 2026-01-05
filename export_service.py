import os
import boto3
import pandas as pd
import io
from flask import Flask, Response

S3_BUCKET = os.environ["S3_BUCKET"]
AWS_REGION = os.environ.get("AWS_REGION", "eu-west-2")

s3 = boto3.client("s3", region_name=AWS_REGION)

app = Flask(__name__)

@app.route("/export/log")
def export_log():
    obj = s3.get_object(Bucket=S3_BUCKET, Key="charging_log.csv")
    body = obj["Body"].read()
    df = pd.read_csv(io.BytesIO(body))

    csv = df.to_csv(index=False)

    return Response(
        csv,
        mimetype="text/csv",
        headers={
            "Content-Disposition": "attachment; filename=charging_log.csv"
        }
    )

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)

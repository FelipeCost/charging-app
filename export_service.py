import os
import boto3
from flask import Flask, Response

app = Flask(__name__)

S3_BUCKET = os.environ["S3_BUCKET"]
LOG_FILE = "charging_log.csv"

s3 = boto3.client("s3")

@app.route("/export/log")
def export_log():
    obj = s3.get_object(Bucket=S3_BUCKET, Key=LOG_FILE)
    csv_data = obj["Body"].read()

    return Response(
        csv_data,
        mimetype="text/csv",
        headers={
            "Content-Disposition": "attachment; filename=charging_log.csv",
            "Cache-Control": "no-cache"
        }
    )

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))

import json
import tempfile
import os
from pdf_processor.config import MANIFEST_S3_BUCKET, MANIFEST_S3_KEY, AWS_REGION
import boto3
from botocore.exceptions import ClientError
from pdf_processor.aws_clients import get_boto3_client, get_aboto3_client


def upload_manifest_entry(entry: dict):
    """
    Append a JSON line to manifest object. Implemented by downloading manifest, appending, and re-uploading.
    For huge manifests you may want to use S3 append patterns (multipart) or maintain per-worker small manifests.
    """
    #s3 = boto3.client('s3', region_name=AWS_REGION, endpoint_url=ENDPOINT_URL, aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY_ID)
    s3 = get_boto3_client("s3")
    bucket = MANIFEST_S3_BUCKET
    key = MANIFEST_S3_KEY
    tmp = tempfile.NamedTemporaryFile(delete=False)
    try:
        # download existing content if exists
        try:
            s3.download_file(bucket.replace("s3://",""), key, tmp.name)
            # append new line
            with open(tmp.name, "a", encoding="utf-8") as f:
                f.write(json.dumps(entry, ensure_ascii=False) + "\n")
        except ClientError as e:
            # not found -> create new
            with open(tmp.name, "w", encoding="utf-8") as f:
                f.write(json.dumps(entry, ensure_ascii=False) + "\n")
        # upload
        s3.upload_file(tmp.name, bucket.replace("s3://",""), key)
    finally:
        try:
            os.unlink(tmp.name)
        except:
            pass

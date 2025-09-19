import asyncio
import tempfile
import os
import io
import json
import logging
import time
import uuid
import gzip
import boto3
import pyarrow as pa
import aioboto3
import pyarrow.parquet as pq
from datetime import datetime
from PyPDF2 import PdfReader, PdfWriter  # not using pypdf as implemented in gpt-5
from pdf_processor.utils import download_s3_to_file, write_parquet_async, upload_file_to_s3
from pdf_processor.manifest import upload_manifest_entry
from pdf_processor.config import OUTPUT_S3_BUCKET, OUTPUT_S3_PREFIX, PDF_FILE_STATE_NAME, PARQUET_MAX_CHUNK_SIZE_MB, \
    OCR_PARQUET_STATE_NAME, PDF_OCR_PARQUET_SQS_QUEUE_NAME
from pdf_processor.aws_clients import get_boto3_client, get_aboto3_client
from pdf_processor.logger import get_logger
from pdf_processor.utils import check_if_file_enqueued, get_queue_url

logger = get_logger("processor")

def _now_iso():
    return datetime.utcnow().isoformat() + "Z"

async def _get_dynamo_item(ddb, s3_key: str):
    resp = await ddb.get_item(TableName=PDF_FILE_STATE_NAME,
                              Key={"s3_key": {"S": s3_key}},
                              ConsistentRead=True)
    return resp.get("Item")

async def _init_or_mark_processing(ddb, s3_key: str):
    """
    Ensure an item exists and set status=processing if not done.
    Returns item (may be None if newly created).
    """
    item = await _get_dynamo_item(ddb, s3_key)
    if item:
        status = item.get("status", {}).get("S")
        if status == "done":
            return item  # done already
    # Put or update to set processing and ensure attempts/pages_processed exist
    now = _now_iso()
    # Use update_item to create if not exists and set status to processing
    await ddb.update_item(
        TableName=PDF_FILE_STATE_NAME,
        Key={"s3_key": {"S": s3_key}},
        UpdateExpression="SET #st = :processing, #lu = :lu, attempts = if_not_exists(attempts, :zero_n), pages_processed = if_not_exists(pages_processed, :zero_n)",
        ExpressionAttributeNames={"#st": "status", "#lu": "last_updated"},
        ExpressionAttributeValues={":processing": {"S": "processing"}, ":lu": {"S": now}, ":zero_n": {"N": "0"}},
    )
    return await _get_dynamo_item(ddb, s3_key)

async def _increment_pages_processed(ddb, s3_key: str, increment: int = 1):
    now = _now_iso()
    await ddb.update_item(
        TableName=PDF_FILE_STATE_NAME,
        Key={"s3_key": {"S": s3_key}},
        UpdateExpression="ADD pages_processed :inc SET last_updated = :lu",
        ExpressionAttributeValues={":inc": {"N": str(increment)}, ":lu": {"S": now}},
    )

async def _set_done(ddb, s3_key: str, total_pages: int):
    now = _now_iso()
    await ddb.update_item(
        TableName=PDF_FILE_STATE_NAME,
        Key={"s3_key": {"S": s3_key}},
        UpdateExpression="SET #st = :done, total_pages = :tp, last_updated = :lu",
        ExpressionAttributeNames={"#st": "status"},
        ExpressionAttributeValues={":done": {"S": "done"}, ":tp": {"N": str(total_pages)}, ":lu": {"S": now}},
    )

async def _mark_failure(ddb, s3_key: str, err_msg: str = ""):
    now = _now_iso()
    # increment attempts and set status=failed
    await ddb.update_item(
        TableName=PDF_FILE_STATE_NAME,
        Key={"s3_key": {"S": s3_key}},
        UpdateExpression="ADD attempts :one SET #st = :failed, last_error = :err, last_updated = :lu",
        ExpressionAttributeNames={"#st": "status"},
        ExpressionAttributeValues={":one": {"N": "1"}, ":failed": {"S": "failed"}, ":err": {"S": err_msg[:1024]}, ":lu": {"S": now}},
    )

class ParquetBatchWriter:
    def __init__(self, s3_bucket, output_prefix, max_chunk_size_mb=PARQUET_MAX_CHUNK_SIZE_MB):
        #self.s3 = boto3.client("s3", region_name=AWS_REGION, endpoint_url=ENDPOINT_URL, aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY_ID)
        #self.sqs = boto3.client("sqs", region_name=AWS_REGION,
                                # aws_access_key_id=AWS_DDB_ACCESS_KEY_ID,
                                # aws_secret_access_key=AWS_DDB_SECRET_ACCESS_KEY_ID,
                                # endpoint_url=ENDPOINT_URL)
        #self.ddb = boto3.client("dynamodb", region_name=AWS_REGION,
                                # aws_access_key_id=AWS_DDB_ACCESS_KEY_ID,
                                # aws_secret_access_key=AWS_DDB_SECRET_ACCESS_KEY_ID,
                                # endpoint_url=ENDPOINT_URL)
        self.s3 = get_boto3_client("s3")
        self.sqs = get_boto3_client("sqs")
        self.ddb = get_boto3_client("dynamodb")
        self.bucket = s3_bucket
        self.prefix = output_prefix
        self.max_chunk = max_chunk_size_mb * 1024 * 1024
        self.buffer = {"doc_id": [], "page_number": [], "s3_source": [], "page_bytes": []}
        self.current_size = 0
        self.parts = []

    def add_page(self, doc_id, page_number, s3_source, page_bytes):
        """Add one page to buffer and flush if needed."""
        self.buffer["doc_id"].append(doc_id)
        self.buffer["page_number"].append(page_number)
        self.buffer["s3_source"].append(s3_source)
        self.buffer["page_bytes"].append(page_bytes)

        # estimate incremental parquet size
        temp_table = pa.Table.from_pydict({
            "doc_id": [doc_id],
            "page_number": [page_number],
            "s3_source": [s3_source],
            "page_bytes": pa.array([page_bytes], type=pa.binary())
        })
        buf = io.BytesIO()
        pq.write_table(temp_table, buf, compression="snappy")
        self.current_size += buf.getbuffer().nbytes

        if self.current_size >= self.max_chunk:
            self.flush()

    def flush(self):
        """Write buffer to parquet and upload to S3."""
        if not self.buffer["doc_id"]:
            return
        table = pa.Table.from_pydict({
            "doc_id": self.buffer["doc_id"],
            "page_number": self.buffer["page_number"],
            "s3_source": self.buffer["s3_source"],
            "page_bytes": pa.array(self.buffer["page_bytes"], type=pa.binary())
        })
        buf = io.BytesIO()
        pq.write_table(table, buf, compression="snappy")
        buf.seek(0)

        key = f"{self.prefix}/{uuid.uuid4()}.parquet"
        self.s3.upload_fileobj(buf, self.bucket, key)
        self.parts.append(key)

        # add item to dynamodb
        try:
            response = check_if_file_enqueued(self.ddb, key, OCR_PARQUET_STATE_NAME)
            if not response:
                self.ddb.put_item(
                    TableName=OCR_PARQUET_STATE_NAME,
                    Item={
                        's3_key': {'S': key},
                        'status': {"S": "enqueued"},
                        'timestamp': {'N': str(int(time.time()))}
                    },
                    ConditionExpression="attribute_not_exists(s3_key)"
                )
            logger.info(f'Successfully updated key {key} to dynamo db.')
        except Exception as ddb_e:
            logger.warning(f'Failed to update key {key} to dynamo db: {ddb_e}')

        # publish to SQS
        try:
            response = self.sqs.send_message(
                QueueUrl=get_queue_url(self.sqs, PDF_OCR_PARQUET_SQS_QUEUE_NAME),
                MessageBody=json.dumps({'s3_key': key})
            )
            logger.info(f"Successfully enqueued S3 key {key} to SQS. Message ID: {response['MessageId']}")
        except Exception as sqs_e:
            logger.warning(f"Error sending message to SQS for {key}: {sqs_e}")

        # reset buffer
        self.buffer = {"doc_id": [], "page_number": [], "s3_source": [], "page_bytes": []}
        self.current_size = 0

    def close(self):
        """Flush remaining pages before shutdown."""
        self.flush()
        return self.parts


async def process_pdf_per_page(s3_bucket_uri: str, s3_key: str, writer: ParquetBatchWriter, ddb_client=None):
    """
    Downloads PDF, extracts per-page text, updates DynamoDB per-page progress,
    saves per-document parquet and uploads. Returns summary dict.
    """
    if ddb_client is None:
        # create a short-lived client if not provided
        #session = aioboto3.Session()
        #ddb_client = await session.client("dynamodb", endpoint_url=ENDPOINT_URL, aws_access_key_id=AWS_DDB_ACCESS_KEY_ID, aws_secret_access_key=AWS_DDB_SECRET_ACCESS_KEY_ID).__aenter__()  # we will not close to keep calling functions simple
        ddb_client = await get_aboto3_client("dynamodb")
    # First, check DynamoDB: if status == done, skip
    try:
        existing = await _get_dynamo_item(ddb_client, s3_key)
        if existing and existing.get("status", {}).get("S") == "done":
            logger.info("Skipping %s — already done in DynamoDB", s3_key)
            return {"s3_key": s3_key, "status": "skipped", "reason": "already_done"}

        # mark processing (create or set)
        await _init_or_mark_processing(ddb_client, s3_key)

        tmp_pdf = tempfile.NamedTemporaryFile(suffix=".pdf", delete=False)
        tmp_pdf.close()
        try:
            logger.info("Downloading s3://%s/%s to %s", s3_bucket_uri.replace("s3://", ""), s3_key, tmp_pdf.name)
            await download_s3_to_file(s3_bucket_uri, s3_key, tmp_pdf.name)

            # Use PyPDF2 to read pages
            doc_id = s3_key
            reader = PdfReader(tmp_pdf.name)
            num_pages = len(reader.pages)

            for i in range(num_pages):
                page = reader.pages[i]
                writer_pdf = PdfWriter()
                writer_pdf.add_page(page)
                page_buf = io.BytesIO()
                writer_pdf.write(page_buf)
                raw_bytes = page_buf.getvalue()

                # gzip compress
                compressed_bytes = gzip.compress(raw_bytes)

                # update manifest (best-effort)
                # try:
                #     upload_manifest_entry({"s3_key": s3_key, "page": i + 1, "ts": _now_iso()})
                # except Exception as e:
                #     logger.warning("Manifest update failed for page %s: %s", i + 1, e)

                # DynamoDB per-page increment (atomic)
                try:
                    await _increment_pages_processed(ddb_client, s3_key, 1)
                except Exception as e:
                    logger.warning("DynamoDB pages increment failed for %s page %s: %s", s3_key, i + 1, e)

                # add page into shared writer
                s3_bucket = s3_bucket_uri.replace("s3://","") if s3_bucket_uri.startswith("s3://") else s3_bucket_uri
                writer.add_page(
                    doc_id=doc_id,
                    page_number=i,
                    s3_source=f"s3://{s3_bucket}/{s3_key}",
                    page_bytes=compressed_bytes,
                )

            # set done and total_pages in DynamoDB
            try:
                await _set_done(ddb_client, s3_key, num_pages)
            except Exception as e:
                logger.warning("Failed to set done in DynamoDB for %s: %s", s3_key, e)

            return {"s3_key": s3_key, "status": "success", "pages": num_pages}
        finally:
            try:
                os.unlink(tmp_pdf.name)
            except:
                pass
    except Exception as e:
        logger.exception("Unexpected failure processing %s: %s", s3_key, e)
        try:
            await _mark_failure(ddb_client, s3_key, str(e))
        except Exception as ex:
            logger.exception("Also failed to mark failure in DynamoDB: %s", ex)
        raise

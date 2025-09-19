import asyncio
import json
import logging
from typing import List
from pdf_processor.aws_clients import sqs_client, s3_client
from pdf_processor.processor import ParquetBatchWriter, process_pdf_per_page
from pdf_processor.logger import get_logger
import aioboto3
from botocore.exceptions import ClientError
import time
from pdf_processor.config import (
                        PDF_SQS_QUEUE_NAME,
                        PDF_DLQ_QUEUE_NAME,
                        MAX_WORKERS,
                        MAX_RETRIES,
                        AWS_REGION,
                        PDF_FILE_STATE_NAME,
                        INPUT_S3_BUCKET,
                        SQS_MAX_MESSAGES,
                        SQS_WAIT_TIME,
                        OUTPUT_S3_BUCKET,
                        OUTPUT_S3_PREFIX,
)
from pdf_processor.aws_clients import get_boto3_client, get_aboto3_client
from pdf_processor.utils import check_if_file_enqueued


logger = get_logger("worker")

"""
Main worker process: polls SQS, dispatches per-message handlers, handles retries and DLQ.

"""
class Worker:
    def __init__(self):
        self.running = True
        self.logger = logger


    async def run(self):
        logger.info("Worker starting with max workers=%s", MAX_WORKERS)
        #sess = aioboto3.Session(region_name=AWS_REGION)

        # Create a pool of tasks to process messages
        sem = asyncio.Semaphore(MAX_WORKERS)
        writer = ParquetBatchWriter(OUTPUT_S3_BUCKET, OUTPUT_S3_PREFIX) ## TODo Check writer logic for testing
        tasks = set()
        # async with (sess.client('sqs', region_name=AWS_REGION, endpoint_url=ENDPOINT_URL, aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY_ID) as sqs,
        #             sess.client('dynamodb', region_name=AWS_REGION, endpoint_url=ENDPOINT_URL, aws_access_key_id=AWS_DDB_ACCESS_KEY_ID, aws_secret_access_key=AWS_DDB_SECRET_ACCESS_KEY_ID) as ddb):
        async with await get_aboto3_client("sqs") as sqs, await get_aboto3_client("dynamodb") as ddb:
            while self.running: # Continously polls the SQS server
                try:
                    queue_response = await sqs.get_queue_url(QueueName=PDF_SQS_QUEUE_NAME)
                    queue_url = queue_response['QueueUrl']
                    resp = await sqs.receive_message(QueueUrl=queue_url , MaxNumberOfMessages=SQS_MAX_MESSAGES, WaitTimeSeconds=SQS_WAIT_TIME, VisibilityTimeout=60, AttributeNames=['All'])
                    messages = resp.get('Messages', [])
                    if not messages:
                        await asyncio.sleep(1)
                        continue
                    for msg in messages:
                        await sem.acquire()
                        t = asyncio.create_task(self._handle_message(msg, sem, sqs, writer, ddb))
                        tasks.add(t)
                        t.add_done_callback(lambda fut: tasks.discard(fut))
                except Exception as e:
                    logger.exception("Error polling SQS: %s", e)
                    await asyncio.sleep(5)

    async def _publish_to_dlq(sqs_client, failed_keys: List[str], reason: str):
        """Optionally push failed keys into DLQ."""
        queue_response = await sqs_client.get_queue_url(QueueName=PDF_SQS_QUEUE_NAME)
        queue_url = queue_response['QueueUrl']

        if not queue_url or not failed_keys:
            return

        payload = {
            "failed_keys": failed_keys,
            "reason": reason,
        }
        await sqs_client.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(payload)
        )
        print(f"[dlq] published {len(failed_keys)} failed keys → DLQ")

    async def _handle_message(self, msg, sem, sqs_client, writer, ddb_client):
        print("init handle message")
        try:
            body = msg.get("Body")
            receipt_handle = msg.get("ReceiptHandle")
            # Assume JSON body with s3_key
            try:
                payload = json.loads(body)
            except Exception:
                # raw string?
                payload = {"s3_key": body}
            s3_keys = payload.get("s3_keys") or payload.get("s3_key") or payload.get("key") or payload.get("s3Uri") or payload.get("s3_uri") or body
            logger.info("Processing message for %s", s3_keys)

            failed_keys = []
            for s3_key in s3_keys:
            # Process PDF per page
                try:
                    result = await process_pdf_per_page(INPUT_S3_BUCKET, s3_key, writer, ddb_client=ddb_client)
                    logger.info("Processed: %s", result)
                    # on success delete message
                    # await sqs_client.delete_message(QueueUrl=ENQUEUE_PDF_SQS_URL, ReceiptHandle=receipt_handle)
                except Exception as e:
                    logger.exception("Processing failed for %s: %s", s3_key, e)
                    # implement retry counting via message attributes or via visibility timeout and a Dead Letter Queue
                    # Here we'll send to DLQ explicitly after MAX_RETRIES.
                    # Check message attributes for retries
                    attrs = msg.get("Attributes", {})
                    # If SQS redrive policy is configured, it might be handled automatically; still, we implement fallback logic:
                    attempt = int(msg.get("Attributes", {}).get("ApproximateReceiveCount", "1"))
                    if attempt and attempt >= MAX_RETRIES:
                        logger.warning("Max retries reached for %s; sending to DLQ", s3_key)
                        failed_keys.append(s3_key)
                        # try:
                        #     await sqs_client.send_message(QueueUrl=ENQUEUE_PDF_DLQ_URL, MessageBody=json.dumps(payload))
                        # except Exception as ex:
                        #     logger.exception("Failed to send to DLQ: %s", ex)
                        # # delete original
                        # await sqs_client.delete_message(QueueUrl=ENQUEUE_PDF_SQS_URL, ReceiptHandle=receipt_handle)
                    else:
                        # make message visible again after short backoff by changing visibility timeout
                        try:
                            await sqs_client.change_message_visibility(QueueUrl=await sqs_client.get_queue_url(QueueName=PDF_SQS_QUEUE_NAME), ReceiptHandle=receipt_handle, VisibilityTimeout=30)
                        except Exception as ex:
                            logger.exception("Failed to change visibility: %s", ex)

            if not failed_keys:
                # all keys succeeded → delete message
                queue_response = await sqs_client.get_queue_url(QueueName=PDF_SQS_QUEUE_NAME)
                queue_url = queue_response['QueueUrl']
                await sqs_client.delete_message(
                    QueueUrl=queue_url,
                    ReceiptHandle=msg["ReceiptHandle"]
                )
                print(f"[sqs] deleted message with {len(s3_keys)} keys")
            else:
                # explicitly publish failures to DLQ
                await self._publish_to_dlq(sqs_client, failed_keys, "worker_processing_failed")
                print(f"[sqs] leaving message in main queue for redelivery")
                # SQS will eventually move the msg to DLQ after maxReceiveCount
                # We don’t delete the original msg here

        finally:
            sem.release()
            writer.close()

def main():
    worker = Worker()
    try:
        asyncio.run(worker.run())
    except KeyboardInterrupt:
        logger.info("Worker exiting on keyboard interrupt")

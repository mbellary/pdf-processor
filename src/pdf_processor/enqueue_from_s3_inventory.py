# enqueue_from_s3_inventory.py
import argparse
import asyncio
import csv
import gzip
import io
import json
import math
import os
import sys
import time
import zlib
from typing import AsyncIterator, List, Optional, Tuple

import aioboto3
import botocore
import boto3

from pdf_processor.config import SQS_MAX_MSG_SIZE, SQS_SAFE_BODY_BYTES, SQS_BATCH_MAX, INPUT_S3_BUCKET, ENQUEUE_PDF_SQS_URL,AWS_REGION, \
AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY_ID, ENDPOINT_URL, MAX_FLUSH_SIZE, MAX_KEYS_PER_MESSAGE

from pdf_processor.logger import get_logger

logger = get_logger("enqueue_worker")

def approx_json_size_bytes(obj) -> int:
    # Fast-ish size estimation without building the final string repeatedly
    return len(json.dumps(obj, separators=(",", ":")).encode("utf-8"))

def chunked(iterable, n):
    buf = []
    for item in iterable:
        buf.append(item)
        if len(buf) >= n:
            yield buf
            buf = []
    if buf:
        yield buf

async def list_inventory_objects(inventory_bucket: str, inventory_prefix: str) -> List[str]:
    """
    Returns a list of object keys (in the inventory bucket) that look like CSV.gz inventory files.
    We search under the given prefix. For very large inventories, this list is still tiny (dozens).
    """
    s3 = boto3.client("s3",  region_name = AWS_REGION,
                                                    aws_access_key_id=AWS_ACCESS_KEY_ID,
                                                    aws_secret_access_key=AWS_SECRET_ACCESS_KEY_ID,
                                                    endpoint_url=ENDPOINT_URL)
    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=inventory_bucket, Prefix=inventory_prefix):
        for obj in page.get("Contents", []):
            k = obj["Key"]
            # S3 Inventory produces manifest.json + data/*.csv.gz (or .csv)
            if k.endswith(".csv") or k.endswith(".csv.gz"):
                keys.append(k)
    if not keys:
        raise RuntimeError(f"No inventory CSV files found under s3://{inventory_bucket}/{inventory_prefix}")
    return keys

def _parse_inventory_csv_stream(stream: io.TextIOBase, filter_prefix: Optional[str], filter_suffix: Optional[str]) -> List[str]:
    """
    Parse one CSV file (already decompressed text stream). Returns list of keys from this CSV.
    CSV header includes column 'Key' (exact), but safer to find index by name.
    """
    reader = csv.reader(stream)
    header = next(reader, None)
    if not header:
        return []
    # Find the "Key" column; S3 Inventory headers are well-known
    try:
        key_idx = header.index("key")
    except ValueError:
        # Some inventory configs place key as 1st or 2nd col; fallback to col 1
        key_idx = 1 if len(header) > 1 else 0

    out = []
    for row in reader:
        if not row or len(row) <= key_idx:
            continue
        key = row[key_idx]
        if filter_prefix and not key.startswith(filter_prefix):
            continue
        if filter_suffix and not key.endswith(filter_suffix):
            continue
        out.append(key)
    return out

def _open_s3_object_stream(bucket: str, key: str):
    """
    Returns a file-like text stream for CSV content. Handles gzip if needed.
    Uses blocking I/O; we'll call it in a thread to avoid blocking the event loop.
    """
    s3 = boto3.client("s3",  region_name = AWS_REGION,
                                                    aws_access_key_id=AWS_ACCESS_KEY_ID,
                                                    aws_secret_access_key=AWS_SECRET_ACCESS_KEY_ID,
                                                    endpoint_url=ENDPOINT_URL)
    obj = s3.get_object(Bucket=bucket, Key=key)
    body = obj["Body"]
    raw = body.read()
    if key.endswith(".gz"):
        data = gzip.decompress(raw)
    else:
        data = raw
    # decode as text, inventory is UTF-8
    decoded_text = io.StringIO(data.decode("utf-8"))
    return decoded_text

async def iter_all_keys_from_inventory(
    inventory_bucket: str,
    inventory_prefix: str,
    filter_prefix: Optional[str],
    filter_suffix: Optional[str],
    progress_every: int = 1_000_000,
) -> AsyncIterator[str]:
    """
    Yields all object keys from all inventory CSV files under the given prefix.
    Streaming in manageable blocks (per-file).
    """
    inventory_files = await list_inventory_objects(inventory_bucket, inventory_prefix)
    total = 0
    for fkey in sorted(inventory_files):
        stream = await asyncio.to_thread(_open_s3_object_stream, inventory_bucket, fkey)
        try:
            keys = await asyncio.to_thread(_parse_inventory_csv_stream, stream, filter_prefix, filter_suffix)
        finally:
            stream.close()
        for k in keys:
            yield k
            total += 1
            if total % progress_every == 0:
                print(f"[inv] scanned {total:,} keys...", flush=True)
    print(f"[inv] total scanned keys: {total:,}", flush=True)

def pack_keys_into_messages(
    keys: List[str],
    keys_per_message: int,
) -> List[str]:
    """
    Pack many keys into as few SQS messages as possible without exceeding ~250KB body size.
    Returns a list of message bodies (JSON strings).
    """
    messages = []
    current = []
    current_size = approx_json_size_bytes({"s3_keys": current})
    for k in keys:
        candidate = {"s3_keys": current + [k]}
        cand_size = approx_json_size_bytes(candidate)
        if len(current) >= keys_per_message or cand_size > SQS_SAFE_BODY_BYTES:
            # finalize current
            messages.append(json.dumps({"s3_keys": current}, separators=(",", ":")))
            current = [k]
            current_size = approx_json_size_bytes({"s3_keys": current})
        else:
            current.append(k)
            current_size = cand_size
    if current:
        messages.append(json.dumps({"s3_keys": current}, separators=(",", ":")))
    return messages

def fifo_ids(body: str, group_id: str) -> Tuple[str, str]:
    """
    Produce deterministic deduplication id for FIFO queues.
    """
    md5 = zlib.adler32(body.encode("utf-8"))  # quick checksum; okay for dedup intent
    return group_id, f"{md5}"

async def send_batch(
    sqs_client,
    queue_url: str,
    message_bodies: List[str],
    fifo: bool,
    fifo_group_id: Optional[str],
    max_retries: int = 7,
):
    """
    Sends up to 10 messages using SendMessageBatch with retries/backoff.
    """
    entries = []

    for i, body in enumerate(message_bodies[:SQS_BATCH_MAX]):

        entry = {
            "Id": str(i),
            "MessageBody": body,
        }
        if fifo:
            gid, did = fifo_ids(body, fifo_group_id or "inventory-enqueue")
            entry["MessageGroupId"] = gid
            entry["MessageDeduplicationId"] = did
        entries.append(entry)

    backoff = 0.5
    for attempt in range(max_retries):
        try:
            resp = await sqs_client.send_message_batch(QueueUrl=queue_url, Entries=entries)
            failed = resp.get("Failed", [])
            if failed:
                # retry only failed ones
                logger.warning(f"[warn] {len(failed)} failed in batch; retrying...", flush=True)
                retry_ids = {f["Id"] for f in failed}
                entries = [e for e in entries if e["Id"] in retry_ids]
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 8.0)
                continue
            return
        except botocore.exceptions.ClientError as e:
            # Usually throttling; retry
            print(f"[error] batch send attempt {attempt+1} failed: {e}", flush=True)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 8.0)
    raise RuntimeError("send_batch: exhausted retries")

async def producer(
    out_q: asyncio.Queue,
    inventory_bucket: str,
    inventory_prefix: str,
    filter_prefix: Optional[str],
    filter_suffix: Optional[str],
    keys_per_message: int,
    dry_run: bool,
    progress_every: int = 100_000,
):
    """
    Reads inventory, packs keys into message bodies, and enqueues into an asyncio queue for senders.
    """
    count_keys = 0
    count_msgs = 0

    # buffer keys to pack in bigger chunks to reduce JSON overhead
    buffer: List[str] = []
    async for key in iter_all_keys_from_inventory(inventory_bucket, inventory_prefix, filter_prefix, filter_suffix):
        count_keys += 1
        buffer.append(key)
        if len(buffer) >= max(keys_per_message, MAX_KEYS_PER_MESSAGE):  # max(keys_per_message * 10, 10_000)# coarse pack threshold
            msgs = pack_keys_into_messages(buffer, keys_per_message)
            buffer.clear()
            for m in msgs:
                await out_q.put(m)
                count_msgs += 1
            if count_keys % progress_every == 0:
                logger.info(f"[pack] {count_keys:,} keys -> {count_msgs:,} msgs", flush=True)

    if buffer:
        msgs = pack_keys_into_messages(buffer, keys_per_message)
        for m in msgs:
            await out_q.put(m)
            count_msgs += 1

    print(f"[pack] FINAL: {count_keys:,} keys -> {count_msgs:,} msgs", flush=True)
    # signal consumers to stop
    for _ in range(4):
        await out_q.put(None)

async def consumer(
    in_q: asyncio.Queue,
    queue_url: str,
    fifo: bool,
    fifo_group_id: Optional[str],
    concurrency: int,
    dry_run: bool,
):
    """
    Pulls message bodies from queue, sends to SQS in batches of 10.
    """
    session = aioboto3.Session()
    async with session.client("sqs", region_name=AWS_REGION, endpoint_url=ENDPOINT_URL, aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY_ID) as sqs_client:
        pending: List[str] = []
        sent_msgs = 0
        start = time.time()

        async def flush_pending():
            nonlocal sent_msgs
            if not pending:
                return
            if dry_run:
                sent_msgs += len(pending)
            else:
                # send in batches of 10
                for group in chunked(pending, SQS_BATCH_MAX):
                    await send_batch(sqs_client, queue_url, group, fifo=fifo, fifo_group_id=fifo_group_id)
                    sent_msgs += len(group)

            pending.clear()

        while True:
            body = await in_q.get()
            if body is None:
                # drain remaining
                await flush_pending()
                in_q.task_done()
                break
            pending.append(body)
            # flush opportunistically if we have enough buffered
            if len(pending) >= MAX_FLUSH_SIZE:  # 200# tune this for throughput/memory
                await flush_pending()
            in_q.task_done()

        dur = time.time() - start
        rate = sent_msgs / dur if dur > 0 else 0.0
        print(f"[send] sent {sent_msgs:,} messages in {dur:.1f}s ({rate:.1f} msg/s)", flush=True)

async def main():
    p = argparse.ArgumentParser(description="One-time bulk enqueue of existing S3 objects from S3 Inventory into SQS.")
    p.add_argument("--inventory-bucket", default=INPUT_S3_BUCKET)
    p.add_argument("--inventory-prefix", default='inventory/s3_inventory.csv', help="Prefix that contains the inventory CSV(.gz) files for a given date.")
    p.add_argument("--sqs-queue-url", default=ENQUEUE_PDF_SQS_URL)
    p.add_argument("--filter-prefix", default=None)
    p.add_argument("--filter-suffix", default=None, help="e.g., .pdf")
    p.add_argument("--keys-per-message", type=int, default=2, help="Target keys per message; script size-limits to <=256KB.")
    p.add_argument("--concurrency", type=int, default=64, help="Number of concurrent senders.")
    p.add_argument("--fifo", action="store_true", help="If the SQS queue is FIFO, enables dedup/grouping.")
    p.add_argument("--fifo-group-id", default="inventory-enqueue")
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    q: asyncio.Queue = asyncio.Queue(maxsize=5000)

    prod = asyncio.create_task(
        producer(
            q,
            args.inventory_bucket,
            args.inventory_prefix,
            args.filter_prefix,
            args.filter_suffix,
            args.keys_per_message,
            args.dry_run,
        )
    )

    # A few consumers is usually enough; each already batches internally.
    consumers = [
        asyncio.create_task(
            consumer(q, ENQUEUE_PDF_SQS_URL, args.fifo, args.fifo_group_id, args.concurrency, args.dry_run)
        )
        for _ in range(4)
    ]

    await prod
    await q.join()  # wait until producer's messages processed
    # signal shutdown already sent; now await consumers
    for c in consumers:
        await c

if __name__ == "__main__":
    asyncio.run(main())

import os
import tempfile
import asyncio
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from aioboto3 import Session
from typing import Tuple
from .config import AWS_REGION

_executor = ThreadPoolExecutor(max_workers=4)

def parse_s3_uri(uri: str) -> Tuple[str, str]:
    # accepts s3://bucket/key or bucket/key
    if uri.startswith("s3://"):
        uri = uri[5:]
    parts = uri.split("/", 1)
    bucket = parts[0]
    key = parts[1] if len(parts) > 1 else ""
    return bucket, key

async def download_s3_to_file(s3_bucket_uri: str, s3_key: str, local_path: str):
    sess = Session(region_name=AWS_REGION)
    async with sess.client('s3') as s3:
        await s3.download_file(Bucket=s3_bucket_uri.replace("s3://","") if s3_bucket_uri.startswith("s3://") else s3_bucket_uri, Key=s3_key, Filename=local_path)

async def upload_file_to_s3(local_path: str, s3_uri: str):
    bucket, key = parse_s3_uri(s3_uri)
    sess = Session(region_name=AWS_REGION)
    async with sess.client('s3') as s3:
        await s3.upload_file(local_path, bucket, key)

def write_pages_to_parquet(records: list, out_path: str):
    """
    Blocking parquet write — run in threadpool.
    records: list of dicts with keys: s3_key, page_num, text
    """
    df = pd.DataFrame(records)
    table = pa.Table.from_pandas(df)
    pq.write_table(table, out_path)

async def write_parquet_async(records: list, out_path: str):
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(_executor, write_pages_to_parquet, records, out_path)

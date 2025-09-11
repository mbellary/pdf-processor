import os
from dotenv import load_dotenv

load_dotenv()

def _env(name, default=None):
    v = os.getenv(name)
    return v if v is not None else default

# AWS CREDENTIALS
AWS_ACCESS_KEY_ID=_env("AWS_ACCESS_KEY_ID", 'test')
AWS_SECRET_ACCESS_KEY_ID=_env("AWS_SECRET_ACCESS_KEY_ID", 'test')
AWS_DDB_ACCESS_KEY_ID=_env("AWS_DDB_ACCESS_KEY_ID", 'fake')
AWS_DDB_SECRET_ACCESS_KEY_ID=_env("AWS_DDB_SECRET_ACCESS_KEY_ID",'fake')
ENDPOINT_URL=_env("ENDPOINT_URL", 'http://localhost:4566')

INPUT_S3_BUCKET = _env("INPUT_S3_BUCKET")
INPUT_S3_PREFIX = _env("INPUT_S3_PREFIX", "")
OUTPUT_S3_BUCKET = _env("OUTPUT_S3_BUCKET")
OUTPUT_S3_PREFIX = _env("OUTPUT_S3_PREFIX", "")
MANIFEST_S3_BUCKET = _env("MANIFEST_S3_BUCKET")
MANIFEST_S3_KEY = _env("MANIFEST_S3_KEY", "manifest")

# SQS_QUEUE_URL = _env("SQS_QUEUE_URL")
# DLQ_SQS_QUEUE_URL = _env("DLQ_SQS_QUEUE_URL")

ENQUEUE_PDF_SQS_URL = _env("ENQUEUE_PDF_SQS_URL", "")
ENQUEUE_PDF_DLQ_URL = _env("ENQUEUE_PDF_DLQ_URL", "")
PDF_OCR_PARQUET_SQS_URL = _env("PDF_OCR_PARQUET_SQS_URL", "")
PDF_OCR_PARQUET_DLQ_URL = _env("PDF_OCR_PARQUET_DLQ_URL", "")


PDF_FILE_STATE = _env("PDF_FILE_STATE", "pdf-processing-state")
OCR_PARQUET_STATE = _env("OCR_PARQUET_STATE", "ocr_parquet_state")

SECRET_NAME = _env("SECRET_NAME")
AWS_REGION = _env("AWS_REGION", "us-east-1")
MAX_WORKERS = int(_env("MAX_WORKERS", 6))
MAX_RETRIES = int(_env("MAX_RETRIES", 3))
LOG_GROUP_NAME = _env("LOG_GROUP_NAME", "/pdf-processor/logs")
SQS_MAX_MSG_SIZE = int(_env("SQS_MAX_MSG_SIZE", 262144))  # 256 KB
SQS_SAFE_BODY_BYTES = int(_env("SQS_SAFE_BODY_BYTES", 256000))  # leave headroom for attributes/overhead
SQS_BATCH_MAX = int(_env("SQS_BATCH_MAX", 1))
SQS_MAX_MESSAGES = int(_env("SQS_MAX_MESSAGES", 10))
SQS_WAIT_TIME = int(_env("SQS_WAIT_TIME", 10))
PARQUET_MAX_CHUNK_SIZE_MB = int(_env("PARQUET_MAX_CHUNK_SIZE_MB", 128))
MAX_FLUSH_SIZE = int(_env("MAX_FLUSH_SIZE", 1))
KEYS_PER_MESSAGE = int(_env("KEYS_PER_MESSAGE", (1 * 1)))
MAX_KEYS_PER_MESSAGE = int(_env("MAX_KEYS_PER_MESSAGE", 10000))
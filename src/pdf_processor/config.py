import os
from dotenv import load_dotenv, dotenv_values
from pdf_processor.logger import get_logger
from pathlib import Path

logger = get_logger("pdf_worker.config")

PACKAGE_DIR = Path(__file__).resolve().parent


APP_ENV = os.getenv("APP_ENV", "production").lower()


if APP_ENV == "localstack":
    load_dotenv(PACKAGE_DIR / ".env.localstack")
    logger.info('Loaded localstack environment variables')
else:
    load_dotenv(PACKAGE_DIR / ".env.production")
    logger.info('Loaded production environment variables')


def _env(name, default=None):
    v = os.getenv(name)
    return v if v is not None else default

#aws region
AWS_REGION = _env("AWS_REGION", "ap-south-1")

# AWS CREDENTIALS
AWS_ACCESS_KEY_ID=_env("AWS_ACCESS_KEY_ID", None) #if use_localstack else None
AWS_SECRET_ACCESS_KEY=_env("AWS_SECRET_ACCESS_KEY", None) #if use_localstack else None
LOCALSTACK_URL=_env("LOCALSTACK_URL", None) #if use_localstack else None


# S3
INPUT_S3_BUCKET = _env("INPUT_S3_BUCKET", None)
INPUT_S3_PREFIX = _env("INPUT_S3_PREFIX", "")
OUTPUT_S3_BUCKET = _env("OUTPUT_S3_BUCKET")
OUTPUT_S3_PREFIX = _env("OUTPUT_S3_PREFIX", "")
MANIFEST_S3_BUCKET = _env("MANIFEST_S3_BUCKET")
MANIFEST_S3_KEY = _env("MANIFEST_S3_KEY", "manifest")

# Queue names - SQS/DLQ
PDF_SQS_QUEUE_NAME = _env("PDF_SQS_QUEUE_NAME", "pdf-queue")
PDF_DLQ_QUEUE_NAME = _env("PDF_DLQ_QUEUE_NAME", "pdf-dlq")
PDF_OCR_PARQUET_SQS_QUEUE_NAME = _env("PDF_OCR_PARQUET_SQS_QUEUE_NAME", "parquet-queue")
PDF_OCR_PARQUET_DLQ_QUEUE_NAME = _env("PDF_OCR_PARQUET_DLQ_QUEUE_NAME", "parquet-dlq")

#DynamoDB
PDF_FILE_STATE_NAME = _env("PDF_FILE_STATE_NAME", "pdf-processing-state")
OCR_PARQUET_STATE_NAME = _env("OCR_PARQUET_STATE_NAME", "ocr_parquet_state")


# app settings
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
# PDF Processor (Async, Multi-Machine, AWS Native)

This repository provides a **ready-to-run Python package** to process **millions of PDF files** stored in S3.  
It uses **async I/O, parallel multi-machine workers, SQS for work distribution, DynamoDB for global state, S3 for output, CloudWatch for logging, and Secrets Manager for credentials**.

The system extracts **per-page text** from PDFs and writes results into **Parquet** format.

---

## 🚀 Features

- **Horizontal scaling** across many machines/containers (ECS Fargate, EC2, or K8s).
- **Asynchronous processing** with `asyncio` + `aioboto3`.
- **Global state tracking** via DynamoDB (progress, retries, completion).
- **Manifest tracking** in S3 for auditing/merging.
- **Resilient queuing** with AWS SQS (and DLQ for failed jobs).
- **Retry & error handling** (with automatic DLQ routing).
- **Structured logging** to AWS CloudWatch.
- **Credential management** via AWS Secrets Manager.
- **Output in Parquet format**, suitable for downstream analytics.

---

## 📂 Project Structure
```bash
pdf_processor/
├── pdf_processor
│ ├── init.py
│ ├── main.py # entrypoint: python -m pdf_processor
│ ├── config.py # environment & config
│ ├── aws_clients.py # async boto3 client factories
│ ├── processor.py # per-PDF processing logic
│ ├── worker.py # SQS-driven worker loop
│ ├── manifest.py # manifest writer (S3)
│ ├── utils.py # helpers (S3 I/O, parquet writer)
│ └── logger.py # CloudWatch + console logging
├── requirements.txt
├── .env.example
├── run_worker.sh
└── README.md
```

## 🔧 Setup

### 1. Clone repo
```bash
git clone https://github.com/your-org/pdf-processor.git
cd pdf-processor
```

## Create Python environment
```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Configure .env
- Copy .env.example → .env and set values: 
- Ensure .env is in the project root.
```bash
INPUT_S3_BUCKET=s3://my-input-bucket/
INPUT_S3_PREFIX=pdfs
OUTPUT_S3_BUCKET=s3://my-output-bucket/
OUTPUT_S3_PREFIX=parquet
MANIFEST_S3_BUCKET=s3://my-manifest-bucket/
MANIFEST_S3_KEY=manifest
SQS_QUEUE_URL=https://sqs.us-east-1.amazonaws.com/123456789012/my-queue
DLQ_SQS_QUEUE_URL=https://sqs.us-east-1.amazonaws.com/123456789012/my-dlq
DYNAMO_TABLE_NAME=pdf-processing-state
SECRET_NAME=my/aws/creds
AWS_REGION=us-east-1
MAX_WORKERS=6
MAX_RETRIES=3
LOG_GROUP_NAME=/pdf-processor/logs

```
## 🛠️ AWS Infrastructure
- This worker expects existing AWS resources:
    1. Input S3 Bucket: contains PDF files (INPUT_S3_BUCKET).
    2. Output S3 Bucket: parquet files written here (OUTPUT_S3_BUCKET).
    3. SQS Queue: tasks with messages like:
        ```json
            {"s3_key": "pdfs/mydoc.pdf"}
        ```
    4. DLQ (Dead Letter Queue): for failed jobs.
    5. DynamoDB Table: state table (DYNAMO_TABLE_NAME).
        ```bash
            aws dynamodb create-table \
            --table-name pdf-processing-state \
            --attribute-definitions AttributeName=s3_key,AttributeType=S \
            --key-schema AttributeName=s3_key,KeyType=HASH \
            --billing-mode PAY_PER_REQUEST
        ```
    6. CloudWatch Log Group: defined in LOG_GROUP_NAME.
    7. IAM Role/Policy with permissions:
        - S3: GetObject, PutObject, ListBucket
        - SQS: ReceiveMessage, DeleteMessage, SendMessage, ChangeMessageVisibility
        - DynamoDB: GetItem, UpdateItem, PutItem
        - Logs: CreateLogStream, PutLogEvents
        - SecretsManager: GetSecretValue

## ▶️ Running the Worker
- Run locally:
```bash
    ./run_worker.sh
    # or
    python -m pdf_processor
```
- Run in background:
```bash
    nohup python -m pdf_processor > worker.log 2>&1 &
```
- Deploy as container on ECS Fargate (recommended for production).

## 📊 DynamoDB Schema
- Each PDF has a record:

| Attribute         | Type   | Description                           |
| ----------------- | ------ | ------------------------------------- |
| `s3_key`          | String | Primary key (S3 object key)           |
| `status`          | String | `"processing"`, `"done"`, `"failed"`  |
| `pages_processed` | Number | Count incremented per page            |
| `total_pages`     | Number | Set when finished                     |
| `attempts`        | Number | Retry attempts                        |
| `last_updated`    | String | ISO timestamp                         |
| `last_error`      | String | Last error message (truncated to 1KB) |

## 📒 Manifest File
- Workers append JSON lines to a manifest file in S3:
```json
    {"s3_key": "pdfs/doc1.pdf", "page": 3, "ts": "2025-08-30T05:33:20Z"}
```
- This provides an audit trail of progress.
(At very high throughput, prefer per-worker manifests and a merge step.)

## ⚙️ Message Processing Flow

1) SQS Message → Worker
    - Body contains PDF key (relative to INPUT_S3_BUCKET).
2) Check DynamoDB
    - If status=done, skip.
    - Otherwise set status=processing.
3) Download PDF (S3) and process per page.
4) Extract text (PyPDF2) → accumulate per-page records.
5) Upload Parquet to S3.
6) Update DynamoDB
    - Increment pages_processed per page.
    - On success: set status=done, total_pages.
    - On error: increment attempts, set status=failed.
7) Delete or retry SQS message.
8) Send to DLQ if max retries exceeded.

## 🔎 Logging
- Logs are sent to console and CloudWatch (LOG_GROUP_NAME).
- Example log line:
```bash
    2025-08-30 05:33:21,102 INFO pdf_processor.worker Processing message for pdfs/doc1.pdf
```
## 🧪 Testing Locally
- Use LocalStack for S3/SQS/DynamoDB emulation.
- localstack : https://github.com/localstack/localstack
```bash
    # Run worker against LocalStack
    AWS_ENDPOINT_URL=http://localhost:4566 \
    AWS_ACCESS_KEY_ID=test \
    AWS_SECRET_ACCESS_KEY=test \
    python -m pdf_processor
```

## 🐳 Docker / ECS
- Build image:
```bash
    docker build -t pdf-processor:latest .
```
- Run locally:
```bash
    docker run --env-file .env pdf-processor:latest
```
- Deploy via ECS Fargate using a TaskDefinition with:
    - NetworkMode: awsvpc
    - IAM Task Role granting S3/SQS/DynamoDB access
    - CPU/memory tuned to PDF load

## ⚠️ Limitations
- Text extraction uses PyPDF2. For scanned PDFs, integrate OCR (pdf2image + pytesseract or Textract).
- Manifest append is naïve (re-download/upload). For huge loads, use per-worker manifests + merge.
- Blocking libs (PyPDF2, pandas/pyarrow) run in a threadpool. If CPU is the bottleneck, scale out horizontally.
- Retries use ApproximateReceiveCount. Ensure SQS redrive policy is configured for DLQ.

## 📌 Next Steps
- Add OCR fallback for image-based PDFs.
- Add a "merge manifests" step.
- Terraform scripts for infra creation (S3, SQS, DynamoDB, ECS).
- Unit tests with LocalStack.
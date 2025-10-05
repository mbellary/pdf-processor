# pdf-processor

[![Powered by ChatGPT](https://img.shields.io/badge/Powered%20by-ChatGPT-10a37f?style=for-the-badge&logo=openai&logoColor=white)](https://openai.com/chatgpt)


The **pdf-processor** is a core service within the *Unstruct AI Modular Data Pipeline*, responsible for **processing and transforming PDF documents** stored in S3 into structured text and metadata for downstream NLP and embedding pipelines.

This repository supports **asynchronous, containerized processing**, includes **AWS integrations (S3, SQS, DynamoDB)**, and provides **monitoring** via Prometheus and Grafana.

---

## 🧩 System Architecture Overview

The `pdf-processor` works as a middle layer in the Unstruct architecture:

| Component | Repository | Description |
|------------|-------------|-------------|
| **File Loader** | [`file-loader`](https://github.com/mbellary/file-loader) | Uploads and queues files for processing |
| **PDF Processor** | [`pdf-processor`](https://github.com/mbellary/pdf-processor) | Converts PDFs to structured text and metadata |
| **Extractor** | `extraction` | Extracts entities, keywords, and context |
| **Embeddings** | `embeddings` | Generates vector embeddings (Titan/BGE models) |
| **Search** | `search` | Indexes processed data into OpenSearch |
| **Infra (Terraform)** | `infra` | Manages AWS ECS, VPC, Redis, DynamoDB, etc. |

The **pdf-processor** consumes SQS messages produced by the **file-loader**, processes the corresponding S3-stored PDF, and outputs results to:
- **S3** (for extracted text artifacts)
- **DynamoDB** (for file metadata and processing status)
- **SQS** (to trigger downstream services like extraction or embeddings)

---

## ⚙️ Core Responsibilities

- Polls **SQS queue** for new PDF processing jobs  
- Fetches PDF files from **S3**  
- Performs text extraction using OCR or PDF parsers (e.g., PyMuPDF, Tesseract)  
- Generates structured text and metadata JSON  
- Uploads processed data back to S3  
- Updates **DynamoDB** with job status and metadata  
- Publishes completion messages to next-stage **SQS** queue  
- Exposes Prometheus metrics for monitoring  

---

## 🏗️ Repository Structure

```
pdf-processor/
├─ src/pdf_processor/          # Core Python package
│  ├─ main.py                  # Entry point for worker
│  ├─ worker.py                # Orchestrates SQS polling and PDF processing
│  ├─ processor.py             # Handles PDF parsing and text extraction
│  ├─ aws_client.py            # AWS S3, SQS, DynamoDB utilities
│  ├─ metrics.py               # Prometheus metrics exporter
│  └─ __init__.py
├─ Dockerfile.dev              # Development Dockerfile
├─ Dockerfile.prod             # Production Dockerfile
├─ docker-compose.yml          # Compose setup with LocalStack, Prometheus, Grafana
├─ prometheus.yml              # Prometheus configuration
├─ requirements.txt            # Python dependencies
├─ pyproject.toml              # Project build config
├─ localstack_data/            # LocalStack persistent storage
├─ grafana_data/               # Grafana storage
├─ LICENSE                     # Apache License 2.0
└─ README.md                   # Project documentation
```

---

## 🚀 Quickstart

### 1️⃣ Prerequisites

- Python 3.10+
- Docker & Docker Compose
- LocalStack CLI (optional)
- AWS credentials configured (for non-local use)

### 2️⃣ Clone the repo

```bash
git clone https://github.com/mbellary/pdf-processor.git
cd pdf-processor
```

### 3️⃣ Run the service

```bash
docker compose up --build
```

This spins up:
- `pdf-processor` worker
- `localstack` (mock AWS for S3, SQS, DynamoDB)
- `prometheus` (metrics)
- `grafana` (dashboards)

> Prometheus → [http://localhost:9090](http://localhost:9090)  
> Grafana → [http://localhost:3000](http://localhost:3000)  
> LocalStack → [http://localhost:4566](http://localhost:4566)

---

## 🧠 Local Development

Create a virtual environment and install dependencies:

```bash
python -m venv .venv
source .venv/bin/activate   # (Windows: .venv\Scripts\activate)
pip install -r requirements.txt
python -m pdf_processor
```

---

## ⚙️ Configuration

Create a `.env` file with the following variables:

```env
# AWS and LocalStack
AWS_ACCESS_KEY_ID=test
AWS_SECRET_ACCESS_KEY=test
AWS_DEFAULT_REGION=ap-south-1
LOCALSTACK_ENDPOINT=http://localstack:4566

# S3 and SQS Resources
S3_INPUT_BUCKET=unstruct-ingestion-bucket
S3_OUTPUT_BUCKET=unstruct-processed-bucket
SQS_INPUT_QUEUE=unstruct-file-events
SQS_OUTPUT_QUEUE=unstruct-processed-events
DYNAMODB_TABLE=unstruct-file-metadata

# Processing
OCR_ENABLED=True
BATCH_SIZE=10

# Monitoring
PROMETHEUS_PORT=9092
LOG_LEVEL=INFO
```

To create buckets and queues in LocalStack:
```bash
docker exec -it localstack awslocal s3 mb s3://unstruct-ingestion-bucket
docker exec -it localstack awslocal s3 mb s3://unstruct-processed-bucket
docker exec -it localstack awslocal sqs create-queue --queue-name unstruct-file-events
docker exec -it localstack awslocal sqs create-queue --queue-name unstruct-processed-events
```

---

## 📦 Example Flow

1️⃣ The `file-loader` uploads a file to S3 and sends an SQS message:  
```json
{
  "bucket": "unstruct-ingestion-bucket",
  "key": "uploads/sample.pdf",
  "job_id": "12345"
}
```

2️⃣ `pdf-processor` receives this message, downloads `sample.pdf`, extracts text, and uploads:
```
s3://unstruct-processed-bucket/text/sample.txt
s3://unstruct-processed-bucket/meta/sample.json
```

3️⃣ It then sends an SQS message to trigger the extractor:
```json
{
  "bucket": "unstruct-processed-bucket",
  "key": "text/sample.txt",
  "status": "processed"
}
```

---

## 📊 Monitoring and Metrics

- **Prometheus** scrapes metrics from `/metrics` endpoint.  
- **Grafana** dashboards visualize processing rate, errors, and latency.

Metrics include:
- `pdf_files_processed_total`
- `pdf_processing_duration_seconds`
- `sqs_messages_consumed_total`
- `s3_upload_failures_total`

---

## 🧪 Testing

```bash
pytest -q
ruff check src
black src
```

---

## 🚀 Deployment

In production, this service runs on **AWS ECS Fargate**, configured by the **Terraform infra repository**.  

Key integrations:
- ECS Task Definition with IAM role granting access to S3, SQS, DynamoDB  
- Logs streamed to CloudWatch  
- Prometheus metrics scraped via ECS service discovery  
- Deployed via GitHub Actions pipeline on merge to `main`

---

## 🧭 Roadmap

- [X] Add asyncio-based parallel PDF parsing  
- [X] Integrate OpenAI and Amazon Bedrock OCR models  
- [X] Add retry strategy for failed S3 uploads  
- [X] Support multi-page and scanned PDF pipelines  
- [X] Add CI/CD workflows for ECS deploy  
- [ ] Extend Prometheus metrics and Grafana dashboards  

---

## 📜 License

Licensed under the [Apache License 2.0](./LICENSE).

---

## 🧾 Author

**Mohammed Ali**  
📧 [www.linkedin.com/in/mbellary](www.linkedin.com/in/mbellary)

🌐 [https://github.com/mbellary](https://github.com/mbellary)

---

### 🤖 Powered by [ChatGPT](https://openai.com/chatgpt)
_This project was documented and scaffolded with assistance from OpenAI’s ChatGPT._

---

> _Part of the **Unstruct Modular Data Pipeline** — a fully containerized, serverless-ready ecosystem for ingestion, processing, and search._

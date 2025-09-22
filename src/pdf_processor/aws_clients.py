import aioboto3
import boto3
import os
from pdf_processor.config import AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, LOCALSTACK_URL, APP_ENV
from pdf_processor.logger import get_logger

logger = get_logger("enqueue_worker.clients")

# aioboto3 session
_session = aioboto3.Session(region_name=AWS_REGION)
_boto3_session = boto3.session.Session(region_name=AWS_REGION)

# async clients factories
async def s3_client():
    return _session.client('s3',
                           endpoint_url=S3_ENDPOINT_URL
                           )

async def sqs_client():
    return _session.client('sqs',
                           endpoint_url=SQS_ENDPOINT_URL
                           )

async def dynamodb_client():
    return _session.client('dynamodb',
                           endpoint_url=DYNAMODB_ENDPOINT_URL
                           )

def s3_boto_client():
    return _boto3_session.client("s3",
                        endpoint_url=S3_ENDPOINT_URL)

def sqs_boto_client():
    return _boto3_session.client("sqs",
                        endpoint_url=SQS_ENDPOINT_URL)

def dynamodb_boto_client():
    return _boto3_session.client("dynamodb",
                        endpoint_url=DYNAMODB_ENDPOINT_URL)

def get_boto3_client(service):
    if APP_ENV == "localstack":
        # LocalStack setup
        logger.info(f"Initializing client {service} locally")
        return boto3.client(
            service,
            region_name=AWS_REGION,
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            endpoint_url=LOCALSTACK_URL
        )
    else:
        # Production: use IAM Role if available
        # If AWS_PROFILE is set, boto3 will use it.
        # If not, it will fall back to the IAM Role automatically.
        # Ensure no accidental env creds are used in production
        os.environ.pop("AWS_ACCESS_KEY_ID", None)
        os.environ.pop("AWS_SECRET_ACCESS_KEY", None)

        logger.info(f"Initializing client {service} in production")
        aws_profile = os.getenv("AWS_PROFILE")
        if aws_profile:
            logger.info(f"Initializing client {service} in production using AWS_PROFILE {aws_profile}")
            session = boto3.Session(profile_name=aws_profile)
            return session.client(service)
        else:
            # No profile → IAM Role will be used (via metadata service)
            logger.info(f"Initializing client {service} in production using IAM Role")
            return boto3.client(service, region_name=AWS_REGION)

async def get_aboto3_client(service):
    if APP_ENV == "localstack":
        # LocalStack setup
        logger.info(f"Initializing client {service} locally")
        return _session.client(
            service,
            region_name=AWS_REGION,
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            endpoint_url=LOCALSTACK_URL
        )
    else:
        # Production: use IAM Role if available
        # If AWS_PROFILE is set, boto3 will use it.
        # If not, it will fall back to the IAM Role automatically.
        logger.info(f"Initializing client {service} in production")
        os.environ.pop("AWS_ACCESS_KEY_ID", None)
        os.environ.pop("AWS_SECRET_ACCESS_KEY", None)
        aws_profile = os.getenv("AWS_PROFILE")
        if aws_profile:
            logger.info(f"Initializing client {service} in production using AWS_PROFILE {aws_profile}")
            profile_session = aioboto3.Session(region_name=AWS_REGION , profile_name=aws_profile)
            return profile_session.client(service)
        else:
            # No profile → IAM Role will be used (via metadata service)
            logger.info(f"Initializing client {service} in production using IAM Role")
            return _session.client(service, region_name=AWS_REGION)
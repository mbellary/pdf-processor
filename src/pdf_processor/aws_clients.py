import aioboto3
import boto3
from .config import AWS_REGION, SECRET_NAME

# aioboto3 session
_session = aioboto3.Session(region_name=AWS_REGION)

# async clients factories
async def s3_client():
    return _session.client('s3')

async def sqs_client():
    return _session.client('sqs')

async def dynamodb_client():
    return _session.client('dynamodb')

# For secrets we will use boto3 (sync)
def get_secret_sync(secret_name):
    client = boto3.client('secretsmanager', region_name=AWS_REGION)
    resp = client.get_secret_value(SecretId=secret_name)
    return resp.get('SecretString')

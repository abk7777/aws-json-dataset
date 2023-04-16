import os
import pytest
import boto3
from moto import (
    mock_s3,
    mock_firehose,
    mock_sns,
    mock_sqs,
    mock_sts
)

@pytest.fixture(scope='function')
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
    os.environ['AWS_SECURITY_TOKEN'] = 'testing'
    os.environ['AWS_SESSION_TOKEN'] = 'testing'
    os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'

@pytest.fixture(scope='function')
def sqs(aws_credentials):
    with mock_sqs():
        yield boto3.client('sqs', region_name='us-east-1')

@pytest.fixture(scope='function')
def sns(aws_credentials):
    with mock_sns():
        yield boto3.client('sns', region_name='us-east-1')

@pytest.fixture(scope='function')
def s3(aws_credentials):
    with mock_s3():
        yield boto3.client('s3', region_name='us-east-1')

@pytest.fixture(scope='function')
def firehose(aws_credentials):
    with mock_firehose():
        yield boto3.client('firehose', region_name='us-east-1')

@pytest.fixture(scope='function')
def sts(aws_credentials):
    with mock_sts():
        yield boto3.client('sts', region_name='us-east-1')
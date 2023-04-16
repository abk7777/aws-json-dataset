import sys
sys.path.append("../awsjsondataset")
import pytest
from pathlib import Path
from awsjsondataset.aws.models import (
    AwsServiceBase,
    SqsQueue,
    SnsTopic,
    KinesisFirehoseDeliveryStream
)
from tests.fixtures import *

@mock_sts
def test_aws_service_base_attrs(sts):
    service = AwsServiceBase()
    assert service.account_id == "123456789012"
    assert service.region_name == "us-east-1"

@mock_sts
@mock_sqs
def test_sqs_queue_attrs(sts, sqs):
    queue_url = sqs.create_queue(QueueName="test_queue")["QueueUrl"]
    _sqs = SqsQueue(queue_url=queue_url)
    assert _sqs.queue_url == queue_url
    assert _sqs.region_name == "us-east-1"
    assert _sqs.account_id == "123456789012"
    assert _sqs.client._endpoint.host == "https://sqs.us-east-1.amazonaws.com"

@mock_sts
@mock_sqs
def test_sqs_queue_send_messages(sts, sqs):

    queue_url = sqs.create_queue(QueueName="test_queue")["QueueUrl"]
    _sqs = SqsQueue(queue_url=queue_url)

    # queue small batch of records to the queue
    messages = [{"a": 1}, {"b": 2}]
    _sqs.data = messages
    response = _sqs.send_messages()
    assert response is None

    # queue records to the queue
    messages = [{"a": 1}, {"b": 2}]*100
    _sqs.data = messages
    response = _sqs.send_messages()
    assert response is None

@mock_sts
@mock_sns
def test_sns_topic_attrs(sts, sns):

    topic_arn = sns.create_topic(Name="test_topic")["TopicArn"]
    _sns = SnsTopic(topic_arn=topic_arn)
    assert _sns.topic_arn == topic_arn
    assert _sns.region_name == "us-east-1"
    assert _sns.account_id == "123456789012"
    assert _sns.client._endpoint.host == "https://sns.us-east-1.amazonaws.com"

@mock_sts
@mock_sns
def test_sns_topic_publish_messages(sts, sns):

    topic_arn = sns.create_topic(Name="test_topic")["TopicArn"]
    _sns = SnsTopic(topic_arn=topic_arn)

    # publish small batch of messages to the topic
    with pytest.raises(Exception):
        _sns.data = [{"a": 1}, {"b": 2}]
        response = _sns.publish_messages()
        assert response is None

    # publish records to the topic
    _sns.data = [{"a": 1}, {"b": 2}]*100
    response = _sns.publish_messages()
    assert response is None

@mock_s3
@mock_sts
@mock_firehose
def test_kinesis_firehose_delivery_stream(s3, sts, firehose):

    # create a mock bucket
    DATA_BUCKET_NAME = 'data-bucket'
    response = s3.create_bucket(Bucket=DATA_BUCKET_NAME)

    # create a mock delivery stream
    DELIVERY_STREAM_NAME = "data-delivery-stream"
    response = firehose.create_delivery_stream(
        DeliveryStreamName=DELIVERY_STREAM_NAME,
        ExtendedS3DestinationConfiguration={
            'RoleARN': 'arn:aws:iam::123456789012:role/firehose_delivery_role',
            'BucketARN': f'arn:aws:s3:::{DATA_BUCKET_NAME}'
        })

    firehose = KinesisFirehoseDeliveryStream(stream_name=DELIVERY_STREAM_NAME)
    assert firehose.stream_name == DELIVERY_STREAM_NAME
    assert firehose.region_name == "us-east-1"
    assert firehose.account_id == "123456789012"
    assert firehose.client._endpoint.host == "https://firehose.us-east-1.amazonaws.com"

    # put a small batch of records to the delivery stream
    with pytest.raises(Exception):
        firehose.data = [{"a": 1}, {"b": 1234567891011}]
        response = firehose.put_records()

    # put a large batch of records to the delivery stream
    firehose.data = [{"a": 1}, {"b": 1234567891011}]*100
    response = firehose.put_records()
    assert response is None



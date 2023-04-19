import sys
import logging
from typing import List, Union, Dict
import json
import boto3
from botocore.exceptions import ClientError
from awsjsondataset.exceptions import MissingRecords
from awsjsondataset.types import JSONDataset
from awsjsondataset.constants import service_size_limits_bytes
from awsjsondataset.utils import (
    get_record_size_bytes
)

# initialize logger using basicConfig
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

### SQS ###
def send_messages(client, messages: JSONDataset, queue_url: str):

    if len(messages) > 10:
        return send_message_batch(client, messages, queue_url)
    else:
        counter = 0
        errors = 0

        for message in messages:
            counter += 1
            response = client.send_message(
                QueueUrl=queue_url,
                MessageBody=json.dumps(message)
            )

            if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
                logger.error("Failed to send message")
                counter -= 1
                errors += 1

        logger.info(f'{counter} messages queued to {queue_url}')


def send_message_batch(client, messages: JSONDataset, queue_url: str):

    if len(messages) < 10:
        raise Exception("Batch size must be greater than 10")

    batch = []
    batch_bytes = 0
    counter = 0
    max_bytes = service_size_limits_bytes["sqs"]["max_record_size_bytes"]

    # SQS API accepts a max batch size of 10 max payload size of 256 kilobytes
    for idx, record in enumerate(messages):
        if get_record_size_bytes(record) > service_size_limits_bytes["sqs"]["max_record_size_bytes"]:
            raise Exception(f'Record size must be less than {service_size_limits_bytes["sqs"]["max_record_size_bytes"]} bytes')

        if (batch_bytes + get_record_size_bytes(record) < max_bytes) and (len(batch) < 10):
            batch.append(record)
        else:
            entries = [
                {
                    'Id': str(batch_idx),
                    'MessageBody': json.dumps(message)
                } for batch_idx, message in enumerate(batch)
            ]

            response = client.send_message_batch(
                QueueUrl=queue_url,                
                Entries=entries)
            if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
                logger.error("Failed to send message")
                counter -= 1
                errors += 1

            counter += len(batch)
            batch = [record]

        batch_bytes = get_record_size_bytes(batch)

    # Publish remaining JSON objects
    if len(batch) > 0:
        response = client.send_message_batch(
            QueueUrl=queue_url,            
            Entries=entries)
        if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
            logger.error("Failed to send message")
            counter -= len(batch)
            errors += 1
        counter += len(batch)

        if counter != idx+1:
            raise MissingRecords(expected=idx+1, actual=counter)

### SNS ###
def publish_record(client, message: dict, topic_arn: str):
    """Send a message to an SNS topic.

    Args:
        client (SNS.Client): Boto3 client for SNS.
        topic_arn (str): SNS Topic ARN.
        message (dict): Message dict.

    Raises:
        error: ClientError

    Returns:
        dict: The response from SNS that contains the HTTP status and the list of successful and failed messages.
    """
    try:
        response = client.publish(
            TopicArn=topic_arn,
            Message=json.dumps(message)
        )
        return response
    except ClientError as e:
        logger.error(e)
        raise e

def publish_messages_batch(client, messages: list, topic_arn: str, message_attributes: list = None) -> dict:
    """Send a batch of messages in a single request to an SNS topic.
    This request may return overall success even when some messages were not published.
    The caller must inspect the Successful and Failed lists in the response and
    republish any failed messages.

    Args:
        client (SNS.Client): Boto3 client for SNS.
        topic_arn (str): SNS Topic ARN.
        messages (list): List of messages.
        message_attributes (list, optional): List of attributes for each message, used for filtering. Defaults to None.

    Raises:
        error: ClientError

    Returns:
        dict: The response from SNS that contains the HTTP status and the list of successful and failed messages.
    """

    if len(messages) < 10:
        raise Exception("Batch size must be greater than 10")

    batch = []
    batch_bytes = 0
    counter = 0
    max_bytes = service_size_limits_bytes["sns"]["max_record_size_bytes"]

    # SNS API accepts a max batch size of 10 max payload size of 256 kilobytes
    for idx, record in enumerate(messages):
        if get_record_size_bytes(record) > service_size_limits_bytes["sns"]["max_record_size_bytes"]:
            raise Exception(f'Record size must be less than {service_size_limits_bytes["sns"]["max_record_size_bytes"]} bytes')

        if (batch_bytes + get_record_size_bytes(record) < max_bytes) and (len(batch) < 10):
            batch.append(record)
        else:
            entries = [
                {
                    'Id': str(idx),
                    'Message': json.dumps(message)
                } for idx, message in enumerate(batch)
            ]

            # TODO: Add message attributes
            # if message_attributes is not None:
            #     for item, attrs in zip(entries, message_attributes):
            #         item["MessageAttributes"] = attrs

            # logger.info(json.dumps(entries, indent=4))
            logger.info(len(entries))
            response = client.publish_batch(
                TopicArn=topic_arn,
                PublishBatchRequestEntries=entries)

            if 'Successful' in response.keys():
                # logger.info(f'Messages published: {json.dumps(response["Successful"])}')
                logger.info(len(response["Successful"]))
            if 'Failed' in response.keys():
                if len(response['Failed']) > 0:
                    logger.info(f'Messages failed: {json.dumps(response["Failed"])}')
                    raise Exception("Failed to publish messages")
                
            counter += len(batch)
            batch = [record]

        batch_bytes = get_record_size_bytes(batch)

    # Publish remaining JSON objects
    if len(batch) > 0:
        entries = [
            {
                'Id': str(idx),
                'Message': json.dumps(message)
            } for idx, message in enumerate(batch)
        ]

        # TODO: Add message attributes
        # if message_attributes is not None:
        #     for item, attrs in zip(entries, message_attributes):
        #         item["MessageAttributes"] = attrs

        response = client.publish_batch(
            TopicArn=topic_arn,
            PublishBatchRequestEntries=entries)

        if 'Successful' in response.keys():
            # logger.info(f'Messages published: {json.dumps(response["Successful"])}')
            logger.info(len(response["Successful"]))
        if 'Failed' in response.keys():
            if len(response['Failed']) > 0:
                logger.info(f'Messages failed: {json.dumps(response["Failed"])}')
                raise Exception("Failed to publish messages")
            
        counter += len(batch)

        if counter != idx+1:
            raise MissingRecords(expected=idx+1, actual=counter)

### Kinesis Firehose ###
def put_record(client, stream_name: str, data: str) -> Dict:
    """Streams a record to AWS Kinesis Firehose.

    Args:
        client (boto3.client): Kinesis Firehose client.
        stream_name (str): Name of Firehose delivery stream.
        data (str): Record to stream.

    Returns:
        Dict: Response from Kinesis Firehose service.
    """

    # validate record size
    if get_record_size_bytes(data) > 1000000:
        raise Exception("Record size must be less than 1 megabyte")

    response = client.put_record(
        DeliveryStreamName=stream_name,
        Record={
            'Data': "".join([json.dumps(data, ensure_ascii=False), "\n"]).encode('utf8')
        })
    
    return response


def put_records_batch(client, stream_name: str, records: list) -> Dict:
    """Streams a batch of records to AWS Kinesis Firehose.

    Args:
        client (boto3.client): Kinesis Firehose client.
        stream_name (str): Name of Firehose delivery stream.
        records (list): List of records.

    Returns:
        Dict: Response from Kinesis Firehose service.
    """

    if len(records) < 10:
        raise Exception("Total records must be greater than 10")

    batch = []
    batch_bytes = 0
    counter = 0
    max_bytes = service_size_limits_bytes["firehose"]["max_batch_size_bytes"]

    # Kinesis API accepts a max batch size of 500 max payload size of 5 megabytes
    for idx, record in enumerate(records):
        if get_record_size_bytes(record) > service_size_limits_bytes["firehose"]["max_record_size_bytes"]:
            raise Exception("Record size must be less than 1 megabyte")

        if (batch_bytes + get_record_size_bytes(record) < max_bytes) and (len(batch) < 500):
            batch.append(record)
        else:
            entries = [
                {
                    'Data': "".join([json.dumps(message, ensure_ascii=False), "\n"]).encode('utf8')
                } for message in batch
            ]

            response = client.put_record_batch(
                DeliveryStreamName=stream_name,
                Records=entries)

            if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
                logger.error("Failed to send message")
                counter -= 1
                errors += 1

            counter += len(batch)
            batch = [record]

        batch_bytes = get_record_size_bytes(batch)

    # Publish remaining JSON objects
    if len(batch) > 0:
        entries = [
            {
                'Data': "".join([json.dumps(message, ensure_ascii=False), "\n"]).encode('utf8')
            } for message in batch
        ]

        response = client.put_record_batch(
            DeliveryStreamName=stream_name,
            Records=entries)

        if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
            logger.error("Failed to send message")
            counter -= len(batch)
            errors += 1
        counter += len(batch)

        if counter != idx+1:
            raise MissingRecords(expected=idx+1, actual=counter)
        
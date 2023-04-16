import sys
import logging
from typing import List, Union, Dict
import json
import boto3
from botocore.exceptions import ClientError
from .types import JSONDataset
from .exceptions import InvalidJsonDataset, MissingRecords, ServiceRecordSizeLimitExceeded
from .constants import (
    service_size_limits_bytes,
    available_services
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def get_record_size_bytes(record: dict) -> int:
    """Get the size of a record in bytes.

    Returns:
        int: Size of record in bytes
    """
    return sys.getsizeof(json.dumps(record))


def sort_records_by_size_bytes(data: JSONDataset, ascending: bool = True):
    records_by_size_bytes = [(record, get_record_size_bytes(record)) for record in data]
    records_by_size_bytes.sort(key=lambda record: record[1], reverse=not ascending)
    return records_by_size_bytes


def max_record_size_bytes(data: JSONDataset):
    return max([ item[1] for item in sort_records_by_size_bytes(data=data, ascending=False) ])


def validate_data(data: JSONDataset) -> List[Union[Dict, any]]:
    """Validate that a list of records is a list of dictionaries.
    
    Args:
        data (List[Union[Dict, any]]): A list of records.

    Returns:
        bool: True if all records are dictionaries, False otherwise.
    """
    if not all([ isinstance(item, dict) for item in data ]):
        raise InvalidJsonDataset()
    return data


def get_available_services_by_limit(max_record_size_bytes):
    service_size_record_limits_bytes = { k: v["max_record_size_bytes"] for k, v in service_size_limits_bytes.items() }
    service_status = [ (k, max_record_size_bytes < v)  for k, v in service_size_record_limits_bytes.items() ]
    return [ x[0] for x in list(filter(lambda x: x[1] == True, service_status)) ]


# def validate_service(service: str, max_record_size_bytes: float) -> str:
#     if service not in available_services:
#         raise ValueError(f"Invalid service: {service}")
#     else:
#         available_services_by_limit = get_available_services_by_limit(service, max_record_size_bytes)
#         if service not in available_services_by_limit:
#             raise ServiceRecordSizeLimitExceeded(service, max_record_size_bytes)
#         else:
#             return service


# ### SQS ###
# def send_messages(client, data: JSONDataset, sqs_queue_url: str):

#     if len(data) > 10:
#         return send_message_batch(client, data, sqs_queue_url)
#     else:
#         counter = 0
#         errors = 0

#         for record in data:
#             counter += 1
#             response = client.send_message(
#                 QueueUrl=sqs_queue_url,
#                 MessageBody=json.dumps(record)
#             )

#             if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
#                 logger.error("Failed to send message")
#                 counter -= 1
#                 errors += 1

#         logger.info(f'{counter} messages queued to {sqs_queue_url}')


# def send_message_batch(client, data: JSONDataset, sqs_queue_url: str):

#     if len(data) < 10:
#         raise Exception("Batch size must be greater than 10")

#     batch = []
#     batch_bytes = 0
#     counter = 0
#     max_bytes = service_size_limits_bytes["sqs"]["max_record_size_bytes"]

#     # SQS API accepts a max batch size of 10 max payload size of 256 kilobytes
#     for idx, record in enumerate(data):
#         if get_record_size_bytes(record) > service_size_limits_bytes["sqs"]["max_record_size_bytes"]:
#             raise Exception(f'Record size must be less than {service_size_limits_bytes["sqs"]["max_record_size_bytes"]} bytes')

#         if (batch_bytes + get_record_size_bytes(record) < max_bytes) and (len(batch) < 10):
#             batch.append(record)
#         else:
#             entries = [
#                 {
#                     'Id': str(batch_idx),
#                     'MessageBody': json.dumps(message)
#                 } for batch_idx, message in enumerate(batch)
#             ]

#             response = client.send_message_batch(
#                 QueueUrl=sqs_queue_url,                
#                 Entries=entries)
#             if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
#                 logger.error("Failed to send message")
#                 counter -= 1
#                 errors += 1

#             counter += len(batch)
#             batch = [record]

#         batch_bytes = get_record_size_bytes(batch)

#     # Publish remaining JSON objects
#     if len(batch) > 0:
#         response = client.send_message_batch(
#             QueueUrl=sqs_queue_url,            
#             Entries=entries)
#         if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
#             logger.error("Failed to send message")
#             counter -= len(batch)
#             errors += 1
#         counter += len(batch)

#         if counter != idx+1:
#             raise MissingRecords(expected=idx+1, actual=counter)

# ### SNS ###
# def publish_record(client, message: dict, topic_arn: str):
#     """Send a message to an SNS topic.

#     Args:
#         client (SNS.Client): Boto3 client for SNS.
#         topic_arn (str): SNS Topic ARN.
#         message (dict): Message dict.

#     Raises:
#         error: ClientError

#     Returns:
#         dict: The response from SNS that contains the HTTP status and the list of successful and failed messages.
#     """
#     try:
#         response = client.publish(
#             TopicArn=topic_arn,
#             Message=json.dumps(message)
#         )
#         return response
#     except ClientError as e:
#         logger.error(e)
#         raise e

# def publish_messages_batch(client, messages: list, topic_arn: str, message_attributes: list = None) -> dict:
#     """Send a batch of messages in a single request to an SNS topic.
#     This request may return overall success even when some messages were not published.
#     The caller must inspect the Successful and Failed lists in the response and
#     republish any failed messages.

#     Args:
#         client (SNS.Client): Boto3 client for SNS.
#         topic_arn (str): SNS Topic ARN.
#         messages (list): List of messages.
#         message_attributes (list, optional): List of attributes for each message, used for filtering. Defaults to None.

#     Raises:
#         error: ClientError

#     Returns:
#         dict: The response from SNS that contains the HTTP status and the list of successful and failed messages.
#     """

#     if len(messages) < 10:
#         raise Exception("Batch size must be greater than 10")

#     batch = []
#     batch_bytes = 0
#     counter = 0
#     max_bytes = service_size_limits_bytes["sns"]["max_record_size_bytes"]

#     # SNS API accepts a max batch size of 10 max payload size of 256 kilobytes
#     for idx, record in enumerate(messages):
#         if get_record_size_bytes(record) > service_size_limits_bytes["sns"]["max_record_size_bytes"]:
#             raise Exception(f'Record size must be less than {service_size_limits_bytes["sns"]["max_record_size_bytes"]} bytes')

#         if (batch_bytes + get_record_size_bytes(record) < max_bytes) and (len(batch) < 10):
#             batch.append(record)
#         else:
#             entries = [
#                 {
#                     'Id': str(idx),
#                     'Message': json.dumps(message)
#                 } for idx, message in enumerate(batch)
#             ]

#             # TODO: Add message attributes
#             # if message_attributes is not None:
#             #     for item, attrs in zip(entries, message_attributes):
#             #         item["MessageAttributes"] = attrs

#             # logger.info(json.dumps(entries, indent=4))
#             logger.info(len(entries))
#             response = client.publish_batch(
#                 TopicArn=topic_arn,
#                 PublishBatchRequestEntries=entries)

#             if 'Successful' in response.keys():
#                 # logger.info(f'Messages published: {json.dumps(response["Successful"])}')
#                 logger.info(len(response["Successful"]))
#             if 'Failed' in response.keys():
#                 if len(response['Failed']) > 0:
#                     logger.info(f'Messages failed: {json.dumps(response["Failed"])}')
#                     raise Exception("Failed to publish messages")
                
#             counter += len(batch)
#             batch = [record]

#         batch_bytes = get_record_size_bytes(batch)

#     # Publish remaining JSON objects
#     if len(batch) > 0:
#         entries = [
#             {
#                 'Id': str(idx),
#                 'Message': json.dumps(message)
#             } for idx, message in enumerate(batch)
#         ]

#         # TODO: Add message attributes
#         # if message_attributes is not None:
#         #     for item, attrs in zip(entries, message_attributes):
#         #         item["MessageAttributes"] = attrs

#         response = client.publish_batch(
#             TopicArn=topic_arn,
#             PublishBatchRequestEntries=entries)

#         if 'Successful' in response.keys():
#             # logger.info(f'Messages published: {json.dumps(response["Successful"])}')
#             logger.info(len(response["Successful"]))
#         if 'Failed' in response.keys():
#             if len(response['Failed']) > 0:
#                 logger.info(f'Messages failed: {json.dumps(response["Failed"])}')
#                 raise Exception("Failed to publish messages")
            
#         counter += len(batch)

#         if counter != idx+1:
#             raise MissingRecords(expected=idx+1, actual=counter)

# ### Kinesis ###
# def put_record(client, stream_name: str, data: str) -> Dict:
#     """Streams a record to AWS Kinesis Firehose.

#     Args:
#         client (boto3.client): Kinesis Firehose client.
#         stream_name (str): Name of Firehose delivery stream.
#         data (str): Record to stream.

#     Returns:
#         Dict: Response from Kinesis Firehose service.
#     """

#     # validate record size
#     if get_record_size_bytes(data) > 1000000:
#         raise Exception("Record size must be less than 1 megabyte")

#     response = client.put_record(
#         DeliveryStreamName=stream_name,
#         Record={
#             'Data': "".join([json.dumps(data, ensure_ascii=False), "\n"]).encode('utf8')
#         })
    
#     return response


# def put_records_batch(client, stream_name: str, records: list) -> Dict:
#     """Streams a batch of records to AWS Kinesis Firehose.

#     Args:
#         client (boto3.client): Kinesis Firehose client.
#         stream_name (str): Name of Firehose delivery stream.
#         records (list): List of records.

#     Returns:
#         Dict: Response from Kinesis Firehose service.
#     """

#     if len(records) < 10:
#         raise Exception("Total records must be greater than 10")

#     batch = []
#     batch_bytes = 0
#     counter = 0
#     max_bytes = service_size_limits_bytes["kinesis_firehose"]["max_batch_size_bytes"]

#     # Kinesis API accepts a max batch size of 500 max payload size of 5 megabytes
#     for idx, record in enumerate(records):
#         if get_record_size_bytes(record) > service_size_limits_bytes["kinesis_firehose"]["max_record_size_bytes"]:
#             raise Exception("Record size must be less than 1 megabyte")

#         if (batch_bytes + get_record_size_bytes(record) < max_bytes) and (len(batch) < 500):
#             batch.append(record)
#         else:
#             entries = [
#                 {
#                     'Data': "".join([json.dumps(message, ensure_ascii=False), "\n"]).encode('utf8')
#                 } for message in batch
#             ]

#             response = client.put_record_batch(
#                 DeliveryStreamName=stream_name,
#                 Records=entries)

#             if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
#                 logger.error("Failed to send message")
#                 counter -= 1
#                 errors += 1

#             counter += len(batch)
#             batch = [record]

#         batch_bytes = get_record_size_bytes(batch)

#     # Publish remaining JSON objects
#     if len(batch) > 0:
#         entries = [
#             {
#                 'Data': "".join([json.dumps(message, ensure_ascii=False), "\n"]).encode('utf8')
#             } for message in batch
#         ]

#         response = client.put_record_batch(
#             DeliveryStreamName=stream_name,
#             Records=entries)

#         if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
#             logger.error("Failed to send message")
#             counter -= len(batch)
#             errors += 1
#         counter += len(batch)

#         if counter != idx+1:
#             raise MissingRecords(expected=idx+1, actual=counter)
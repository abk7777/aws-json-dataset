# Python models to wrap AWS services including S3, SNS, SQS, and Firehose
# Methods are available in awsjsondataset.services.utils
import os
import logging
from functools import cached_property
import boto3
from awsjsondataset.services.utils import (
    send_messages,
    publish_messages_batch,
    put_records_batch
)

# set up logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class AwsServiceBase:
    """A base class for AWS services.

    Args:
        boto3_session (boto3.session.Session): The boto3 session.

    Attributes:
        boto3_session (boto3.session.Session): The boto3 session.
    """
    def __init__(self) -> None:
        self.boto3_session = boto3.Session()
        self.region_name = self.boto3_session.region_name

    # get the account ID
    @cached_property
    def account_id(self):
        account_id = self.boto3_session.client('sts').get_caller_identity().get('Account')
        logger.info(f"Account ID: {account_id}")
        return account_id
    

class SqsQueue(AwsServiceBase):
    """A class to wrap an SQS queue.

    Args:
        queue_url (str): The URL of the SQS queue.

    Attributes:
        queue_url (str): The URL of the SQS queue.
        client (boto3.client): The boto3 client for the SQS queue.
    """

    def __init__(self, queue_url: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.queue_url = queue_url if queue_url.startswith("http") else f"https://sqs.{self.region_name}.amazonaws.com/{self.account_id}/{queue_url}"
        self.client = self.boto3_session.client('sqs')

    def send_messages(self) -> dict:
        """Queues records to the SQS queue.

        Args:
            record (dict): The records to queue.

        Returns:
            dict: The response from the SQS queue.
        """           
        return send_messages(client=self.client, messages=self.data, queue_url=self.queue_url)


class SnsTopic(AwsServiceBase):
    """A class to wrap an SNS topic.

    Args:
        topic_arn (str): The ARN of the SNS topic.
        region_name (str): The AWS region name.

    Attributes:
        topic_arn (str): The ARN of the SNS topic.
        region_name (str): The AWS region name.
        client (boto3.client): The boto3 client for the SNS topic.
    """
    def __init__(self, topic_arn: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.topic_arn = topic_arn
        self.client = self.boto3_session.client('sns')

    def publish_messages(self) -> dict:
        """Publishes a batch of messages to the SNS topic.

        Args:
            messages (list): The messages to publish.

        Returns:
            dict: The response from the SNS topic.
        """
        return publish_messages_batch(client=self.client, messages=self.data, topic_arn=self.topic_arn)


class KinesisFirehoseDeliveryStream(AwsServiceBase):
    """A class to wrap a Kinesis Firehose delivery stream.

    Args:
        stream_name (str): The name of the Kinesis Firehose delivery stream.
        region_name (str): The AWS region name.

    Attributes:
        stream_name (str): The name of the Kinesis Firehose delivery stream.
        region_name (str): The AWS region name.
        client (boto3.client): The boto3 client for the Kinesis Firehose delivery stream.
    """
    def __init__(self, stream_name: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.stream_name = stream_name
        self.client = self.boto3_session.client('firehose')

    def put_records(self) -> dict:
        """Puts a batch of records to the Kinesis Firehose delivery stream.

        Args:
            records (list): The records to put.

        Returns:
            dict: The response from the Kinesis Firehose delivery stream.
        """
        return put_records_batch(client=self.client, records=self.data, stream_name=self.stream_name)
    
# create service lookup map
aws_service_class_map = {
    "sqs": SqsQueue,
    "sns": SnsTopic,
    "firehose": KinesisFirehoseDeliveryStream
}
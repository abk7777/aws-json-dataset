# Python models to wrap AWS services including S3, SNS, SQS, and Firehose
# Methods are available in awsjsondataset.aws.utils
import logging
import boto3
from awsjsondataset.aws.utils import (
    queue_records,
    publish_records_batch,
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
    def __init__(self, boto3_session) -> None:
        self.boto3_session = boto3_session


class SqsQueue(AwsServiceBase):
    """A class to wrap an SQS queue.

    Args:
        queue_url (str): The URL of the SQS queue.
        region_name (str): The AWS region name.

    Attributes:
        queue_url (str): The URL of the SQS queue.
        region_name (str): The AWS region name.
        client (boto3.client): The boto3 client for the SQS queue.
    """
    def __init__(self, boto3_session, queue_url: str) -> None:
        super().__init__(boto3_session)
        self.queue_url = queue_url
        self.client = self.boto3_session.client('sqs')

    def queue_records(self, records: list) -> dict:
        """Queues records to the SQS queue.

        Args:
            record (dict): The records to queue.

        Returns:
            dict: The response from the SQS queue.
        """
        return queue_records(client=self.client, records=records, queue_url=self.queue_url)


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
    def __init__(self, boto3_session, topic_arn: str, region_name: str) -> None:
        super().__init__(boto3_session, region_name)
        self.topic_arn = topic_arn
        self.client = self.boto3_session.client('sns', region_name=self.region_name)

    def publish_records(self, messages: list) -> dict:
        """Publishes a batch of messages to the SNS topic.

        Args:
            messages (list): The messages to publish.

        Returns:
            dict: The response from the SNS topic.
        """
        return publish_records_batch(client=self.client, messages=messages, topic_arn=self.topic_arn)


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
    def __init__(self, boto3_session, stream_name: str, region_name: str) -> None:
        super().__init__(boto3_session, region_name)
        self.stream_name = stream_name
        self.client = self.boto3_session.client('firehose', region_name=self.region_name)

    def put_records_batch(self, records: list) -> dict:
        """Puts a batch of records to the Kinesis Firehose delivery stream.

        Args:
            records (list): The records to put.

        Returns:
            dict: The response from the Kinesis Firehose delivery stream.
        """
        return put_records_batch(client=self.client, records=records, stream_name=self.stream_name)
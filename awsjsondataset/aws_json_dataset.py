import os
import logging
from typing import List, Union, Iterator, Optional
from pathlib import Path
import json
import boto3
from exceptions import InvalidJsonDataset

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Explicit types
JSONDataset = List[dict]
JSONPath = Union[Path, str]


class DatetimeEncoder(json.JSONEncoder):
    """Extension of ``json.JSONEncoder`` to convert Python datetime objects to pure strings.

    Useful for responses from AWS service APIs. Does not convert timezone information.
    """

    def default(self, obj):
        try:
            return super().default(obj)
        except TypeError:
            return str(obj)

class JsonDataset:
    """
    A class to read and write JSON-formatted datasets.
    """

    def __init__(
        self,
        data: JSONDataset = None,
        path: JSONPath = None
    ) -> None:
        self.data: JSONDataset = data
        self.path: JSONPath = path

        if (not self.data) and (self.path is not None):
            self.load(self.path)

        self.num_records: int = len(self.data)


    def _read_local(self, path: JSONPath) -> JSONDataset:
        with open(path, 'r') as file:
            local_data = json.load(file)

        if not isinstance(local_data, List):
            del local_data
            raise InvalidJsonDataset()

        self.data = local_data

    def _write_local(self, path: JSONPath):
        with open(path, 'w', encoding='utf-8') as file:
            json.dump(
                self.data,
                file,
                ensure_ascii=False,
                indent=4,
                cls=DatetimeEncoder)

    def load(self, path: Path) -> JSONDataset:
        # TODO: support S3 download
        self._read_local(path)

    def save(self, path: Path):
        # TODO: support S3 upload
        return self._write_local(path)

    def __repr__(self) -> str:
        return f"JsonDataset(data='{json.dumps(self.data)[:30]}...',path='{str(self.path)}',num_records='{self.num_records}')"

class AwsJsonDataset(JsonDataset):

    # TODO: stream to SQS
    # TODO: stream to SNS

    def __init__(self,
            data: JSONDataset = None,
            path: JSONPath = None,
            **kwargs
        ) -> None:

        super().__init__(data, path)
        self._data: Optional[Iterator[str]] = iter(self.data) if self.data else None
        self._boto3_session = boto3.Session(region_name=os.environ["AWS_REGION"])

        # Set conditional attributes
        self._get_sqs(sqs_queue_url=kwargs.get("sqs_queue_url"))

    def _get_sqs(self, sqs_queue_url):
        self._sqs_queue_url = sqs_queue_url if sqs_queue_url else None
        self._sqs = self._boto3_session.resource('sqs') if sqs_queue_url else None
        self._sqs_queue = self._sqs.Queue(self._sqs_queue_url) if sqs_queue_url else None

    def queue_records(self, batch=False):

        if not batch:
            counter = 0
            errors = 0

            for record in self.data:
                counter += 1
                response = self._sqs_queue.send_message(
                    MessageBody=json.dumps(record)
                )

                if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
                    logger.error("Failed to send message")
                    counter -= 1
                    errors += 1

            logger.info(f'{counter} messages queued to {self._sqs_queue_url}')




if __name__ == "__main__":

    path = Path("___data/subtechniques.json")
    queue_url = "https://sqs.us-east-1.amazonaws.com/531868584498/dev-gfe-db-pipeline-FailedAllelesQueue-P0lkITOMth2s"

    awsdataset = AwsJsonDataset(path=path, sqs_queue_url=queue_url)
    awsdataset.queue_records()

    print("")

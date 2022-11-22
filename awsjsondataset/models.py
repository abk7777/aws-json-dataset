import os
import sys
import logging
from typing import List, Union, Iterator, Optional
from pathlib import Path
from functools import cached_property
import json
import boto3
from exceptions import InvalidJsonDataset, MissingRecords
from constants import service_size_limits_kb

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
        self.response: dict = None

        if (not self.data) and (self.path is not None):
            self.load(self.path)

        self.num_records: int = len(self.data)

    @staticmethod
    def _get_record_size_kb(record: dict) -> int:
        return round(sys.getsizeof(json.dumps(record))/1000, 2)

    @cached_property
    def _records_by_size_kb(self):
        _records_by_size_kb = [ (record, self._get_record_size_kb(record)) for record in self.data ]
        _records_by_size_kb.sort(key=lambda record: record[1])
        return _records_by_size_kb

    @cached_property
    def _max_record_size_kb(self):
        return max([ item[1] for item in self._records_by_size_kb ])

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

    # TODO: stream to Firehose
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

    @cached_property
    def _available_services(self):
        service_status = [ (k, self._max_record_size_kb < v)  for k, v in service_size_limits_kb.items() ]
        return [ x[0] for x in list(filter(lambda x: x[1] == True, service_status)) ]

    def _get_sqs(self, sqs_queue_url):
        if sqs_queue_url and ('sqs' in self._available_services):
            if self._max_record_size_kb > service_size_limits_kb["sqs"]:
                 logger.warn('Service size limit exceeded')
            self._sqs_queue_url = sqs_queue_url
            self._sqs = self._boto3_session.resource('sqs')
            self._sqs_queue = self._sqs.Queue(self._sqs_queue_url)
        else:
            self._sqs_queue_url = None
            self._sqs = None
            self._sqs_queue = None

    # TODO convert to staticmethod
    # TODO test batch=True/False
    def queue_records(self, batch=False):

        if batch:
            return self._queue_records_batch()
        else:
            counter = 0
            errors = 0

            for record in self.data:
                counter += 1
                self.response = self._sqs_queue.send_message(
                    MessageBody=json.dumps(record)
                )

                if self.response["ResponseMetadata"]["HTTPStatusCode"] != 200:
                    logger.error("Failed to send message")
                    counter -= 1
                    errors += 1

            logger.info(f'{counter} messages queued to {self._sqs_queue_url}')

    # TODO convert to staticmethod
    def _queue_records_batch(self):

        batch = []
        batch_bytes = 0
        counter = 0

        # SQS API accepts a max batch size of 10 max payload size of 256 bytes
        for record in self._data:
            if batch_bytes < 256 and len(batch) < 10:
                batch.append(record)
            else:
                entries = [
                    {
                        'Id': str(idx),
                        'MessageBody': json.dumps(message)
                    } for idx, message in enumerate(batch)
                ]

                self.response = self._sqs_queue.send_messages(Entries=entries)

                counter += len(batch)
                batch = [record]

            batch_bytes = self._get_record_size_kb(batch)

        # Publish remaining JSON objects
        if len(batch) > 0:
            self.response = self._sqs_queue.send_messages(Entries=entries)
            counter += len(batch)

            # TODO use idx instead of self.num_records
            if counter != self.num_records:
                raise MissingRecords(expected=self.num_records, actual=counter)

    def __repr__(self) -> str:
        return f"AwsJsonDataset(data='{json.dumps(self.data)[:30]}...',path='{str(self.path)}',num_records='{self.num_records}')"



if __name__ == "__main__":

    path = Path("___data/subtechniques.json")
    queue_url = "https://sqs.us-east-1.amazonaws.com/531868584498/dev-gfe-db-pipeline-FailedAllelesQueue-P0lkITOMth2s"

    awsdataset = AwsJsonDataset(path=path, sqs_queue_url=queue_url)
    awsdataset.queue_records(batch=True)
    print(awsdataset._available_services)

    # TODO test without queue url
    # TODO add max_size_kb

    print("")

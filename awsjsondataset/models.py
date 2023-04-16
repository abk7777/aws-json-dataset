import os
import sys
import logging
from typing import Iterator, Optional
from pathlib import Path
from functools import cached_property
import json
import boto3
from awsjsondataset.types import JSONDataset, JSONLocalPath
from awsjsondataset.exceptions import InvalidJsonDataset, ServiceRecordSizeLimitExceeded
from awsjsondataset.constants import service_size_limits_bytes, available_services
from awsjsondataset.utils import (
    sort_records_by_size_bytes, 
    max_record_size_bytes,
    validate_data,
    get_available_services_by_limit,
    validate_service
)
from awsjsondataset.aws.models import (
    SqsQueue,
)


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


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
    """A class to read and write JSON-formatted datasets.

    Args:
        data (JSONDataset): A list of dictionaries representing the dataset.
        path (JSONLocalPath): A path to a local JSON file.

    Attributes:
        data (JSONDataset): A list of dictionaries representing the dataset.
        path (JSONLocalPath): A path to a local JSON file.
        num_records (int): The number of records in the dataset.

    Raises:
        InvalidJsonDataset: Raised when an invalid dataset type is passed.
    """

    # trying slots for more speed
    __slots__ = '__dict__'

    def __init__(self, data: JSONDataset = None, path: Path = None) -> None:

        # if both data and path are passed, raise TypeError
        if data is not None and path is not None:
            raise TypeError(f"JsonDataset() takes either a path or data argument but not both")

        # assign from kwargs
        self.data: Optional[JSONDataset] = validate_data(data) if data is not None else None
        self.path: Optional[JSONLocalPath] = path if path is not None else None

        if self.path is not None:
            self.load(self.path)

    @property
    def num_records(self) -> int:
        return len(self.data)
    
    @cached_property
    def _sort_records_by_size_bytes(self):
        return sort_records_by_size_bytes(self.data)
    
    @cached_property
    def _max_record_size_bytes(self):
        return max_record_size_bytes(self.data)

    def _read_local(self, path: JSONLocalPath) -> JSONDataset:
        with open(path, 'r') as f:
            data = json.load(f)
            if not isinstance(data, list):
                raise InvalidJsonDataset()

        return data

    def _write_local(self, path: JSONLocalPath):
        with open(path, 'w', encoding='utf-8') as file:
            json.dump(
                self.data,
                file,
                ensure_ascii=False,
                indent=4,
                cls=DatetimeEncoder)

    def load(self, path: Path) -> JSONDataset:
        """Handles loading a dataset from a local or remote file.

        Args:
            path (Path): Path to a local or remote file.

        Returns:
            JSONDataset: A list of dictionaries representing the dataset.
        """
        # TODO: support S3 download
        self.data = self._read_local(path)

    def save(self, path: Path):
        # support writing to JSON lines
        # TODO: support S3 upload
        return self._write_local(path)

    def __len__(self) -> int:
        return self.num_records

    def __repr__(self) -> str:
        return f"JsonDataset(data='{json.dumps(self.data)[:30]}...',path='{str(self.path)}',num_records='{self.num_records}')"


# TODO break this out into separate classes: S3JsonDataset, SqsJsonDataset, etc.
class AwsJsonDataset(JsonDataset):

    def __init__(self,
            service: str,
            region: str = None,
            data: JSONDataset = None,
            path: JSONLocalPath = None,
        ) -> None:

        super().__init__(data, path)
        self.service = self._validate_service(service)
        self.region: str = region or os.environ.get("AWS_REGION")
        self._data: Optional[Iterator[str]] = iter(self.data) if self.data else None
        self._boto3_session = boto3.Session(region_name=self.region)

    def _validate_service(self, service: str) -> str:
        return validate_service(service, self._max_record_size_bytes)

    # get the account ID
    @cached_property
    def _account_id(self):
        account_id = self._boto3_session.client('sts').get_caller_identity().get('Account')
        logger.info(f"Account ID: {account_id}")
        return account_id

    @cached_property
    def _available_services(self):
        return get_available_services_by_limit(self.service, self._max_record_size_bytes)


    def __repr__(self) -> str:
        return f"AwsJsonDataset(data='{json.dumps(self.data)[:30]}...',path='{str(self.path)}',num_records='{self.num_records}')"


class SqsJsonDataset(AwsJsonDataset):

    def __init__(self,
            queue_url: str,
            service: str = "sqs",
            region: str = None,
            data: JSONDataset = None,
            path: JSONLocalPath = None,
        ) -> None:

        super().__init__(service, region, data, path)
        self.queue_url: str = queue_url
        self._queue = SqsQueue(self._boto3_session, self.queue_url)

    def queue_records(self):
        """Queues records to an SQS queue.
        """
        self._queue.queue_records(self.data)

    def __repr__(self) -> str:
        return f"AwsSqsJsonDataset(data='{json.dumps(self.data)[:30]}...',path='{str(self.path)}',num_records='{self.num_records}')"
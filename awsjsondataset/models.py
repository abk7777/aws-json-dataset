import os
import sys
import logging
from typing import Iterator, Optional
from pathlib import Path
from functools import cached_property
import json
import boto3
from .types import JSONDataset, JSONLocalPath
from .exceptions import InvalidJsonDataset
from .constants import service_size_limits_kb
from .utils import get_record_size_kb, sort_records_by_size_kb, validate_data

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
    def _sort_records_by_size_kb(self):
        return sort_records_by_size_kb(self.data)

    @cached_property
    def _max_record_size_kb(self):
        return max([ item[1] for item in self._sort_records_by_size_kb ])

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
            data: JSONDataset = None,
            path: JSONLocalPath = None,
            **kwargs
        ) -> None:

        super().__init__(data, path)
        self._data: Optional[Iterator[str]] = iter(self.data) if self.data else None
        self.region: str = os.environ.get("AWS_REGION") or kwargs.get("region")
        self._boto3_session = boto3.Session(region_name=self.region)

    @cached_property
    def _available_services(self):
        service_status = [ (k, self._max_record_size_kb < v)  for k, v in service_size_limits_kb.items() ]
        return [ x[0] for x in list(filter(lambda x: x[1] == True, service_status)) ]

    def __repr__(self) -> str:
        return f"AwsJsonDataset(data='{json.dumps(self.data)[:30]}...',path='{str(self.path)}',num_records='{self.num_records}')"


class SqsJsonDataset(AwsJsonDataset):

    def __init__(self,
            data: JSONDataset = None,
            path: JSONLocalPath = None,
            **kwargs
        ) -> None:

        super().__init__(data, path, **kwargs)

        # Set conditional attributes
        self._get_sqs(sqs_queue_url=kwargs.get("sqs_queue_url"))

    def _get_sqs(self, sqs_queue_url):
        if sqs_queue_url and ('sqs' in self._available_services):
            if self._max_record_size_kb > service_size_limits_kb["sqs"]:
                 logger.warn('Service size limit exceeded')
            self._sqs_queue_url = sqs_queue_url
            self._sqs = self._boto3_session.resource('sqs')
        else:
            self._sqs_queue_url = None
            self._sqs = None

    def __repr__(self) -> str:
        return f"AwsSqsJsonDataset(data='{json.dumps(self.data)[:30]}...',path='{str(self.path)}',num_records='{self.num_records}')"
import logging
from typing import Optional
from pathlib import Path
from functools import cached_property
import json
from awsjsondataset.types import JSONDataset, JSONLocalPath
from awsjsondataset.utils import (
    sort_records_by_size_bytes, 
    max_record_size_bytes,
    validate_data,
    get_available_services_by_limit
)
from awsjsondataset.services.models import aws_service_class_map

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
        TypeError: If both ``data`` and ``path`` are passed.
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
        # TODO support for JSON lines format
        # TODO support for multiple files
        with open(path, 'r') as f:
            data = json.load(f)
            if not isinstance(data, list):
                data = [data]

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
        return f"JsonDataset()"


class BaseAwsJsonDataset(JsonDataset):

    def __init__(self,
            data: JSONDataset = None,
            path: JSONLocalPath = None,
        ) -> None:

        super().__init__(data, path)

    @cached_property
    def available_services(self):
        return get_available_services_by_limit(self._max_record_size_bytes)

    def __repr__(self) -> str:
        return f"BaseAwsJsonDataset()"


class AwsJsonDataset(BaseAwsJsonDataset):

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

        for item in self.available_services:
            setattr(self, item, aws_service_class_map[item])
            setattr(self.__getattribute__(item), 'data', self.data)

    def __repr__(self) -> str:
        return f"AwsJsonDataset()"
    
import sys
import logging
from typing import List, Union, Dict
import json
import boto3
from botocore.exceptions import ClientError
from .types import JSONDataset
from .exceptions import InvalidJsonDataset
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


# TODO handle iterators
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

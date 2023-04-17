import sys
sys.path.append("../awsjsondataset")
import pytest
from awsjsondataset.exceptions import InvalidJsonDataset
from awsjsondataset.utils import (
    get_record_size_bytes,
    sort_records_by_size_bytes,
    max_record_size_bytes,
    validate_data,
    get_available_services_by_limit
)
from tests.fixtures import *

def test_get_record_size_bytes():
    record = {"a": 1}
    assert get_record_size_bytes(record) == 57

def test_sort_records_by_size_bytes():
    records = [{"a": 1}, {"b": 1234567891011}]
    assert sort_records_by_size_bytes(records) == [(records[0], 57), (records[1], 69)]

    records = [{"a": 1}, {"b": 1234567891011}]
    assert sort_records_by_size_bytes(records, ascending=False) == [(records[1], 69), (records[0], 57)]

def test_validate_data():
    data = [{"a": 1}, {"b": 1234567891011}]
    assert validate_data(data) == data

    data = [1, 2, 3]
    with pytest.raises(InvalidJsonDataset):
        validate_data(data)

def test_max_record_size_bytes():
    data = [{"a": 1}, {"b": 1234567891011}]
    assert max_record_size_bytes(data) == 69

def test_get_available_services_by_limit():
    assert get_available_services_by_limit(max_record_size_bytes=1000) == ['sqs', 'sns', 'firehose']
    assert get_available_services_by_limit(max_record_size_bytes=1000000) == ['firehose']


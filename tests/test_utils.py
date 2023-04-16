import sys
sys.path.append("../awsjsondataset")
import pytest
from awsjsondataset.exceptions import InvalidJsonDataset
from awsjsondataset.utils import (
    get_record_size_bytes,
    sort_records_by_size_bytes,
    validate_data
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
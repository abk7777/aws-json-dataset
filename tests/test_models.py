import sys
sys.path.append("../awsjsondataset")
import pytest
from pathlib import Path
from awsjsondataset.models import (
    JsonDataset,
    AwsJsonDataset
    )
from awsjsondataset.exceptions import InvalidJsonDataset

root_dir = Path(__file__).parent.parent
test_data_dir = root_dir / "tests" / "test_data"

def test_json_dataset_init():

    # test kwargs
    dataset = JsonDataset(data=[{"a": 1}, {"b": 2}])
    assert dataset.data == [{"a": 1}, {"b": 2}]
    assert dataset.path is None
    assert dataset.num_records == 2

    dataset = JsonDataset(path=test_data_dir / "subtechniques.json")
    assert dataset.data is not None
    assert dataset.path == test_data_dir / "subtechniques.json"
    assert dataset.num_records == 21

    # test args
    dataset = JsonDataset(data=[{"a": 1}, {"b": 2}])
    assert dataset.data == [{"a": 1}, {"b": 2}]
    assert dataset.path is None
    assert dataset.num_records == 2

    dataset = JsonDataset(path=test_data_dir / "subtechniques.json")
    assert dataset.data is not None
    assert dataset.path == test_data_dir / "subtechniques.json"
    assert dataset.num_records == 21

    # Test exceptions
    with pytest.raises(TypeError):
        JsonDataset(data=[{"a": 1}, {"b": 2}], path=test_data_dir / "subtechniques.json")

    with pytest.raises(TypeError):
        JsonDataset(data=[{"a": 1}, {"b": 2}], path=test_data_dir / "subtechniques.json")

    with pytest.raises(InvalidJsonDataset):
        JsonDataset(data=[{"a": 1}, {"b": 2}, 3])

    with pytest.raises(InvalidJsonDataset):
        JsonDataset(data=[{"a": 1}, {"b": 2}, 3])

def test_json_dataset_load():
    dataset = JsonDataset(path=test_data_dir / "subtechniques.json")
    assert dataset.data is not None
    assert dataset.path == test_data_dir / "subtechniques.json"
    assert dataset.num_records == 21

def test_json_dataset_save():
    dataset = JsonDataset(data=[{"a": 1}, {"b": 2}])
    dataset.save(test_data_dir / "test.json")
    assert test_data_dir / "test.json" in test_data_dir.iterdir()
    # clean up
    (test_data_dir / "test.json").unlink()

def test_json_dataset_records_by_size_kb():
    dataset = JsonDataset(data=[{"a": 1}, {"b": 2}])
    assert dataset._sort_records_by_size_kb == [({"a": 1}, 0.06), ({"b": 2}, 0.06)]

def test_json_dataset_max_record_size_kb():
    dataset = JsonDataset(data=[{"a": 1}, {"b": 2}])
    assert dataset._max_record_size_kb == 0.06

def test_aws_json_dataset_init():
    dataset = AwsJsonDataset(data=[{"a": 1}, {"b": 2}])
    assert dataset.data == [{"a": 1}, {"b": 2}]
    assert dataset.path is None
    assert dataset.num_records == 2

    dataset = AwsJsonDataset(path=test_data_dir / "subtechniques.json")
    assert dataset.data is not None
    assert dataset.path == test_data_dir / "subtechniques.json"
    assert dataset.num_records == 21

    # test args
    dataset = AwsJsonDataset(data=[{"a": 1}, {"b": 2}])
    assert dataset.data == [{"a": 1}, {"b": 2}]
    assert dataset.path is None
    assert dataset.num_records == 2

    dataset = AwsJsonDataset(path=test_data_dir / "subtechniques.json")
    assert dataset.data is not None
    assert dataset.path == test_data_dir / "subtechniques.json"
    assert dataset.num_records == 21

    # Test exceptions
    with pytest.raises(TypeError):
        AwsJsonDataset(data=[{"a": 1}, {"b": 2}], path=test_data_dir / "subtechniques.json")

    with pytest.raises(InvalidJsonDataset):
        AwsJsonDataset(data=[{"a": 1}, {"b": 2}, 3])

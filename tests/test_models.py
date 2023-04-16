import sys
sys.path.append("../awsjsondataset")
import pytest
from pathlib import Path
from awsjsondataset.models import (
    JsonDataset,
    AwsJsonDataset,
    SqsJsonDataset,
    )
from awsjsondataset.exceptions import InvalidJsonDataset, ServiceRecordSizeLimitExceeded
from tests.fixtures import *

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
    assert dataset._sort_records_by_size_bytes == [({"a": 1}, 57), ({"b": 2}, 57)]

def test_json_dataset_max_record_size_kb():
    dataset = JsonDataset(data=[{"a": 1}, {"b": 2}])
    assert dataset._max_record_size_bytes == 57

def test_aws_json_dataset_init():
    dataset = AwsJsonDataset(data=[{"a": 1}, {"b": 2}], service="sqs")
    assert dataset.data == [{"a": 1}, {"b": 2}]
    assert dataset.path is None
    assert dataset.num_records == 2

    dataset = AwsJsonDataset(path=test_data_dir / "subtechniques.json", service="sqs")
    assert dataset.data is not None
    assert dataset.path == test_data_dir / "subtechniques.json"
    assert dataset.num_records == 21

    # test args
    dataset = AwsJsonDataset(data=[{"a": 1}, {"b": 2}], service="sqs")
    assert dataset.data == [{"a": 1}, {"b": 2}]
    assert dataset.path is None
    assert dataset.num_records == 2

    dataset = AwsJsonDataset(path=test_data_dir / "subtechniques.json", service="sqs")
    assert dataset.data is not None
    assert dataset.path == test_data_dir / "subtechniques.json"
    assert dataset.num_records == 21

    # Test exceptions
    with pytest.raises(TypeError):
        AwsJsonDataset(data=[{"a": 1}, {"b": 2}], path=test_data_dir / "subtechniques.json", service="sqs")

    with pytest.raises(InvalidJsonDataset):
        AwsJsonDataset(data=[{"a": 1}, {"b": 2}, 3], service="sqs")

# test available services with record under max service size
def test_aws_json_dataset_available_services():

    # sqs within size limit
    dataset = AwsJsonDataset(data=[{"a": 1}, {"b": 2}], service="sqs")
    assert "sqs" in dataset._available_services

    # sqs over size limit
    with pytest.raises(ServiceRecordSizeLimitExceeded):
        dataset = AwsJsonDataset(data=[{"a": "value"*100000}], service="sqs")

    # sns within size limit
    dataset = AwsJsonDataset(data=[{"a": 1}, {"b": 2}], service="sns")
    assert "sns" in dataset._available_services

    # sns over size limit
    with pytest.raises(ServiceRecordSizeLimitExceeded):
        dataset = AwsJsonDataset(data=[{"a": "value"*100000}], service="sns")

    # kinesis within size limit
    dataset = AwsJsonDataset(data=[{"a": 1}, {"b": 2}], service="kinesis_firehose")
    assert "kinesis_firehose" in dataset._available_services

    # kinesis over size limit
    with pytest.raises(ServiceRecordSizeLimitExceeded):
        dataset = AwsJsonDataset(data=[{"a": "value"*1050000}], service="kinesis_firehose")

@mock_sts
def test_aws_json_dataset_account_id(sts):
    account_id = sts.get_caller_identity()["Account"]
    assert AwsJsonDataset(data=[{"a": 1}, {"b": 2}], service="sqs")._account_id == account_id

def test_aws_json_dataset_validate_service():
    # test valid service
    assert AwsJsonDataset(data=[{"a": 1}, {"b": 2}], service="sqs")

    # test invalid service
    with pytest.raises(ValueError):
        AwsJsonDataset(data=[{"a": 1}, {"b": 2}], service="invalid_service")

@mock_sqs
def test_sqs_json_dataset_init(sqs):

    # create a mock resource
    QUEUE_NAME = 'queue-url'
    sqs.create_queue(QueueName=QUEUE_NAME)
    res = sqs.get_queue_url(QueueName=QUEUE_NAME)
    queue_url = res['QueueUrl']

    dataset = SqsJsonDataset(data=[{"a": 1}, {"b": 2}], queue_url=queue_url)
    assert dataset.data == [{"a": 1}, {"b": 2}]
    assert dataset.path is None
    assert dataset.num_records == 2

    dataset = SqsJsonDataset(path=test_data_dir / "subtechniques.json", queue_url=queue_url)
    assert dataset.data is not None
    assert dataset.path == test_data_dir / "subtechniques.json"
    assert dataset.num_records == 21

    # test args
    dataset = SqsJsonDataset(data=[{"a": 1}, {"b": 2}], queue_url=queue_url)
    assert dataset.data == [{"a": 1}, {"b": 2}]
    assert dataset.path is None
    assert dataset.num_records == 2

    dataset = SqsJsonDataset(path=test_data_dir / "subtechniques.json", queue_url=queue_url)
    assert dataset.data is not None
    assert dataset.path == test_data_dir / "subtechniques.json"
    assert dataset.num_records == 21

    # Test exceptions
    with pytest.raises(TypeError):
        SqsJsonDataset(data=[{"a": 1}, {"b": 2}], path=test_data_dir / "subtechniques.json", queue_url=queue_url)

    with pytest.raises(InvalidJsonDataset):
        SqsJsonDataset(data=[{"a": 1}, {"b": 2}, 3], queue_url=queue_url)

@mock_sqs
def test_sqs_json_dataset_queue_records(sqs):

    # create a mock resource
    QUEUE_NAME = 'queue-url'
    sqs.create_queue(QueueName=QUEUE_NAME)
    res = sqs.get_queue_url(QueueName=QUEUE_NAME)
    queue_url = res['QueueUrl']

    dataset = SqsJsonDataset(data=[{"a": 1}, {"b": 2}], queue_url=queue_url)
    assert dataset.queue_records() is None
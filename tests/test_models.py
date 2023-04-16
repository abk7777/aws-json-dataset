import sys
sys.path.append("../awsjsondataset")
import pytest
from pathlib import Path
from awsjsondataset.models import (
    JsonDataset,
    BaseAwsJsonDataset,
    AwsJsonDataset
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

def test_base_aws_json_dataset_init():
    dataset = BaseAwsJsonDataset(data=[{"a": 1}, {"b": 2}])
    assert dataset.data == [{"a": 1}, {"b": 2}]
    assert dataset.path is None
    assert dataset.num_records == 2

    dataset = BaseAwsJsonDataset(path=test_data_dir / "subtechniques.json")
    assert dataset.data is not None
    assert dataset.path == test_data_dir / "subtechniques.json"
    assert dataset.num_records == 21

    # test args
    dataset = BaseAwsJsonDataset(data=[{"a": 1}, {"b": 2}])
    assert dataset.data == [{"a": 1}, {"b": 2}]
    assert dataset.path is None
    assert dataset.num_records == 2

    dataset = BaseAwsJsonDataset(path=test_data_dir / "subtechniques.json")
    assert dataset.data is not None
    assert dataset.path == test_data_dir / "subtechniques.json"
    assert dataset.num_records == 21

    # Test exceptions
    with pytest.raises(TypeError):
        BaseAwsJsonDataset(data=[{"a": 1}, {"b": 2}], path=test_data_dir / "subtechniques.json")

    with pytest.raises(InvalidJsonDataset):
        BaseAwsJsonDataset(data=[{"a": 1}, {"b": 2}, 3])

# test available services with record under max service size
def test_aws_json_dataset_available_services():

    # sqs within size limit
    dataset = BaseAwsJsonDataset(data=[{"a": 1}, {"b": 2}])
    assert "sqs" in dataset.available_services

    # # sqs over size limit
    # with pytest.raises(ServiceRecordSizeLimitExceeded):
    #     dataset = BaseAwsJsonDataset(data=[{"a": "value"*100000}])

    # sns within size limit
    dataset = BaseAwsJsonDataset(data=[{"a": 1}, {"b": 2}])
    assert "sns" in dataset.available_services

    # # sns over size limit
    # with pytest.raises(ServiceRecordSizeLimitExceeded):
    #     dataset = BaseAwsJsonDataset(data=[{"a": "value"*100000}])

    # kinesis within size limit
    dataset = BaseAwsJsonDataset(data=[{"a": 1}, {"b": 2}])
    assert "firehose" in dataset.available_services

    # # kinesis over size limit
    # with pytest.raises(ServiceRecordSizeLimitExceeded):
    #     dataset = BaseAwsJsonDataset(data=[{"a": "value"*1050000}])

def test_aws_json_dataset_validate_service():
    # test valid service
    assert BaseAwsJsonDataset(data=[{"a": 1}, {"b": 2}])

    # # test invalid service
    # with pytest.raises(ValueError):
    #     BaseAwsJsonDataset(data=[{"a": 1}, {"b": 2}])

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

def test_aws_json_dataset_attrs():

    dataset = AwsJsonDataset(data=[{"a": 1}, {"b": 2}])
    assert "sqs" in dataset.available_services
    assert "sns" in dataset.available_services
    assert "firehose" in dataset.available_services

    # assert that dataset has attributes for each service
    for item in dataset.available_services:
        assert hasattr(dataset, item)

    # TODO test that service is not in available services if records is too large

@mock_sqs
def test_aws_json_dataset_sqs_send_messages(sqs):

    # create queue
    queue_url = sqs.create_queue(QueueName="test_queue")["QueueUrl"]

    dataset = AwsJsonDataset(data=[{"a": 1}, {"b": 2}])
    assert dataset.sqs(queue_url).send_messages() is None

@mock_sns
def test_aws_json_dataset_sns_publish(sns):
    
    # create topic
    topic_arn = sns.create_topic(Name="test_topic")["TopicArn"]

    # small batch size
    with pytest.raises(Exception):
        dataset = AwsJsonDataset(data=[{"a": 1}, {"b": 2}])
        assert dataset.sns(topic_arn).publish_messages() is None

    # large batch size
    dataset = AwsJsonDataset(data=[{"a": 1}, {"b": 2}]*100)
    assert dataset.sns(topic_arn).publish_messages() is None

@mock_s3
@mock_firehose
def test_aws_json_dataset_kinesis_firehose_put_records(s3, firehose):
    
    # create a mock bucket
    DATA_BUCKET_NAME = 'data-bucket'
    response = s3.create_bucket(Bucket=DATA_BUCKET_NAME)

    # create a mock delivery stream
    DELIVERY_STREAM_NAME = "data-delivery-stream"
    response = firehose.create_delivery_stream(
        DeliveryStreamName=DELIVERY_STREAM_NAME,
        ExtendedS3DestinationConfiguration={
            'RoleARN': 'arn:aws:iam::123456789012:role/firehose_delivery_role',
            'BucketARN': f'arn:aws:s3:::{DATA_BUCKET_NAME}'
        })

    # small batch size
    with pytest.raises(Exception):
        dataset = AwsJsonDataset(data=[{"a": 1}, {"b": 2}])
        assert dataset.firehose(DELIVERY_STREAM_NAME).put_records() is None

    # large batch size
    dataset = AwsJsonDataset(data=[{"a": 1}, {"b": 2}]*100)
    assert dataset.firehose(DELIVERY_STREAM_NAME).put_records() is None

    # TODO test that service is not in available services if records is too large
    # # kinesis over size limit
    # with pytest.raises(ServiceRecordSizeLimitExceeded):
    #     dataset = AwsJsonDataset(data=[{"a": "value"*1050000}])
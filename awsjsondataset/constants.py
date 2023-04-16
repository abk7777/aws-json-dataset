service_size_limits_bytes = {
    "sqs": {
        "max_batch_size": 10,
        "max_batch_size_bytes": 262144,
        "max_record_size_bytes": 262144
    },
    "sns": {
        "max_batch_size": 10,
        "max_batch_size_bytes": 262144,
        "max_record_size_bytes": 262144
    },
    "firehose": {
        "max_batch_size": 500,
        "max_batch_size_bytes": 1048576*5,
        "max_record_size_bytes": 1048576
    },
}

available_services = list(service_size_limits_bytes.keys())
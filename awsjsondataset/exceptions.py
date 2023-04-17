import logging
from .types import JSONDataset
from .constants import service_size_limits_bytes

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class InvalidJsonDataset(ValueError):
    """Raised when an invalid dataset type is passed"""
    def __str__(self):
        return 'JSON must contain array as top-level element'

class MissingRecords(Exception):
    """Raised when there is inequality between the expected number of records and actual"""

    def __init__(self, expected: int, actual: int) -> None:
        self.expected: str = expected
        self.actual: str = actual

    def __str__(self):
        return f'Expected {self.expected} records to be processed but got {self.actual}'

class ServiceRecordSizeLimitExceeded(Exception):
    """Raised when a record size exceeds the service limit"""

    def __init__(self, service: str, max_record_size_bytes: float) -> None:
        self.service: str = service
        self.max_record_size_bytes: float = max_record_size_bytes
        self.service_limit_bytes: int = service_size_limits_bytes[self.service]['max_record_size_bytes']

    def __str__(self):
        return f'Max record size of {self.max_record_size_bytes} bytes exceeds the {self.service} limit of {self.service_limit_bytes} bytes'
    pass
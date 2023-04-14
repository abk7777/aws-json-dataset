import logging

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

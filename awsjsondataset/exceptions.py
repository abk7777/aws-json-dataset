import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class InvalidJsonDataset(ValueError):
    """Raised when an invalid dataset type is passed"""

    def __str__(self):
        return 'JSON must contain array as top-level element'

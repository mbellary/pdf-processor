import logging
import sys
import json
from logging import Logger

def get_logger(name: str = "sqs_worker") -> Logger:
    # Simple structured JSON logger. Replace with structlog if you prefer.
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        '{"timestamp":"%(asctime)s","level":"%(levelname)s","name":"%(name)s","msg":"%(message)s"}'
    )
    handler.setFormatter(formatter)
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger

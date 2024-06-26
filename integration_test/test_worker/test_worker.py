import logging
import os
import time

from omotes_sdk import LogLevel, setup_logging
from omotes_sdk.internal.worker.worker import initialize_worker, UpdateProgressHandler

setup_logging(LogLevel.parse(os.environ.get("LOG_LEVEL", "INFO")), "test_worker")
setup_logging(LogLevel.parse(os.environ.get("LOG_LEVEL", "INFO")), "celery")
setup_logging(LogLevel.parse(os.environ.get("LOG_LEVEL", "INFO")), "amqp")

logger = logging.getLogger("test_worker")


def test_worker_task(
    input_esdl: str, params_dict: dict, update_progress_handler: UpdateProgressHandler
) -> str:
    update_progress_handler(0.3, "Before log")
    logger.info("Test worker task running.")
    time.sleep(0.2)
    update_progress_handler(0.7, "After log")
    return "Hello from test worker!"


if __name__ == "__main__":
    initialize_worker("test_worker", test_worker_task)

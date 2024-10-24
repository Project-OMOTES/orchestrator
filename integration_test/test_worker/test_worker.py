import logging
import os
import time

from omotes_sdk import LogLevel, setup_logging
from omotes_sdk.internal.worker.worker import initialize_worker, UpdateProgressHandler

setup_logging(LogLevel.parse(os.environ.get("LOG_LEVEL", "INFO")), "test_worker")
setup_logging(LogLevel.parse(os.environ.get("LOG_LEVEL", "INFO")), "celery")
setup_logging(LogLevel.parse(os.environ.get("LOG_LEVEL", "INFO")), "amqp")

logger = logging.getLogger("test_worker")

WORKER_TYPE = os.environ["WORKER_TYPE"]


def worker_type_to_task_type(worker_type: str):
    if worker_type == "NO_FAULT":
        task_type = "test_worker"
    elif worker_type == "HARD_CRASH":
        task_type = "test_worker_hard_crash"
    elif worker_type == "LONG_SLEEP":
        task_type = "test_worker_long_sleep"
    else:
        raise RuntimeError(f"Unknown worker type {worker_type}")

    return task_type


def test_worker_task(
    input_esdl: str, params_dict: dict, update_progress_handler: UpdateProgressHandler
) -> str:
    update_progress_handler(0.3, f"Before log {WORKER_TYPE}")
    logger.info("Test worker task running %s.", WORKER_TYPE)
    if WORKER_TYPE == "NO_FAULT":
        time.sleep(0.2)
    elif WORKER_TYPE == "HARD_CRASH":
        _ = " " * 150 * 1024**2  # Allocate 150mb of memory to trigger OOM
        time.sleep(100)  # Wait for OOM to trigger
    elif WORKER_TYPE == "LONG_SLEEP":
        time.sleep(100)
    update_progress_handler(0.7, f"After log {WORKER_TYPE}")
    return input_esdl


if __name__ == "__main__":
    initialize_worker(worker_type_to_task_type(WORKER_TYPE), test_worker_task)

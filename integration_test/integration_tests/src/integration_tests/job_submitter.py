import logging
import multiprocessing
import os
import threading
import time
import traceback
import uuid

from dotenv import load_dotenv
from omotes_sdk import setup_logging, LogLevel
from omotes_sdk.config import RabbitMQConfig
from omotes_sdk.omotes_interface import (
    OmotesInterface,
    Job,
    JobResult,
    JobProgressUpdate,
    JobStatusUpdate,
)

load_dotenv(verbose=True)

setup_logging(LogLevel.parse(os.environ.get("LOG_LEVEL", "INFO")), "integration_test_job_submitter")

LOG = logging.getLogger("integration_test_job_submitter")

rabbitmq_config = RabbitMQConfig(
    username=os.environ["RABBITMQ_OMOTES_USER_NAME"],
    password=os.environ["RABBITMQ_OMOTES_USER_PASSWORD"],
    virtual_host=os.environ["RABBITMQ_VIRTUALHOST"],
    host=os.environ["RABBITMQ_HOST"],
    port=int(os.environ["RABBITMQ_PORT"]),
)


JOB_COUNT_PER_PROCESS = 100
PROCESS_COUNT = 5
TIMEOUT_IN_WHICH_ALL_JOBS_MUST_FINISH_PER_PROCESS_SECONDS = 60.0


class JobSubmitter:
    process_number: int
    active_jobs: dict[uuid.UUID, Job]
    _result_jobs_lock: threading.Lock
    result_jobs: dict[uuid.UUID, JobResult]

    done: threading.Event
    errors = list[str]

    def __init__(self, process_number: int):
        self.process_number = process_number
        self.active_jobs = {}
        self._result_jobs_lock = threading.Lock()
        self.result_jobs = {}
        self.done = threading.Event()
        self.errors = []

    def handle_on_finished(self, job: Job, result: JobResult):
        if job.id in self.result_jobs:
            error = (
                f"This is weird. Received a result for job {job.id} but already received it previously.\n"
                f"Result received now:\n{result}\n"
                f"Result received before:\n{self.result_jobs[job.id]}\n"
            )
            self.errors.append(error)
        else:
            with self._result_jobs_lock:
                self.result_jobs[job.id] = result
            LOG.info(f"Received result for {job.id}")

            if job.id not in self.active_jobs:
                error = (
                    "Received a result for a job that was never submitted!\n"
                    f"Job: {job}\n"
                    f"Result: {result}\n"
                )
                self.errors.append(error)

            all_counted = False
            if JOB_COUNT_PER_PROCESS == len(self.result_jobs):
                LOG.debug("Received the expected amount of result!")
                all_counted = True

            all_received = False
            if list(self.active_jobs.keys()).sort() == list(self.result_jobs.keys()).sort():
                LOG.debug("All active jobs have received at least 1 result.")
                all_received = True

            if all_received and all_counted:
                LOG.info("Apparently I am done now!")
                self.done.set()

    def handle_on_status_update(self, job: Job, status_update: JobStatusUpdate):
        pass
        # print(
        #     f"Job {job.id} progress (type: {job.workflow_type.workflow_type_name}). "
        #     f"Status: {status_update.status}"
        # )

    def handle_on_progress_update(self, job: Job, progress_update: JobProgressUpdate):
        pass
        # print(
        #     f"Job {job.id} progress (type: {job.workflow_type.workflow_type_name}). Progress:"
        #     f" {progress_update.progress}, message: {progress_update.message}"
        # )

    def run(self):
        omotes_if = None
        try:
            omotes_if = OmotesInterface(
                rabbitmq_config, f"integration_test_job_submitter.{self.process_number}"
            )
            omotes_if.start()

            for i in range(0, JOB_COUNT_PER_PROCESS):
                job_ref = omotes_if.submit_job(
                    esdl="input-esdl-value",
                    params_dict={
                        "key1": "value1",
                        "key2": ["just", "a", "list", "with", "an", "integer", 3],
                    },
                    workflow_type=omotes_if.get_workflow_type_manager().get_workflow_by_name(
                        "test_worker"
                    ),
                    job_timeout=None,
                    callback_on_finished=self.handle_on_finished,
                    callback_on_progress_update=self.handle_on_progress_update,
                    callback_on_status_update=self.handle_on_status_update,
                    auto_disconnect_on_result=True,
                )
                self.active_jobs[job_ref.id] = job_ref

            not_timeout = self.done.wait(TIMEOUT_IN_WHICH_ALL_JOBS_MUST_FINISH_PER_PROCESS_SECONDS)

            if not not_timeout:
                self.errors.append(
                    "Timeout occurred while waiting on test to be done. Please make "
                    "implementation faster or increase timeout."
                )

            for job_id in self.active_jobs.keys():
                if job_id not in self.result_jobs:
                    self.errors.append(f"Did not receive a result for job {job_id}")
        except Exception as e:
            self.errors.append(f"An exception happened: {e}")
            traceback.print_exception(e)
        finally:
            time.sleep(1)
            if omotes_if:
                omotes_if.stop()


def main_process(process_number: int) -> list[str]:
    submitter = JobSubmitter(process_number)
    submitter.run()

    return submitter.errors


def run_high_throughput_test():
    print("Starting with job submissions for integration test.")
    with multiprocessing.Pool(PROCESS_COUNT) as p:
        all_errors: list[list[str]] = p.map(main_process, range(PROCESS_COUNT))

    for i, errors in enumerate(all_errors):
        if errors:
            print(f"Process {i} had errors:")
            for error in errors:
                print(error)
            print()
        else:
            print(f"Process {i} had no errors")
            print()

    if any(any(x) for x in all_errors):
        raise RuntimeError("Error(s) was found.")

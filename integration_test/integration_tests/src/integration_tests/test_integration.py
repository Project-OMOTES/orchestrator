import os
import threading
import unittest
from datetime import timedelta

from omotes_sdk.config import RabbitMQConfig
from omotes_sdk.job import Job
from omotes_sdk.omotes_interface import OmotesInterface
from omotes_sdk.types import ParamsDict
from omotes_sdk_protocol.job_pb2 import JobProgressUpdate, JobStatusUpdate, JobResult
import psycopg

from integration_tests.job_submitter import run_high_throughput_test


RABBITMQ_CONFIG = RabbitMQConfig(
    username=os.environ["RABBITMQ_OMOTES_USER_NAME"],
    password=os.environ["RABBITMQ_OMOTES_USER_PASSWORD"],
    virtual_host=os.environ["RABBITMQ_VIRTUALHOST"],
    host=os.environ["RABBITMQ_HOST"],
    port=int(os.environ["RABBITMQ_PORT"]),
)

SQL_CONFIG = {
    "host": os.environ["POSTGRESQL_HOST"],
    "port": os.environ["POSTGRESQL_PORT"],
    "database": os.environ["POSTGRESQL_DATABASE"],
    "username": os.environ["POSTGRESQL_USERNAME"],
    "password": os.environ["POSTGRESQL_PASSWORD"],
}


class omotes_client:
    omotes_if: OmotesInterface

    def __enter__(self) -> OmotesInterface:
        self.omotes_if = OmotesInterface(RABBITMQ_CONFIG, "integration_test_orchestrator")
        self.omotes_if.start()
        return self.omotes_if

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.omotes_if.stop()


def assert_orchestrator_tables_are_empty() -> None:
    conn_str = f"postgresql://{SQL_CONFIG['username']}:{SQL_CONFIG['password']}@{SQL_CONFIG['host']}:{SQL_CONFIG['port']}/{SQL_CONFIG['database']}"
    # Connect to an existing database
    with psycopg.connect(conninfo=conn_str) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM job")
            count = cur.fetchall()

            assert count[0][0] == 0, "The job table is not empty."

            cur.execute("SELECT count(*) FROM job_starts")
            count = cur.fetchall()

            assert count[0][0] == 0, "The job_starts table is not empty."


class OmotesJobHandler:
    progress_updates: list[JobProgressUpdate]
    status_updates: list[JobStatusUpdate]
    has_result: threading.Event
    received_status_update: dict[JobStatusUpdate.JobStatus, threading.Event]
    result: JobResult | None

    def __init__(self):
        self.progress_updates = []
        self.status_updates = []
        self.has_result = threading.Event()
        self.received_status_update = {
            status: threading.Event() for status in JobStatusUpdate.JobStatus.values()
        }
        self.result = None

    def wait_until_result(self, timeout: float | None = None):
        self.has_result.wait(timeout)

    def wait_on_status(self, status: JobStatusUpdate.JobStatus, timeout: float | None = None):
        self.received_status_update[status].wait(timeout)

    def handle_on_finished(self, _: Job, result: JobResult):
        self.has_result.set()
        self.result = result

    def handle_on_status_update(self, _: Job, status_update: JobStatusUpdate):
        self.status_updates.append(status_update)
        self.received_status_update[status_update.status].set()

    def handle_on_progress_update(self, _: Job, progress_update: JobProgressUpdate):
        self.progress_updates.append(progress_update)


def submit_a_job(
    omotes_client: OmotesInterface,
    esdl_file: str,
    workflow_type: str,
    params_dict: ParamsDict,
    omotes_job_result_handler: OmotesJobHandler,
) -> Job:
    _workflow_type = omotes_client.get_workflow_type_manager().get_workflow_by_name(workflow_type)
    if not _workflow_type:
        raise RuntimeError(
            f"Unknown workflow type {workflow_type}. Available workflows: {[workflow.workflow_type_name for workflow in omotes_client.get_workflow_type_manager().get_all_workflows()]}"
        )

    return omotes_client.submit_job(
        esdl=esdl_file,
        workflow_type=_workflow_type,
        job_timeout=timedelta(hours=1),
        params_dict=params_dict,
        callback_on_finished=omotes_job_result_handler.handle_on_finished,
        callback_on_progress_update=omotes_job_result_handler.handle_on_progress_update,
        callback_on_status_update=omotes_job_result_handler.handle_on_status_update,
        auto_disconnect_on_result=True,
    )


EMPTY_ESDL = """<?xml version='1.0' encoding='UTF-8'?>
<esdl:EnergySystem xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:esdl="http://www.tno.nl/esdl" name="Untitled EnergySystem" description="" id="24e28056-4889-428b-a2d3-065185dbb123" esdlVersion="v2401" version="1">
  <instance xsi:type="esdl:Instance" id="86ebbf1c-809e-43b3-9332-5e40a39e2a84" name="Untitled Instance">
    <area xsi:type="esdl:Area" id="eef34e0d-6df6-4c0d-9038-d924ed9caa12" name="Untitled Area"/>
  </instance>
</esdl:EnergySystem>

"""


class TestIntegration(unittest.TestCase):
    def test__hard_crash(self):
        # Arrange
        job_handler = OmotesJobHandler()
        workflow_type = "test_worker_hard_crash"

        # Act
        with omotes_client() as omotes_if:
            submit_a_job(
                omotes_if,
                EMPTY_ESDL,
                workflow_type,
                params_dict={},
                omotes_job_result_handler=job_handler,
            )

            job_handler.wait_until_result(30.0)

        # Assert
        self.assertEqual(job_handler.result.result_type, JobResult.ResultType.ERROR)
        self.assertEqual(
            job_handler.result.logs,
            "Job cannot be processed due to being retried the maximum number of times.",
        )
        assert_orchestrator_tables_are_empty()

    def test__delete(self):
        # Arrange
        job_handler = OmotesJobHandler()
        workflow_type = "test_worker_long_sleep"

        # Act
        with omotes_client() as omotes_if:
            job = submit_a_job(
                omotes_if,
                EMPTY_ESDL,
                workflow_type,
                params_dict={},
                omotes_job_result_handler=job_handler,
            )

            job_handler.wait_on_status(JobStatusUpdate.RUNNING, 1.0)
            self.assertIn(
                JobStatusUpdate.RUNNING, [update.status for update in job_handler.status_updates]
            )
            omotes_if.delete_job(job)
            job_handler.wait_until_result(30.0)

        # Assert
        self.assertEqual(job_handler.result.result_type, JobResult.ResultType.CANCELLED)
        assert_orchestrator_tables_are_empty()

    def test__high_throughput(self):
        # Act
        run_high_throughput_test()

        # Assert
        assert_orchestrator_tables_are_empty()

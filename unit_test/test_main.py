import datetime
import threading
import time
import unittest
import uuid
from datetime import timedelta
from multiprocessing.pool import ThreadPool, MapResult
from typing import cast
from unittest.mock import patch, Mock
from uuid import UUID

from omotes_sdk.job import Job
from omotes_sdk.workflow_type import WorkflowType
from omotes_sdk_protocol.internal.task_pb2 import TaskProgressUpdate
from omotes_sdk_protocol.job_pb2 import JobSubmission, JobProgressUpdate, JobStatusUpdate, JobResult
from google.protobuf import json_format
from google.protobuf.struct_pb2 import Struct

from omotes_orchestrator.config import OrchestratorConfig
from omotes_orchestrator.db_models.job import JobStatus as JobStatusDB, JobDB
from omotes_orchestrator.main import (
    LifeCycleBarrierManager,
    BarrierTimeoutException,
    MissingBarrierException,
    Orchestrator,
)


class LifeCycleBarrierManagerTest(unittest.TestCase):
    def test__ensure_barrier__barrier_is_created(self) -> None:
        # Arrange
        barrier_manager = LifeCycleBarrierManager()
        job_id = UUID("ab995599-a117-47b6-8da5-5d9488900858")

        # Act
        barrier = barrier_manager.ensure_barrier(job_id)

        # Assert
        self.assertEqual(barrier_manager._barriers[job_id], barrier)

    def test__ensure_barrier__barrier_is_already_ensured(self) -> None:
        # Arrange
        barrier_manager = LifeCycleBarrierManager()
        job_id = UUID("ab995599-a117-47b6-8da5-5d9488900858")
        barrier_1 = barrier_manager.ensure_barrier(job_id)

        # Act
        with patch("threading.Event") as event_mock:
            barrier_2 = barrier_manager.ensure_barrier(job_id)

        # Assert
        self.assertIs(barrier_1, barrier_2)
        event_mock.assert_not_called()

    def test__ensure_barrier__barrier_is_ensured_by_many_threads(self) -> None:
        # Arrange
        barrier_manager = LifeCycleBarrierManager()
        job_id = UUID("ab995599-a117-47b6-8da5-5d9488900858")

        num_of_threads = 100
        start_barrier = threading.Barrier(num_of_threads)

        def test_job(i: int) -> threading.Event:
            print(f"{i} is waiting for start barrier.")
            print(f"{start_barrier.n_waiting + 1}/{start_barrier.parties}")
            start_barrier.wait()
            print(f"{i} has passed start barrier.")
            return barrier_manager.ensure_barrier(job_id)

        # Act
        with ThreadPool(num_of_threads) as pool:
            async_results: MapResult[threading.Event] = pool.map_async(
                test_job, range(0, num_of_threads), error_callback=print
            )
            barriers = async_results.get(3.0)

        # Assert
        self.assertEqual(len(barriers), num_of_threads)
        self.assertIsInstance(barriers[0], threading.Event)
        for i in range(1, num_of_threads):
            self.assertIs(barriers[0], barriers[i])

    def test__set_barrier__first_set_the_barrier_then_wait(self) -> None:
        # Arrange
        barrier_manager = LifeCycleBarrierManager()
        job_id = UUID("ab995599-a117-47b6-8da5-5d9488900858")
        barrier_manager.ensure_barrier(job_id)

        # Act / Assert
        barrier_manager.set_barrier(job_id)
        barrier_manager.wait_for_barrier(job_id)

    def test__set_barrier__wait_and_set_by_different_threads(self) -> None:
        # Arrange
        barrier_manager = LifeCycleBarrierManager()
        job_id = UUID("ab995599-a117-47b6-8da5-5d9488900858")
        barrier_manager.ensure_barrier(job_id)

        def set_barrier() -> None:
            time.sleep(0.5)
            barrier_manager.set_barrier(job_id)

        other_thread = threading.Thread(target=set_barrier)

        # Act / Assert
        other_thread.start()
        barrier_manager.wait_for_barrier(job_id)

    def test__wait_barrier__takes_too_long(self) -> None:
        # Arrange
        barrier_manager = LifeCycleBarrierManager()
        job_id = UUID("ab995599-a117-47b6-8da5-5d9488900858")
        barrier_manager.ensure_barrier(job_id)

        # Overwrite the timeout temporarily so the test doesn't take too long.
        previous_timeout = LifeCycleBarrierManager.BARRIER_WAIT_TIMEOUT
        LifeCycleBarrierManager.BARRIER_WAIT_TIMEOUT = 0.1

        # Act / Assert
        with self.assertRaises(BarrierTimeoutException):
            barrier_manager.wait_for_barrier(job_id)

        # Cleanup
        LifeCycleBarrierManager.BARRIER_WAIT_TIMEOUT = previous_timeout

    def test__wait_barrier__barrier_is_not_created(self) -> None:
        # Arrange
        barrier_manager = LifeCycleBarrierManager()
        job_id = UUID("ab995599-a117-47b6-8da5-5d9488900858")

        # Act / Assert
        with self.assertRaises(MissingBarrierException):
            barrier_manager.wait_for_barrier(job_id)

    def test__set_barrier__barrier_is_not_created(self) -> None:
        # Arrange
        barrier_manager = LifeCycleBarrierManager()
        job_id = UUID("ab995599-a117-47b6-8da5-5d9488900858")

        # Act / Assert
        with self.assertRaises(MissingBarrierException):
            barrier_manager.set_barrier(job_id)

    def test__cleanup_barrier__cleaning_non_existing_barrier(self) -> None:
        # Arrange
        barrier_manager = LifeCycleBarrierManager()
        job_id = UUID("ab995599-a117-47b6-8da5-5d9488900858")

        # Act / Assert
        barrier_manager.cleanup_barrier(job_id)

    def test__cleanup_barrier__cleaning_barrier(self) -> None:
        # Arrange
        barrier_manager = LifeCycleBarrierManager()
        job_id = UUID("ab995599-a117-47b6-8da5-5d9488900858")

        barrier_manager.ensure_barrier(job_id)
        self.assertIn(job_id, barrier_manager._barriers)

        # Act
        barrier_manager.cleanup_barrier(job_id)

        # Assert
        self.assertNotIn(job_id, barrier_manager._barriers)


class OrchestratorTest(unittest.TestCase):
    class MockedOrchestrator:
        def __init__(self) -> None:
            self.omotes_orchestrator_sdk_if = Mock()
            self.worker_if = Mock()
            self.celery_if = Mock()
            self.celery_if.start_workflow.return_value = "celery_id"
            self.postgresql_if = Mock()
            self.time_series_db_if = Mock()

            self.workflow_manager = Mock()
            self.postgres_job_manager = Mock()
            self.timeout_job_manager = Mock()
            self.esdl_time_series_manager = Mock()

            with patch(
                "omotes_orchestrator.main.LifeCycleBarrierManager"
            ) as life_cycle_barrier_manager_class_mock:
                self.orchestrator = Orchestrator(
                    config=OrchestratorConfig(),
                    omotes_orchestrator_sdk_if=self.omotes_orchestrator_sdk_if,
                    worker_if=self.worker_if,
                    celery_if=self.celery_if,
                    postgresql_if=self.postgresql_if,
                    time_series_db_if=self.time_series_db_if,
                    workflow_manager=self.workflow_manager,
                    postgres_job_manager=self.postgres_job_manager,
                    timeout_job_manager=self.timeout_job_manager,
                    esdl_time_series_manager=self.esdl_time_series_manager,
                )

            self.life_cycle_barrier_manager_obj_mock = (
                life_cycle_barrier_manager_class_mock.return_value
            )

    def test__new_job_submitted_handler__fully_new_job(self) -> None:
        # Arrange
        mocked_orchestrator = OrchestratorTest.MockedOrchestrator()
        orchestrator = cast(unittest.mock.Mock, mocked_orchestrator.orchestrator)
        celery_if = mocked_orchestrator.celery_if
        postgresql_if = mocked_orchestrator.postgresql_if
        life_cycle_barrier_manager_obj_mock = (
            mocked_orchestrator.life_cycle_barrier_manager_obj_mock
        )

        postgresql_if.job_exists.return_value = False

        job_id = uuid.uuid4()
        timeout = 3000
        workflow_type = WorkflowType("some-workflow", "some-descr")
        esdl = "Some-esdl"
        params_dict = Struct()
        job_submission = JobSubmission(
            uuid=str(job_id),
            job_reference="job_ref",
            timeout_ms=timeout,
            workflow_type=workflow_type.workflow_type_name,
            esdl=esdl,
            params_dict=params_dict,
            job_priority=JobSubmission.JobPriority.MEDIUM,
        )
        job = Job(id=job_id, workflow_type=workflow_type)
        orchestrator.workflow_manager.get_workflow_by_name.return_value = workflow_type

        # Act
        orchestrator.new_job_submitted_handler(job_submission)

        # Assert
        expected_params_dict = json_format.MessageToDict(job_submission.params_dict)
        expected_celery_id = "celery_id"
        expected_timeout = timedelta(milliseconds=timeout)
        expected_job_priority = JobSubmission.JobPriority.MEDIUM

        life_cycle_barrier_manager_obj_mock.ensure_barrier.assert_called_once_with(job_id)
        life_cycle_barrier_manager_obj_mock.set_barrier.assert_called_once_with(job_id)
        celery_if.start_workflow.assert_called_once_with(
            job.workflow_type,
            job.id,
            job_submission.job_reference,
            job_submission.esdl,
            expected_params_dict,
            expected_job_priority,
        )
        postgresql_if.job_exists.assert_called_once_with(job_id)
        postgresql_if.get_job_status.assert_not_called()
        postgresql_if.set_job_submitted.called_called_once_with(job_id, expected_celery_id)
        postgresql_if.put_new_job.assert_called_once_with(
            job_id=job.id,
            job_reference=job_submission.job_reference,
            workflow_type=job_submission.workflow_type,
            timeout_after=expected_timeout,
        )

    def test__new_job_submitted_handler__already_registered_but_not_submitted_new_job(self) -> None:
        # Arrange
        mocked_orchestrator = OrchestratorTest.MockedOrchestrator()
        orchestrator = cast(unittest.mock.Mock, mocked_orchestrator.orchestrator)
        celery_if = mocked_orchestrator.celery_if
        postgresql_if = mocked_orchestrator.postgresql_if
        life_cycle_barrier_manager_obj_mock = (
            mocked_orchestrator.life_cycle_barrier_manager_obj_mock
        )

        postgresql_if.get_job_status.return_value = JobStatusDB.REGISTERED
        postgresql_if.job_exists.return_value = True

        job_id = uuid.uuid4()
        timeout = 3000
        workflow_type = WorkflowType("some-workflow", "some-descr")
        esdl = "Some-esdl"
        params_dict = Struct()
        job_submission = JobSubmission(
            uuid=str(job_id),
            timeout_ms=timeout,
            workflow_type=workflow_type.workflow_type_name,
            esdl=esdl,
            params_dict=params_dict,
            job_priority=JobSubmission.JobPriority.MEDIUM,
        )
        job = Job(id=job_id, workflow_type=workflow_type)
        orchestrator.workflow_manager.get_workflow_by_name.return_value = workflow_type

        # Act
        orchestrator.new_job_submitted_handler(job_submission)

        # Assert
        expected_params_dict = json_format.MessageToDict(job_submission.params_dict)
        expected_celery_id = "celery_id"
        expected_job_priority = JobSubmission.JobPriority.MEDIUM

        life_cycle_barrier_manager_obj_mock.ensure_barrier.assert_called_once_with(job_id)
        life_cycle_barrier_manager_obj_mock.set_barrier.assert_called_once_with(job_id)
        celery_if.start_workflow.assert_called_once_with(
            job.workflow_type,
            job.id,
            None,
            job_submission.esdl,
            expected_params_dict,
            expected_job_priority,
        )
        postgresql_if.job_exists.assert_called_once_with(job_id)
        postgresql_if.get_job_status.assert_called_once_with(job_id)
        postgresql_if.set_job_submitted.called_called_once_with(job_id, expected_celery_id)
        postgresql_if.put_new_job.assert_not_called()

    def test__new_job_submitted_handler__already_registered_and_submitted_new_job(self) -> None:
        # Arrange
        mocked_orchestrator = OrchestratorTest.MockedOrchestrator()
        orchestrator = mocked_orchestrator.orchestrator
        celery_if = mocked_orchestrator.celery_if
        postgresql_if = mocked_orchestrator.postgresql_if
        life_cycle_barrier_manager_obj_mock = (
            mocked_orchestrator.life_cycle_barrier_manager_obj_mock
        )
        postgresql_if.get_job_status.return_value = JobStatusDB.SUBMITTED
        postgresql_if.job_exists.return_value = True

        job_id = uuid.uuid4()
        timeout = 3000
        workflow_type = "some-workflow"
        esdl = "Some-esdl"
        params_dict = Struct()
        job_submission = JobSubmission(
            uuid=str(job_id),
            timeout_ms=timeout,
            workflow_type=workflow_type,
            esdl=esdl,
            params_dict=params_dict,
        )

        # Act
        orchestrator.new_job_submitted_handler(job_submission)

        # Assert
        life_cycle_barrier_manager_obj_mock.ensure_barrier.assert_not_called()
        life_cycle_barrier_manager_obj_mock.set_barrier.assert_not_called()
        celery_if.start_workflow.assert_not_called()
        postgresql_if.job_exists.assert_called_once_with(job_id)
        postgresql_if.get_job_status.assert_called_once_with(job_id)
        postgresql_if.set_job_submitted.assert_not_called()
        postgresql_if.put_new_job.assert_not_called()

    def test__new_job_submitted_handler__already_running_new_job(self) -> None:
        # Arrange
        mocked_orchestrator = OrchestratorTest.MockedOrchestrator()
        orchestrator = mocked_orchestrator.orchestrator
        celery_if = mocked_orchestrator.celery_if
        postgresql_if = mocked_orchestrator.postgresql_if
        life_cycle_barrier_manager_obj_mock = (
            mocked_orchestrator.life_cycle_barrier_manager_obj_mock
        )
        postgresql_if.get_job_status.return_value = JobStatusDB.RUNNING
        postgresql_if.job_exists.return_value = True

        job_id = uuid.uuid4()
        timeout = 3000
        workflow_type = "some-workflow"
        esdl = "Some-esdl"
        params_dict = Struct()
        job_submission = JobSubmission(
            uuid=str(job_id),
            timeout_ms=timeout,
            workflow_type=workflow_type,
            esdl=esdl,
            params_dict=params_dict,
        )

        # Act
        orchestrator.new_job_submitted_handler(job_submission)

        # Assert
        life_cycle_barrier_manager_obj_mock.ensure_barrier.assert_not_called()
        life_cycle_barrier_manager_obj_mock.set_barrier.assert_not_called()
        celery_if.start_workflow.assert_not_called()
        postgresql_if.job_exists.assert_called_once_with(job_id)
        postgresql_if.get_job_status.assert_called_once_with(job_id)
        postgresql_if.set_job_submitted.assert_not_called()
        postgresql_if.put_new_job.assert_not_called()

    def test__task_progress_update__START_progress_update(self) -> None:
        # Arrange
        job_id = uuid.uuid4()
        celery_task_id = uuid.uuid4()
        workflow_type_name = "some-workflow-type"
        workflow_type = WorkflowType(
            workflow_type_name=workflow_type_name, workflow_type_description_name="description"
        )
        job = Job(id=job_id, workflow_type=workflow_type)

        job_db = JobDB(
            job_id=job_id,
            celery_id=str(celery_task_id),
            workflow_type=workflow_type_name,
            status=JobStatusDB.SUBMITTED,
            registered_at=datetime.datetime.fromisoformat("2024-08-29T14:00:00"),
            submitted_at=datetime.datetime.fromisoformat("2024-08-29T14:00:01"),
            timeout_after_ms=10_000,
        )
        task_progress_update = TaskProgressUpdate(
            job_id=str(job_id),
            celery_task_id=str(celery_task_id),
            celery_task_type=workflow_type_name,
            status=TaskProgressUpdate.START,
            message="Task has started. Some message here.",
        )

        mocked_orchestrator = OrchestratorTest.MockedOrchestrator()
        orchestrator = mocked_orchestrator.orchestrator

        workflow_manager = mocked_orchestrator.workflow_manager
        workflow_manager.get_workflow_by_name.return_value = workflow_type

        postgresql_if = mocked_orchestrator.postgresql_if
        postgresql_if.get_job.return_value = job_db
        postgresql_if.count_job_starts.return_value = 0

        omotes_sdk_if = mocked_orchestrator.omotes_orchestrator_sdk_if

        life_cycle_barrier_manager_obj_mock = (
            mocked_orchestrator.life_cycle_barrier_manager_obj_mock
        )

        # Act
        orchestrator.task_progress_update(task_progress_update)

        # Assert
        workflow_manager.get_workflow_by_name.assert_called_once_with(workflow_type_name)
        postgresql_if.get_job.assert_called_once_with(job_id)
        postgresql_if.count_job_starts.assert_called_once_with(job_id)
        postgresql_if.set_job_running.assert_called_once_with(job_id)
        omotes_sdk_if.send_job_status_update.assert_called_once_with(
            job=job,
            status_update=JobStatusUpdate(
                uuid=str(job_id), status=JobStatusUpdate.JobStatus.RUNNING
            ),
        )

        life_cycle_barrier_manager_obj_mock.wait_for_barrier.assert_not_called()

    def test__task_progress_update__START_progress_update_before_db_update(self) -> None:
        # Arrange
        job_id = uuid.uuid4()
        celery_task_id = uuid.uuid4()
        workflow_type_name = "some-workflow-type"
        workflow_type = WorkflowType(
            workflow_type_name=workflow_type_name, workflow_type_description_name="description"
        )
        job = Job(id=job_id, workflow_type=workflow_type)

        job_db_1 = JobDB(
            job_id=job_id,
            celery_id=str(celery_task_id),
            workflow_type=workflow_type_name,
            status=JobStatusDB.REGISTERED,
            registered_at=datetime.datetime.fromisoformat("2024-08-29T14:00:00"),
            timeout_after_ms=10_000,
        )
        job_db_2 = JobDB(
            job_id=job_id,
            celery_id=str(celery_task_id),
            workflow_type=workflow_type_name,
            status=JobStatusDB.SUBMITTED,
            registered_at=datetime.datetime.fromisoformat("2024-08-29T14:00:00"),
            submitted_at=datetime.datetime.fromisoformat("2024-08-29T14:00:01"),
            timeout_after_ms=10_000,
        )
        task_progress_update = TaskProgressUpdate(
            job_id=str(job_id),
            celery_task_id=str(celery_task_id),
            celery_task_type=workflow_type_name,
            status=TaskProgressUpdate.START,
            message="Task has started. Some message here.",
        )

        mocked_orchestrator = OrchestratorTest.MockedOrchestrator()
        orchestrator = mocked_orchestrator.orchestrator

        workflow_manager = mocked_orchestrator.workflow_manager
        workflow_manager.get_workflow_by_name.return_value = workflow_type

        postgresql_if = mocked_orchestrator.postgresql_if
        postgresql_if.get_job.side_effect = [job_db_1, job_db_2]
        postgresql_if.count_job_starts.return_value = 0

        omotes_sdk_if = mocked_orchestrator.omotes_orchestrator_sdk_if

        life_cycle_barrier_manager_obj_mock = (
            mocked_orchestrator.life_cycle_barrier_manager_obj_mock
        )

        # Act
        orchestrator.task_progress_update(task_progress_update)

        # Assert
        workflow_manager.get_workflow_by_name.assert_called_once_with(workflow_type_name)
        postgresql_if.get_job.assert_has_calls(
            [unittest.mock.call(job_id), unittest.mock.call(job_id)]
        )
        life_cycle_barrier_manager_obj_mock.wait_for_barrier.assert_called_with(job_id)
        postgresql_if.count_job_starts.assert_called_once_with(job_id)
        postgresql_if.set_job_running.assert_called_once_with(job_id)
        omotes_sdk_if.send_job_status_update.assert_called_once_with(
            job=job,
            status_update=JobStatusUpdate(
                uuid=str(job_id), status=JobStatusUpdate.JobStatus.RUNNING
            ),
        )

    def test__task_progress_update__repeated_START_progress_update_over_threshold(self) -> None:
        # Arrange
        job_id = uuid.uuid4()
        celery_task_id = uuid.uuid4()
        workflow_type_name = "some-workflow-type"
        workflow_type = WorkflowType(
            workflow_type_name=workflow_type_name, workflow_type_description_name="description"
        )
        job = Job(id=job_id, workflow_type=workflow_type)

        job_db = JobDB(
            job_id=job_id,
            celery_id=str(celery_task_id),
            workflow_type=workflow_type_name,
            status=JobStatusDB.SUBMITTED,
            registered_at=datetime.datetime.fromisoformat("2024-08-29T14:00:00"),
            submitted_at=datetime.datetime.fromisoformat("2024-08-29T14:00:01"),
            timeout_after_ms=10_000,
        )
        task_progress_update = TaskProgressUpdate(
            job_id=str(job_id),
            celery_task_id=str(celery_task_id),
            celery_task_type=workflow_type_name,
            status=TaskProgressUpdate.START,
            message="Task has started. Some message here.",
        )

        mocked_orchestrator = OrchestratorTest.MockedOrchestrator()
        orchestrator = mocked_orchestrator.orchestrator

        workflow_manager = mocked_orchestrator.workflow_manager
        workflow_manager.get_workflow_by_name.return_value = workflow_type

        postgresql_if = mocked_orchestrator.postgresql_if
        postgresql_if.get_job.return_value = job_db
        postgresql_if.count_job_starts.return_value = 1

        omotes_sdk_if = mocked_orchestrator.omotes_orchestrator_sdk_if

        life_cycle_barrier_manager_obj_mock = (
            mocked_orchestrator.life_cycle_barrier_manager_obj_mock
        )

        celery_if = mocked_orchestrator.celery_if

        # Act
        orchestrator.task_progress_update(task_progress_update)

        # Assert
        workflow_manager.get_workflow_by_name.assert_called_once_with(workflow_type_name)
        postgresql_if.get_job.assert_called_once_with(job_id)
        postgresql_if.count_job_starts.assert_called_once_with(job_id)
        celery_if.cancel_workflow.assert_called_once_with(str(celery_task_id))
        omotes_sdk_if.send_job_status_update.assert_called_once_with(
            job=job,
            status_update=JobStatusUpdate(
                uuid=str(job_id), status=JobStatusUpdate.JobStatus.CANCELLED
            ),
        )
        omotes_sdk_if.send_job_result.assert_called_once_with(
            job=job,
            result=JobResult(
                uuid=str(job.id),
                result_type=JobResult.ResultType.ERROR,
                output_esdl=None,
                logs="Job cannot be processed due to being retried the maximum number of times.",
            ),
        )
        postgresql_if.delete_job.assert_called_once_with(job_id)
        life_cycle_barrier_manager_obj_mock.cleanup_barrier(job_id)

        life_cycle_barrier_manager_obj_mock.wait_for_barrier.assert_not_called()
        postgresql_if.set_job_running.assert_not_called()

    def test__task_progress_update__progress_update(self) -> None:
        # Arrange
        job_id = uuid.uuid4()
        celery_task_id = uuid.uuid4()
        workflow_type_name = "some-workflow-type"
        workflow_type = WorkflowType(
            workflow_type_name=workflow_type_name, workflow_type_description_name="description"
        )
        job = Job(id=job_id, workflow_type=workflow_type)

        job_db = JobDB(
            job_id=job_id,
            celery_id=str(celery_task_id),
            workflow_type=workflow_type_name,
            status=JobStatusDB.RUNNING,
            registered_at=datetime.datetime.fromisoformat("2024-08-29T14:00:00"),
            submitted_at=datetime.datetime.fromisoformat("2024-08-29T14:00:01"),
            running_at=datetime.datetime.fromisoformat("2024-08-29T14:00:02"),
            timeout_after_ms=10_000,
        )
        task_progress_update = TaskProgressUpdate(
            job_id=str(job_id),
            celery_task_id=str(celery_task_id),
            celery_task_type=workflow_type_name,
            progress=0.1,
            message="Task is sort of progressing.",
        )

        mocked_orchestrator = OrchestratorTest.MockedOrchestrator()
        orchestrator = mocked_orchestrator.orchestrator

        workflow_manager = mocked_orchestrator.workflow_manager
        workflow_manager.get_workflow_by_name.return_value = workflow_type

        postgresql_if = mocked_orchestrator.postgresql_if
        postgresql_if.get_job.return_value = job_db

        omotes_sdk_if = mocked_orchestrator.omotes_orchestrator_sdk_if

        life_cycle_barrier_manager_obj_mock = (
            mocked_orchestrator.life_cycle_barrier_manager_obj_mock
        )

        # Act
        orchestrator.task_progress_update(task_progress_update)

        # Assert
        workflow_manager.get_workflow_by_name.assert_called_once_with(workflow_type_name)
        postgresql_if.get_job.assert_called_once_with(job_id)

        omotes_sdk_if.send_job_progress_update.assert_called_once_with(
            job,
            JobProgressUpdate(
                uuid=str(job.id),
                progress=task_progress_update.progress,
                message=task_progress_update.message,
            ),
        )

        life_cycle_barrier_manager_obj_mock.wait_for_barrier.assert_not_called()
        postgresql_if.set_job_running.assert_not_called()
        postgresql_if.count_job_starts.assert_not_called()
        omotes_sdk_if.send_job_status_update.assert_not_called()

    def test__task_progress_update__unknown_workflow(self) -> None:
        # Arrange
        job_id = uuid.uuid4()
        celery_task_id = uuid.uuid4()
        workflow_type_name = "some-workflow-type"

        task_progress_update = TaskProgressUpdate(
            job_id=str(job_id),
            celery_task_id=str(celery_task_id),
            celery_task_type=workflow_type_name,
            progress=0.1,
            message="Task is sort of progressing.",
        )

        mocked_orchestrator = OrchestratorTest.MockedOrchestrator()
        orchestrator = mocked_orchestrator.orchestrator

        workflow_manager = mocked_orchestrator.workflow_manager
        workflow_manager.get_workflow_by_name.return_value = None

        postgresql_if = mocked_orchestrator.postgresql_if

        omotes_sdk_if = mocked_orchestrator.omotes_orchestrator_sdk_if

        life_cycle_barrier_manager_obj_mock = (
            mocked_orchestrator.life_cycle_barrier_manager_obj_mock
        )

        # Act
        orchestrator.task_progress_update(task_progress_update)

        # Assert
        workflow_manager.get_workflow_by_name.assert_called_once_with(workflow_type_name)

        postgresql_if.get_job.assert_not_called()
        postgresql_if.set_job_running.assert_not_called()
        postgresql_if.count_job_starts.assert_not_called()
        omotes_sdk_if.send_job_progress_update.assert_not_called()
        omotes_sdk_if.send_job_status_update.assert_not_called()
        life_cycle_barrier_manager_obj_mock.wait_for_barrier.assert_not_called()

    def test__task_progress_update__unknown_job(self) -> None:
        # Arrange
        job_id = uuid.uuid4()
        celery_task_id = uuid.uuid4()
        workflow_type_name = "some-workflow-type"
        workflow_type = WorkflowType(
            workflow_type_name=workflow_type_name, workflow_type_description_name="description"
        )
        task_progress_update = TaskProgressUpdate(
            job_id=str(job_id),
            celery_task_id=str(celery_task_id),
            celery_task_type=workflow_type_name,
            status=TaskProgressUpdate.START,
            message="Task has started. Some message here.",
        )

        mocked_orchestrator = OrchestratorTest.MockedOrchestrator()
        orchestrator = mocked_orchestrator.orchestrator

        workflow_manager = mocked_orchestrator.workflow_manager
        workflow_manager.get_workflow_by_name.return_value = workflow_type

        postgresql_if = mocked_orchestrator.postgresql_if
        postgresql_if.get_job.return_value = None

        omotes_sdk_if = mocked_orchestrator.omotes_orchestrator_sdk_if

        life_cycle_barrier_manager_obj_mock = (
            mocked_orchestrator.life_cycle_barrier_manager_obj_mock
        )

        celery_if = mocked_orchestrator.celery_if

        # Act
        orchestrator.task_progress_update(task_progress_update)

        # Assert
        workflow_manager.get_workflow_by_name.assert_called_once_with(workflow_type_name)
        postgresql_if.get_job.assert_called_once_with(job_id)
        celery_if.cancel_workflow.assert_called_once_with(str(celery_task_id))

        postgresql_if.count_job_starts.assert_not_called()
        postgresql_if.set_job_running.assert_not_called()
        omotes_sdk_if.send_job_status_update.assert_not_called()
        omotes_sdk_if.send_job_progress_update.assert_not_called()
        life_cycle_barrier_manager_obj_mock.wait_for_barrier.assert_not_called()

    def test__task_progress_update__wrong_celery_task(self) -> None:
        # Arrange
        job_id = uuid.uuid4()
        celery_task_id_progress_update = "celery-id-wrong"
        celery_task_id_db = "celery-id"
        workflow_type_name = "some-workflow-type"
        workflow_type = WorkflowType(
            workflow_type_name=workflow_type_name, workflow_type_description_name="description"
        )

        job_db = JobDB(
            job_id=job_id,
            celery_id=celery_task_id_db,
            workflow_type=workflow_type_name,
            status=JobStatusDB.SUBMITTED,
            registered_at=datetime.datetime.fromisoformat("2024-08-29T14:00:00"),
            submitted_at=datetime.datetime.fromisoformat("2024-08-29T14:00:01"),
            timeout_after_ms=10_000,
        )
        task_progress_update = TaskProgressUpdate(
            job_id=str(job_id),
            celery_task_id=celery_task_id_progress_update,
            celery_task_type=workflow_type_name,
            status=TaskProgressUpdate.START,
            message="Task has started. Some message here.",
        )

        mocked_orchestrator = OrchestratorTest.MockedOrchestrator()
        orchestrator = mocked_orchestrator.orchestrator

        workflow_manager = mocked_orchestrator.workflow_manager
        workflow_manager.get_workflow_by_name.return_value = workflow_type

        postgresql_if = mocked_orchestrator.postgresql_if
        postgresql_if.get_job.return_value = job_db
        postgresql_if.count_job_starts.return_value = 0

        omotes_sdk_if = mocked_orchestrator.omotes_orchestrator_sdk_if

        life_cycle_barrier_manager_obj_mock = (
            mocked_orchestrator.life_cycle_barrier_manager_obj_mock
        )
        celery_if = mocked_orchestrator.celery_if

        # Act
        orchestrator.task_progress_update(task_progress_update)

        # Assert
        workflow_manager.get_workflow_by_name.assert_called_once_with(workflow_type_name)
        postgresql_if.get_job.assert_called_once_with(job_id)
        celery_if.cancel_workflow.assert_called_once_with(celery_task_id_progress_update)

        postgresql_if.count_job_starts.assert_not_called()
        postgresql_if.set_job_running.assert_not_called()
        omotes_sdk_if.send_job_status_update.assert_not_called()
        omotes_sdk_if.send_job_progress_update.assert_not_called()
        life_cycle_barrier_manager_obj_mock.wait_for_barrier.assert_not_called()

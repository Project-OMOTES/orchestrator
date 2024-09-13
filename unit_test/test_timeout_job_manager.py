import unittest
from datetime import datetime, timedelta

from omotes_orchestrator.db_models import JobStatus, JobDB
from omotes_orchestrator.timeout_job_manager import TimeoutJobManager


class TimeoutJobManagerTest(unittest.TestCase):
    def test__job_is_timed_out__returns_false_on_status_registered(self) -> None:
        # Arrange
        job = JobDB()
        # Arrange the case where the job should be considered as timed out,
        # but job_is_timedout() returns false due to job.status != RUNNING
        job.running_at = datetime.now() - timedelta(milliseconds=100)
        job.timeout_after_ms = 0
        job.status = JobStatus.REGISTERED

        # Act / Assert
        self.assertFalse(TimeoutJobManager.job_is_timedout(job))

    def test__job_is_timed_out__returns_false_on_status_submitted(self) -> None:
        # Arrange
        job = JobDB()
        # Arrange the case where the job should be considered as timed out,
        # but job_is_timedout() returns false due to job.status != RUNNING
        job.running_at = datetime.now() - timedelta(milliseconds=100)
        job.timeout_after_ms = 0
        job.status = JobStatus.SUBMITTED

        # Act / Assert
        self.assertFalse(TimeoutJobManager.job_is_timedout(job))

    def test__job_is_timed_out__returns_false_on_status_running(self) -> None:
        # Arrange
        job = JobDB()
        # Arrange the case where the job is not timed out yet
        job.running_at = datetime.now() - timedelta(milliseconds=100)
        job.timeout_after_ms = 10000
        job.status = JobStatus.RUNNING

        # Act / Assert
        self.assertFalse(TimeoutJobManager.job_is_timedout(job))

    def test__job_is_timed_out__returns_true_on_status_running(self) -> None:
        # Arrange
        job = JobDB()
        # Arrange the case where the job should be considered as timed out
        job.running_at = datetime.now() - timedelta(milliseconds=100)
        job.timeout_after_ms = 0
        job.status = JobStatus.RUNNING

        # Act / Assert
        self.assertTrue(TimeoutJobManager.job_is_timedout(job))

    def test__job_is_timed_out__returns_false_on_timeout_not_set(self) -> None:
        # Arrange
        job = JobDB()
        # Arrange the case where the job should be considered as timed out,
        # but the check is skipped by job_is_timedout()
        # because job.timeout_after_ms is not specified.
        job.running_at = datetime.now() - timedelta(milliseconds=100)
        job.timeout_after_ms = None
        job.status = JobStatus.RUNNING

        # Act / Assert
        self.assertFalse(TimeoutJobManager.job_is_timedout(job))

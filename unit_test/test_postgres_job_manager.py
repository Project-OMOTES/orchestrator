import unittest
from datetime import datetime, timedelta

from omotes_orchestrator.db_models import JobDB
from omotes_orchestrator.postgres_job_manager import PostgresJobManager


class PostgresJobManagerTest(unittest.TestCase):
    def test__job_row_is_stale_on_registered_at__returns_true(self) -> None:
        # Arrange
        cur_time = datetime.now()
        job = JobDB()
        job.registered_at = cur_time - timedelta(seconds=65)

        # Act / Assert
        self.assertTrue(
            PostgresJobManager.job_row_is_stale(job=job, ref_time=cur_time, job_retention_sec=60)
        )

    def test__job_row_is_stale_on_registered_at__returns_false(self) -> None:
        # Arrange
        cur_time = datetime.now()
        job = JobDB()
        job.registered_at = cur_time - timedelta(seconds=55)

        # Act / Assert
        self.assertFalse(
            PostgresJobManager.job_row_is_stale(job=job, ref_time=cur_time, job_retention_sec=60)
        )

    def test__job_row_is_stale_on_submitted_at__returns_true(self) -> None:
        # Arrange
        cur_time = datetime.now()
        job = JobDB()
        job.registered_at = cur_time - timedelta(seconds=65)
        job.submitted_at = cur_time - timedelta(seconds=64)

        # Act / Assert
        self.assertTrue(
            PostgresJobManager.job_row_is_stale(job=job, ref_time=cur_time, job_retention_sec=60)
        )

    def test__job_row_is_stale_on_submitted_at__returns_false(self) -> None:
        # Arrange
        cur_time = datetime.now()
        job = JobDB()
        job.registered_at = cur_time - timedelta(seconds=65)
        job.submitted_at = cur_time - timedelta(seconds=55)

        # Act / Assert
        self.assertFalse(
            PostgresJobManager.job_row_is_stale(job=job, ref_time=cur_time, job_retention_sec=60)
        )

    def test__job_row_is_stale_on_running_at__returns_true(self) -> None:
        # Arrange
        cur_time = datetime.now()
        job = JobDB()
        job.registered_at = cur_time - timedelta(seconds=65)
        job.submitted_at = cur_time - timedelta(seconds=64)
        job.running_at = cur_time - timedelta(seconds=63)

        # Act / Assert
        self.assertTrue(
            PostgresJobManager.job_row_is_stale(job=job, ref_time=cur_time, job_retention_sec=60)
        )

    def test__job_row_is_stale_on_running_at__returns_false(self) -> None:
        # Arrange
        cur_time = datetime.now()
        job = JobDB()
        job.registered_at = cur_time - timedelta(seconds=65)
        job.submitted_at = cur_time - timedelta(seconds=64)
        job.running_at = cur_time - timedelta(seconds=55)

        # Act / Assert
        self.assertFalse(
            PostgresJobManager.job_row_is_stale(job=job, ref_time=cur_time, job_retention_sec=60)
        )

import logging
from datetime import datetime, timezone
import time
import threading
from omotes_orchestrator.config import PostgresJobManagerConfig
from omotes_orchestrator.postgres_interface import PostgresInterface
from omotes_orchestrator.db_models.job import JobDB


LOGGER = logging.getLogger("omotes_orchestrator")


class PostgresJobManager:
    """Periodically checks the job row in the database and cleans up the stale one."""

    postgresql_if: PostgresInterface
    """Interface to PostgreSQL."""
    postgres_job_manager_config: PostgresJobManagerConfig
    """PostgresJobManager configuration"""
    _init_time: datetime
    """Instantiated datetime (UTC), is typically instantiated when the orchestrator is started"""
    _active_threshold_sec: int
    """Only when the instance is initialized longer than the threshold period
    can it start cleaning the stale jobs."""
    _job_retention_sec: int
    """The allowed retention time in seconds of a postgres job row"""
    _rerun_sec: int
    """The period in seconds to rerun the stale job cleaning task"""

    def __init__(self,
                 postgresql_if: PostgresInterface,
                 postgres_job_manager_config: PostgresJobManagerConfig) -> None:
        """Construct the postgres job manager."""
        self.postgresql_if = postgresql_if
        self.postgres_job_manager_config = postgres_job_manager_config
        self._init_time = datetime.now(timezone.utc)
        self._active_threshold_sec = self.postgres_job_manager_config.job_retention_sec
        self._job_retention_sec = self.postgres_job_manager_config.job_retention_sec
        self._rerun_sec = 30

        self._stale_jobs_cleaner_thread = threading.Thread(target=self.stale_jobs_cleaner,
                                                           daemon=True)
        self._stale_jobs_cleaner_thread_active = False

    def start(self) -> None:
        """Start the postgres job manager activities as a daemon process."""
        if not self._stale_jobs_cleaner_thread_active:
            self._stale_jobs_cleaner_thread.start()
            self._stale_jobs_cleaner_thread_active = True

            LOGGER.info("Starting the postgres job manager")

    def stop(self) -> None:
        """Stop the postgres job manager activities."""
        if self._stale_jobs_cleaner_thread_active:
            self._stale_jobs_cleaner_thread.join(timeout=0.0)
            self._stale_jobs_cleaner_thread_active = False

            LOGGER.info("Stopped the postgres job manager")

    def stale_jobs_cleaner(self) -> None:
        """Start a background process to clean up stale jobs longer than the retention time.

        The function periodically checks if there are any stale jobs/rows in the database
        longer than the configured retention time. Meanwhile if the PostgresJobManager
        is instantiated (usually when the orchestrator is started) longer than
        the configured time, the job/row will be deleted outright.
        """
        while True:
            cur_time = datetime.now(timezone.utc)
            active_sec = (cur_time - self._init_time).total_seconds()

            if active_sec > self._active_threshold_sec:
                jobs = self.postgresql_if.get_all_jobs()
                for job in jobs:
                    if self.job_row_is_stale(job, cur_time):
                        self.postgresql_if.delete_job(job.job_id)

                        job_manager_up_mins = round(active_sec / 60, 1)
                        LOGGER.info("PostgresJobManager is up %s mins. "
                                    + "Founded and deleted a stale job %s",
                                    job_manager_up_mins, job.job_id)

            time.sleep(self._rerun_sec)

    def job_row_is_stale(self, job: JobDB, ref_time: datetime) -> bool:
        """Check if the job row is stale and can be safely deleted subsequently.

        :param job: Database job row
        :param ref_time: Reference datetime used for determining if the job row is stale
        :return: True if the job is stale and can be safely deleted subsequently
        """
        if job.running_at:
            job_duration_sec = (ref_time - job.running_at).total_seconds()
        elif job.submitted_at:
            job_duration_sec = (ref_time - job.submitted_at).total_seconds()
        else:
            job_duration_sec = (ref_time - job.registered_at).total_seconds()

        job_row_is_stale = job_duration_sec > self._job_retention_sec
        return job_row_is_stale


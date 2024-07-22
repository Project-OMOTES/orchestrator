# To avoid cyclic import error
from __future__ import annotations
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from omotes_orchestrator.main import Orchestrator

import logging
from datetime import datetime, timedelta
import threading
from omotes_orchestrator.postgres_interface import PostgresInterface
from omotes_orchestrator.db_models.job import JobDB, JobStatus
from omotes_sdk_protocol.job_pb2 import JobCancel
from omotes_orchestrator.config import TimeoutJobManagerConfig

LOGGER = logging.getLogger("omotes_orchestrator")


class TimeoutJobManager:
    """Periodically checks and cancels if any job is timed out."""

    postgresql_if: PostgresInterface
    """Interface to PostgreSQL."""
    orchestrator: Optional[Orchestrator]
    """Orchestrator application. Only when the orchestrator is linked with
    the TimeoutJobManager instance, can the timeout_jobs_handler() method function properly."""
    config: TimeoutJobManagerConfig
    """TimeoutJobManager configuration."""
    _init_time: datetime
    """Instantiated datetime, is typically instantiated when the orchestrator is started."""
    _start_buffer_sec: int
    """Only when the instance is initialized longer than the buffer period
    can it start canceling the timed out jobs."""
    _rerun_sec: int
    """Period in seconds to rerun the timeout job cancellation task."""
    _stop_event: threading.Event
    """Event to signal the thread to stop."""
    _timeout_jobs_handler_thread: threading.Thread
    """Thread for canceling the timed out job."""
    _timeout_jobs_handler_thread_active: bool
    """Flag indicating if the timeout job handler thread is active."""

    def __init__(
        self,
        postgresql_if: PostgresInterface,
        orchestrator: Optional[Orchestrator],
        config: TimeoutJobManagerConfig,
    ) -> None:
        """Construct the timeout job manager."""
        self.postgresql_if = postgresql_if
        self.orchestrator = orchestrator

        self._init_time = datetime.now()
        self._start_buffer_sec = config.start_buffer_sec
        self._rerun_sec = config.rerun_sec

        self._stop_event = threading.Event()
        self._timeout_jobs_handler_thread = threading.Thread(target=self.timeout_jobs_handler)
        self._timeout_jobs_handler_thread_active = False

    def start(self) -> None:
        """Start the timeout job manager as a daemon process."""
        if not self._timeout_jobs_handler_thread_active:
            LOGGER.info("Starting the timeout job manager")

            self._stop_event.clear()
            self._timeout_jobs_handler_thread.start()
            self._timeout_jobs_handler_thread_active = True

    def stop(self) -> None:
        """Stop the timeout job manager."""
        if self._timeout_jobs_handler_thread_active:
            LOGGER.info("Stopping the timeout job manager")

            self._stop_event.set()
            self._timeout_jobs_handler_thread.join(timeout=5.0)
            self._timeout_jobs_handler_thread_active = False

    def _system_activation_sec(self) -> float:
        """Return period in seconds since the TimeoutJobManager instantiated."""
        return (datetime.now() - self._init_time).total_seconds()

    def timeout_jobs_handler(self) -> None:
        """Start a background process to cancel timed out jobs.

        The function periodically checks if there are any timed out jobs.
        Meanwhile, if the TimeoutJobManager is instantiated longer than the
        configured buffer time, and the relationship between TimeoutJobManager
        and Orchestrator is correctly linked (self.orchestrator is not None, so
        self.orchestrator.job_cancellation_handler() can be called),
        the job/row can be canceled outright.
        """
        while not self._stop_event.is_set():
            if self.orchestrator is None:
                LOGGER.error("Exiting timeout_jobs_handler(): orchestrator is None.")
                break

            if self._system_activation_sec() > self._start_buffer_sec:
                jobs = self.postgresql_if.get_all_jobs()
                for job in jobs:
                    if self.job_is_timedout(job):
                        self.orchestrator.job_cancellation_handler(JobCancel(uuid=str(job.job_id)))

                        timeout_job_manager_up_mins = round(self._system_activation_sec() / 60, 1)
                        LOGGER.warning(
                            "TimeoutJobManager is up %s mins. "
                            + "Found and canceled a timed out job %s",
                            timeout_job_manager_up_mins,
                            job.job_id,
                        )

                LOGGER.debug("Processed %s jobs to check for timed out jobs", len(jobs))

            if self._stop_event.is_set():
                LOGGER.info("Stopped the timeout job manager gracefully.")
                break

            self._stop_event.wait(self._rerun_sec)

    @staticmethod
    def job_is_timedout(job: JobDB) -> bool:
        """Check if the job is timed out.

        :param job: Database job row
        :return: True if the job status is RUNNING and the current time exceeds
        the job running time + configured job timeout delta
        """
        if job.status == JobStatus.RUNNING and job.running_at:
            job_tz = job.running_at.tzinfo
            cur_time_tz = datetime.now(job_tz)
            return cur_time_tz > job.running_at + timedelta(milliseconds=job.timeout_after_ms)
        else:
            return False

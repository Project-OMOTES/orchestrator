import logging
from datetime import datetime, timedelta, timezone
import threading
from omotes_orchestrator.postgres_interface import PostgresInterface
# from omotes_orchestrator.main import Orchestrator
from omotes_orchestrator.db_models.job import JobDB, JobStatus
from omotes_sdk_protocol.job_pb2 import JobCancel

LOGGER = logging.getLogger("omotes_orchestrator")


class TimeoutJobManager:
    """Periodically checks if any job is timed out. Cancel and delete the timed-out job."""

    postgresql_if: PostgresInterface
    """Interface to PostgreSQL."""
    # orchestrator: Orchestrator  # TODO
    """Orchestrator application instance"""
    _init_time: datetime
    """Instantiated datetime (UTC), is typically instantiated when the orchestrator is started"""
    _active_threshold_sec: int
    """Only when the instance is initialized longer than the threshold period
    can it start canceling the timed-out jobs."""
    _rerun_sec: int
    """Period in seconds to rerun the timeout job cancellation task"""
    _stop_event: threading.Event
    """Event to signal the thread to stop"""
    _timeout_jobs_handler_thread: threading.Thread
    """Thread for canceling the timed-out job"""
    _timeout_jobs_handler_thread_active: bool
    """Flag indicating if the timeout job handler thread is active"""

    def __init__(self, postgresql_if: PostgresInterface) -> None:
        """Construct the timeout job manager."""
        self.postgresql_if = postgresql_if
        # self.orchestrator = orchestrator
        self._init_time = datetime.now(timezone.utc)
        # TODO: get this from config
        self._active_threshold_sec = 20
        # TODO: overwrite the value when finish
        self._rerun_sec = 10

        self._stop_event = threading.Event()
        self._timeout_jobs_handler_thread = threading.Thread(target=self.timeout_jobs_handler)
        self._timeout_jobs_handler_thread_active = False

    def start(self) -> None:
        """
        TODO
        """
        if not self._timeout_jobs_handler_thread_active:
            LOGGER.info("Starting the timeout job manager")

            self._stop_event.clear()
            self._timeout_jobs_handler_thread.start()
            self._timeout_jobs_handler_thread_active = True

    def stop(self) -> None:
        """
        TODO
        """
        if self._timeout_jobs_handler_thread_active:
            LOGGER.info("Stopping the timeout job manager")

            self._stop_event.set()
            self._timeout_jobs_handler_thread.join(timeout=5.0)
            self._timeout_jobs_handler_thread_active = False

    def timeout_jobs_handler(self) -> None:
        """
        TODO
        """
        while not self._stop_event.is_set():
            cur_time = datetime.now(timezone.utc)
            active_sec = (cur_time - self._init_time).total_seconds()

            if active_sec > self._active_threshold_sec:
                jobs = self.postgresql_if.get_all_jobs()
                for job in jobs:
                    if self.job_is_timedout(job):
                        # job_cancellation = JobCancel(uuid=str(job.job_id))
                        
                        # self.orchestrator.job_cancellation_handler(job_cancellation)

                        timeout_job_manager_up_mins = round(active_sec / 60, 1)
                        LOGGER.warning("TimeoutJobManager is up %s mins. "
                                       + "Found and deleted a timed-out job %s",
                                       timeout_job_manager_up_mins, job.job_id)

            if self._stop_event.is_set():
                LOGGER.info("Stopped the timeout job manager gracefully.")
                break

            self._stop_event.wait(self._rerun_sec)

    @staticmethod
    def job_is_timedout(job: JobDB) -> bool:
        """
        TODO

        TODO: if job.timeout_after_ms is not specified, job can be cancelled immediately -> add a test case on this
        """
        if job.status == JobStatus.RUNNING and job.running_at:
            # To ensure current time is on the same timezone as job.running_at to ensure a valid comparison
            job_tz = job.running_at.tzinfo
            cur_time_tz = datetime.now().replace(tzinfo=job_tz)
            return cur_time_tz >= job.running_at + timedelta(milliseconds=job.timeout_after_ms)
        else:
            return False

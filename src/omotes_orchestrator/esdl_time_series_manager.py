import logging
from datetime import datetime, timezone
import threading
from omotes_orchestrator.config import PostgresJobManagerConfig
from omotes_orchestrator.db_models import EsdlTimeSeriesInfoDB
from omotes_orchestrator.postgres_interface import PostgresInterface
from omotes_orchestrator.time_series_db_interface import TimeSeriesDBInterface

LOGGER = logging.getLogger("omotes_orchestrator")


class EsdlTimeSeriesManager:
    """Periodically checks the time series databases and cleans up stale time series data."""

    postgresql_if: PostgresInterface
    """Interface to PostgreSQL."""
    time_series_database_if: TimeSeriesDBInterface
    """Interface to time series database."""
    postgres_job_manager_config: PostgresJobManagerConfig
    """PostgresJobManager configuration"""
    _job_retention_sec: int
    """Allowed retention time in seconds of a postgres job row: maximum duration of a job"""
    _stop_event: threading.Event
    """Event to signal the thread to stop"""
    _inactive_time_series_data_cleaner_thread: threading.Thread
    """Thread for cleaning time series data"""
    _inactive_time_series_data_cleaner_thread_active: bool
    """Flag indicating if the time series data cleaner thread is active"""

    def __init__(
        self,
        postgresql_if: PostgresInterface,
        time_series_database_if: TimeSeriesDBInterface,
        postgres_job_manager_config: PostgresJobManagerConfig,
    ) -> None:
        """Construct the ESDL time series manager."""
        self.postgresql_if = postgresql_if
        self.time_series_database_if = time_series_database_if
        self.postgres_job_manager_config = postgres_job_manager_config
        self._job_retention_sec = self.postgres_job_manager_config.job_retention_sec

        self._stop_event = threading.Event()
        self._inactive_time_series_data_cleaner_thread = threading.Thread(
            target=self.time_series_data_cleaner
        )
        self._inactive_time_series_data_cleaner_thread_active = False

    def start(self) -> None:
        """Start the time series data manager activities as a daemon process."""
        if not self._inactive_time_series_data_cleaner_thread_active:
            LOGGER.info("Starting the time series data manager")

            self._stop_event.clear()
            self._inactive_time_series_data_cleaner_thread.start()
            self._inactive_time_series_data_cleaner_thread_active = True

    def stop(self) -> None:
        """Stop the time series data manager activities."""
        if self._inactive_time_series_data_cleaner_thread_active:
            LOGGER.info("Stopping the time series data manager")

            self._stop_event.set()
            self._inactive_time_series_data_cleaner_thread.join(timeout=5.0)
            self._inactive_time_series_data_cleaner_thread_active = False

    def time_series_data_cleaner(self) -> None:
        """Start a background process to clean up time series data.

        1. Time series database cleanup
        - if an ESDL time series data collection (database) has no corresponding
          `esdl_time_series_info` table entry: register with `deactivated_at` set to the current
          time, marking the time series data for deletion
        - if an ESDL time series data collection (database) has a corresponding
          `esdl_time_series_info` table entry, and `deactivated_at` is non-`NULL`, and
          `_job_retention_sec` has passed since `deactivated_at`: remove the time series database
          and the `esdl_time_series_info` table entry
        2. Postgres `esdl_time_series_info` table cleanup
        - if a `esdl_time_series_info` table entry has no 'esdl_id' of no corresponding time series
          data, and _job_retention_sec` has passed since `registered_at`: remove this
          `esdl_time_series_info` table entry
        """
        while not self._stop_event.is_set():
            now_time = datetime.now(timezone.utc)

            time_series_db_esdl_ids = (
                self.time_series_database_if.get_esdl_ids_with_time_series_data()
            )

            for esdl_id in time_series_db_esdl_ids:
                esdl_time_series_info = self.postgresql_if.get_esdl_time_series_info_by_esdl_id(
                    esdl_id
                )
                if esdl_time_series_info:
                    if self.time_series_data_is_timed_out(
                        esdl_time_series_info, now_time, self._job_retention_sec
                    ):
                        LOGGER.info("time series manager: delete time series for ESDL: %s", esdl_id)
                        self.time_series_database_if.delete_database_for_esdl(esdl_id)
                        self.postgresql_if.delete_esdl_time_series_info_by_esdl_id(esdl_id)
                else:  # stale time series data
                    self.postgresql_if.put_new_esdl_time_series_info(
                        output_esdl_id=esdl_id,
                        set_inactive=True,
                    )

            esdl_time_series_info_entries = self.postgresql_if.get_all_esdl_time_series_infos()
            for esdl_time_series_info in esdl_time_series_info_entries:
                if self.esdl_time_series_info_is_stale(
                    esdl_time_series_info,
                    time_series_db_esdl_ids,
                    now_time,
                    self._job_retention_sec,
                ):
                    LOGGER.info(
                        "time series manager: delete ESDL time series info for: %s",
                        esdl_time_series_info.esdl_id,
                    )
                    self.postgresql_if.delete_esdl_time_series_info(esdl_time_series_info)

            if self._stop_event.is_set():
                LOGGER.info("Stopped the time series data cleaner gracefully.")
                break

            self._stop_event.wait(self._job_retention_sec)

    @staticmethod
    def time_series_data_is_timed_out(
        esdl_time_series_info: EsdlTimeSeriesInfoDB, ref_time: datetime, retention_sec: int
    ) -> bool:
        """Check if the time series data is timed out and can be safely deleted.

        :param esdl_time_series_info: Database esdl_time_series_info row
        :param ref_time: Reference datetime used for determining if the job row is stale
        :param retention_sec: Allowed retention time in seconds of stale time series data
        :return: True if the time series data is timed out and can be safely deleted
        """
        if esdl_time_series_info.deactivated_at:  # time series data set to be deleted
            inactive_seconds = (ref_time - esdl_time_series_info.deactivated_at).total_seconds()
            return inactive_seconds > retention_sec
        return False

    @staticmethod
    def esdl_time_series_info_is_stale(
        esdl_time_series_info: EsdlTimeSeriesInfoDB,
        time_series_db_esdl_ids: list[str],
        ref_time: datetime,
        retention_sec: int,
    ) -> bool:
        """Check if the esdl_time_series_info row is stale and can be safely deleted subsequently.

        :param esdl_time_series_info: Database esdl_time_series_info row
        :param time_series_db_esdl_ids: ESDL id's for which data exists in the time series DB
        :param ref_time: Reference datetime used for determining if the job row is stale
        :param retention_sec: Allowed retention time in seconds of esdl time series info without
                              corresponding time series DB data
        :return: True if the esdl time series info is stale and can be safely deleted
        """
        if (
            not esdl_time_series_info.esdl_id
            or esdl_time_series_info.esdl_id not in time_series_db_esdl_ids
        ):  # esdl time series info has no corresponding data in time series database
            registered_seconds = (ref_time - esdl_time_series_info.registered_at).total_seconds()
            return registered_seconds > retention_sec
        return False

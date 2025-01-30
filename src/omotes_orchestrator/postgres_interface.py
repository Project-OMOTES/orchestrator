import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import logging
from typing import Generator, Optional

from sqlalchemy import select, update, create_engine, orm, func, delete
from sqlalchemy.orm import Session as SQLSession
from sqlalchemy.engine import Engine, URL

from omotes_orchestrator.db_models import JobDB, JobStatus, JobStartsDB, EsdlTimeSeriesInfoDB
from omotes_orchestrator.config import PostgreSQLConfig

LOGGER = logging.getLogger("omotes_orchestrator")

session_factory = orm.sessionmaker(expire_on_commit=False)
Session = orm.scoped_session(session_factory)


@contextmanager
def session_scope() -> Generator[SQLSession, None, None]:
    """Provide a transactional scope around a series of operations.

    Ensures that the session is committed and closed. Exceptions raised within the 'with' block
    using this contextmanager should be handled in the with block itself. They will not be caught
    by the 'except' here.

    :return: A single SQL session with which SQL queries may be executed.
    """
    try:
        yield Session()
        Session.commit()
    except Exception as e:
        # Only the exceptions raised by session.commit above are caught here
        Session.rollback()
        raise e
    finally:
        Session.remove()


def initialize_db(application_name: str, config: PostgreSQLConfig) -> Engine:
    """Initialize the database connection by creating the engine.

    Also configure the default session maker.

    :param application_name: Identifier for the connection to the SQL database.
    :param config: Configuration on how to connect to the SQL database.
    """
    LOGGER.info(
        "Connecting to PostgresDB at %s:%s as user %s", config.host, config.port, config.username
    )
    url = URL.create(
        "postgresql+psycopg2",
        username=config.username,
        password=config.password,
        host=config.host,
        port=config.port,
        database=config.database,
    )

    engine = create_engine(
        url,
        pool_size=20,
        max_overflow=5,
        echo=False,
        connect_args={
            "application_name": application_name,
            "options": "-c lock_timeout=30000 -c statement_timeout=300000",  # 5 minutes
        },
    )

    # Bind the global session to the actual engine.
    Session.configure(bind=engine)

    return engine


class PostgresInterface:
    """Interface to the SQL database for any queries to persist or retrieve job information.

    Note: The interface may be called from many threads at once. Therefore each query/function
    in this interface must set up a Session (scope) separately.
    """

    db_config: PostgreSQLConfig
    """Configuration on how to connect to the database."""
    engine: Engine
    """Engine for starting connections to the database."""

    def __init__(self, postgres_config: PostgreSQLConfig) -> None:
        """Create the PostgreSQL interface."""
        self.db_config = postgres_config

    def start(self) -> None:
        """Start the interface and connect to the database."""
        self.engine = initialize_db("omotes_orchestrator", self.db_config)

    def stop(self) -> None:
        """Stop the interface and dispose of any connections."""
        if self.engine:
            self.engine.dispose()

    def put_new_job(
        self,
        job_id: uuid.UUID,
        job_reference: str,
        workflow_type: str,
        timeout_after: Optional[timedelta],
    ) -> None:
        """Insert a new job into the database.

        Note: Assumption is that the job_id is unique and has not yet been added to the database.

        :param job_id: Unique identifier of the job.
        :param job_reference: Human-readable reference of the job.
        :param workflow_type: Name of the workflow this job will execute.
        :param timeout_after: Maximum duration before the job is terminated due to timing out.
        """
        with session_scope() as session:
            # If timeout_after is not given,
            # timeout_after_ms is registered as null in the database.
            if timeout_after is not None:
                timeout_after_ms = round(timeout_after.total_seconds() * 1000)
            else:
                timeout_after_ms = None

            new_job = JobDB(
                job_id=job_id,
                job_reference=job_reference,
                workflow_type=workflow_type,
                status=JobStatus.REGISTERED,
                registered_at=datetime.now(timezone.utc),
                timeout_after_ms=timeout_after_ms,
            )
            session.add(new_job)
        LOGGER.debug("Job %s is submitted as new job in database", job_id)

    def set_job_submitted(self, job_id: uuid.UUID, celery_id: str) -> None:
        """Set the status of the job to SUBMITTED and set the celery task id.

        :param job_id: Job to set the status to SUBMITTED.
        :param celery_id: The celery task that will execute this job.
        """
        LOGGER.debug("Started job with id '%s'", job_id)
        with session_scope() as session:
            stmnt = (
                update(JobDB)
                .where(JobDB.job_id == job_id)
                .values(
                    status=JobStatus.SUBMITTED,
                    submitted_at=datetime.now(timezone.utc),
                    celery_id=celery_id,
                )
            )
            session.execute(stmnt)

    def set_job_running(self, job_id: uuid.UUID) -> None:
        """Set the status of the job to RUNNING.

        :param job_id: Job to set the status to RUNNING.
        """
        LOGGER.debug("Started job with id '%s'", job_id)
        with session_scope() as session:
            started_at = datetime.now(timezone.utc)
            stmnt1 = (
                update(JobDB)
                .where(JobDB.job_id == job_id)
                .values(status=JobStatus.RUNNING, running_at=started_at)
            )
            session.execute(stmnt1)

            session.add(JobStartsDB(job_id=job_id, started_at=started_at))

    def get_job_status(self, job_id: uuid.UUID) -> Optional[JobStatus]:
        """Retrieve the current job status.

        :param job_id: Job to retrieve the status for.
        :return: Current job status.
        """
        LOGGER.debug("Retrieving job status for job with id '%s'", job_id)
        with session_scope() as session:
            stmnt = select(JobDB.status).where(JobDB.job_id == job_id)
            job_status = session.scalar(stmnt)
        return job_status

    def get_job_celery_id(self, job_id: uuid.UUID) -> Optional[str]:
        """Retrieve the current celery task id for the job.

        Note: The Celery task id may be None if the job has not yet been submitted (successfully).

        :param job_id: Job id to retrieve the celery task for.
        :return: Current celery task id if available.
        """
        LOGGER.debug("Retrieving celery id for job with id '%s'", job_id)
        with session_scope() as session:
            stmnt = select(JobDB.celery_id).where(JobDB.job_id == job_id)
            celery_id = session.scalar(stmnt)
        return celery_id

    def get_job(self, job_id: uuid.UUID) -> Optional[JobDB]:
        """Retrieve the job info from the database.

        :param job_id: Job to retrieve the job information for.
        :return: Job if it is available in the database.
        """
        LOGGER.debug("Retrieving job data for job with id '%s'", job_id)
        with session_scope() as session:
            stmnt = select(JobDB).where(JobDB.job_id == job_id)
            job = session.scalar(stmnt)
        return job

    def delete_job(self, job_id: uuid.UUID) -> bool:
        """Remove the job from the database.

        :param job_id: Job to remove from the database.
        :return: True if the job was removed or False if the job was not in the database.
        """
        LOGGER.debug("Deleting job with id '%s'", job_id)
        with session_scope() as session:
            find_stmnt = select(JobDB).where(JobDB.job_id == job_id)
            job = session.scalar(find_stmnt)
            result = False
            if job:
                delete_job_starts_stmnt = delete(JobStartsDB).where(JobStartsDB.job_id == job_id)
                session.execute(delete_job_starts_stmnt)
                session.delete(job)
                result = True

        return result

    def job_exists(self, job_id: uuid.UUID) -> bool:
        """Check if the job exists in the database.

        :param job_id: Check if a job with this id exists.
        :return: True if the job exists, otherwise False.
        """
        LOGGER.debug("Checking if job with id '%s' exists", job_id)
        with session_scope() as session:
            stmnt = select(1).where(JobDB.job_id == job_id)
            job_exists = bool(session.scalar(stmnt))
        return job_exists

    def get_all_jobs(self) -> list[JobDB]:
        """Retrieve a list of all jobs in the database.

        :return: List of all jobs.
        """
        with session_scope() as session:
            stmnt = select(JobDB)
            jobs = list(session.scalars(stmnt).all())
        return jobs

    def count_job_starts(self, job_id: uuid.UUID) -> int:
        """Count how many records are stored that a specific job has been started.

        :return: Number of job starts
        """
        with session_scope() as session:
            stmnt = (
                select(func.count()).select_from(JobStartsDB).where(JobStartsDB.job_id == job_id)
            )
            count = session.scalar(stmnt)

            if count is None:
                count = 0

        return count

    def put_new_esdl_time_series_info(
        self,
        output_esdl_id: str,
        job_id: uuid.UUID | None = None,
        job_reference: str | None = None,
        set_inactive: bool = False,
    ) -> None:
        """Insert a new esdl time series info entry into the database.

        Note: if an entry already exists with the specified output_esdl_id the data is updated.
        This can happen when receiving a result for a job which has already been cancelled. In this
        case the entry will keep a non-null deactivated_at field, indicating that the time series
        data will be removed.

        :param output_esdl_id: Output ESDL id.
        :param job_id: Unique identifier of the job.
        :param job_reference: Name of the job.
        :param set_inactive: Whether to set the time series data to be inactive.
        """
        with session_scope() as session:
            find_stmnt = select(EsdlTimeSeriesInfoDB).where(
                EsdlTimeSeriesInfoDB.esdl_id == output_esdl_id
            )
            current_esdl_time_series = session.scalar(find_stmnt)
            if current_esdl_time_series:
                stmnt = (
                    update(EsdlTimeSeriesInfoDB)
                    .where(EsdlTimeSeriesInfoDB.esdl_id == output_esdl_id)
                    .values(
                        job_id=job_id,
                        job_reference=job_reference,
                    )
                )
                session.execute(stmnt)
            else:
                deactivated_at = None
                if set_inactive:
                    deactivated_at = datetime.now(timezone.utc)

                new_esdl_time_series_info = EsdlTimeSeriesInfoDB(
                    row_id=uuid.uuid4(),
                    esdl_id=output_esdl_id,
                    registered_at=datetime.now(timezone.utc),
                    deactivated_at=deactivated_at,
                    job_id=job_id,
                    job_reference=job_reference,
                )
                session.add(new_esdl_time_series_info)

    def get_esdl_time_series_info_by_esdl_id(self, esdl_id: str) -> Optional[EsdlTimeSeriesInfoDB]:
        """Retrieve the ESDL time series info from the database.

        :param esdl_id: job_id to retrieve the EsdlTimeSeriesInfoDB information for.
        :return: EsdlTimeSeriesInfoDB if it is available in the database.
        """
        LOGGER.debug("Retrieving ESDL time series info for ESDL with id '%s'", esdl_id)
        with session_scope() as session:
            stmnt = select(EsdlTimeSeriesInfoDB).where(EsdlTimeSeriesInfoDB.esdl_id == esdl_id)

            esdl_time_series_info = session.scalar(stmnt)
        return esdl_time_series_info

    def get_all_esdl_time_series_infos(self) -> list[EsdlTimeSeriesInfoDB]:
        """Retrieve a list of all ESDL time series info entries in the database.

        :return: List of all EsdlTimeSeriesInfoDB entries.
        """
        with session_scope() as session:
            stmnt = select(EsdlTimeSeriesInfoDB)
            esdl_time_series_infos = list(session.scalars(stmnt).all())
        return esdl_time_series_infos

    def set_esdl_time_series_data_inactive(self, job_id: str) -> None:
        """Set the time series data to inactive (stale) for the specified job_id.

        Note: if no time series esdl row can be found a new entry will be created to make sure that,
        if a future job result comes in, it will be deleted anyway.

        :param job_id: id of the job to delete the time series for.
        """
        LOGGER.debug("Set time series data to inactive for job with id '%s'", job_id)
        with session_scope() as session:
            find_stmnt = select(EsdlTimeSeriesInfoDB).where(EsdlTimeSeriesInfoDB.job_id == job_id)
            esdl_time_series = session.scalar(find_stmnt)
            if esdl_time_series:
                if esdl_time_series.deactivated_at is None:  # if 'deactivated_at' not already set
                    stmnt = (
                        update(EsdlTimeSeriesInfoDB)
                        .where(EsdlTimeSeriesInfoDB.job_id == job_id)
                        .values(
                            deactivated_at=datetime.now(timezone.utc),
                        )
                    )
                    session.execute(stmnt)
            else:
                new_esdl_time_series_info = EsdlTimeSeriesInfoDB(
                    row_id=uuid.uuid4(),
                    job_id=uuid.UUID(job_id),
                    registered_at=datetime.now(timezone.utc),
                    deactivated_at=datetime.now(timezone.utc),
                )
                session.add(new_esdl_time_series_info)

    def delete_esdl_time_series_info_by_esdl_id(self, output_esdl_id: str) -> bool:
        """Remove ESDL time series info from the database by ESDL id.

        :param output_esdl_id: output ESDL id to delete the time series for.
        :return: True if the time series was removed or False if it was not in the database.
        """
        LOGGER.debug("Deleting ESDL time series info for ESDL with id '%s'", output_esdl_id)
        with session_scope() as session:
            find_stmnt = select(EsdlTimeSeriesInfoDB).where(
                EsdlTimeSeriesInfoDB.esdl_id == output_esdl_id
            )
            esdl_time_series = session.scalar(find_stmnt)
            result = False
            if esdl_time_series:
                session.delete(esdl_time_series)
                result = True

        return result

    def delete_esdl_time_series_info(self, esdl_time_series_info: EsdlTimeSeriesInfoDB) -> None:
        """Remove ESDL time series info from the database.

        :param esdl_time_series_info: id of the job to delete the time series for.
        :return: True if the time series was removed or False if it was not in the database.
        """
        LOGGER.debug(
            "Deleting ESDL time series info with row id '%s'", esdl_time_series_info.row_id
        )
        with session_scope() as session:
            session.delete(esdl_time_series_info)

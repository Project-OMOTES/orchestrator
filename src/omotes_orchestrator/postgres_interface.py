import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta
import logging
from typing import Generator, Optional

from sqlalchemy import select, update, delete, create_engine, orm
from sqlalchemy.orm import Session as SQLSession
from sqlalchemy.engine import Engine, URL

from omotes_orchestrator.db_models.job import JobDB, JobStatus
from omotes_orchestrator.config import PostgreSQLConfig

LOGGER = logging.getLogger("omotes_orchestrator")

session_factory = orm.sessionmaker()
Session = orm.scoped_session(session_factory)


@contextmanager
def session_scope(do_expunge: bool = False) -> Generator[SQLSession, None, None]:
    """Provide a transactional scope around a series of operations.

    Ensures that the session is committed and closed. Exceptions raised within the 'with' block
    using this contextmanager should be handled in the with block itself. They will not be caught
    by the 'except' here.

    :param do_expunge: Expunge the records cached in this session. Set this to True for SELECT
        queries and keep it False for INSERTS or UPDATES.
    :return: A single SQL session.
    """
    try:
        yield Session()

        if do_expunge:
            Session.expunge_all()
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

    Note: The interface may be called from many threads at once. Therefor each query/function
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
        workflow_type: str,
        timeout_after: timedelta,
    ) -> None:
        """Insert a new job into the database.

        Note: Assumption is that the job_id is unique and has not yet been added to the database.

        :param job_id: Unique identifier of the job.
        :param workflow_type: Name of the workflow this job will execute.
        :param timeout_after: Maximum duration before the job is terminated due to timing out.
        """
        with session_scope(do_expunge=False) as session:
            new_job = JobDB(
                job_id=job_id,
                workflow_type=workflow_type,
                status=JobStatus.REGISTERED,
                registered_at=datetime.now(),
                timeout_after_ms=round(timeout_after.total_seconds() * 1000),
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
                    status=JobStatus.SUBMITTED, submitted_at=datetime.now(), celery_id=celery_id
                )
            )
            session.execute(stmnt)

    def set_job_running(self, job_id: uuid.UUID) -> None:
        """Set the status of the job to RUNNING.

        :param job_id: Job to set the status to RUNNING.
        """
        LOGGER.debug("Started job with id '%s'", job_id)
        with session_scope() as session:
            stmnt = (
                update(JobDB)
                .where(JobDB.job_id == job_id)
                .values(status=JobStatus.RUNNING, running_at=datetime.now())
            )
            session.execute(stmnt)

    def get_job_status(self, job_id: uuid.UUID) -> Optional[JobStatus]:
        """Retrieve the current job status.

        :param job_id: Job to retrieve the status for.
        :return: Current job status.
        """
        LOGGER.debug("Retrieving job status for job with id '%s'", job_id)
        with session_scope(do_expunge=True) as session:
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
        with session_scope(do_expunge=True) as session:
            stmnt = select(JobDB.celery_id).where(JobDB.job_id == job_id)
            celery_id = session.scalar(stmnt)
        return celery_id

    def get_job(self, job_id: uuid.UUID) -> Optional[JobDB]:
        """Retrieve the job info from the database.

        :param job_id: Job to retrieve the job information for.
        :return: Job if it is available in the database.
        """
        LOGGER.debug("Retrieving job data for job with id '%s'", job_id)
        with session_scope(do_expunge=True) as session:
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
            job_deleted = self.job_exists(job_id)
            if job_deleted:
                stmnt = delete(JobDB).where(JobDB.job_id == job_id)
                session.execute(stmnt)

        return job_deleted

    def job_exists(self, job_id: uuid.UUID) -> bool:
        """Check if the job exists in the database.

        :param job_id: Check if a job with this id exists.
        :return: True if the job exists, otherwise False.
        """
        LOGGER.debug("Checking if job with id '%s' exists", job_id)
        with session_scope() as session:
            stmnt = select(1).where(JobDB.job_id.job_id)
            job_exists = bool(session.scalar(stmnt))
        return job_exists

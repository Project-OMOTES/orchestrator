from contextlib import contextmanager
from datetime import datetime, timedelta
import logging
from typing import Generator, Optional
from uuid import uuid4

from sqlalchemy import select, update, delete, create_engine, orm
from sqlalchemy.orm import Session as SQLSession
from sqlalchemy.engine import Engine, URL

from omotes_orchestrator.db_models.job import JobDB, JobStatus
from omotes_orchestrator.config import PostgreSQLConfig


LOGGER = logging.getLogger("omotes_orchestrator")

# ALL_JOBS_STMNT = select(JobDB).options(
#     load_only(
#         JobDB.job_id,
#         JobDB.job_name,
#         JobDB.work_flow_type,
#         JobDB.user_name,
#         JobDB.project_name,
#         JobDB.status,
#         JobDB.added_at,
#         JobDB.running_at,
#         JobDB.stopped_at,
#     )
# )


session_factory = orm.sessionmaker()
Session = orm.scoped_session(session_factory)


@contextmanager
def session_scope(do_expunge=False) -> Generator[SQLSession, None, None]:
    """Provide a transactional scope around a series of operations. Ensures that the session is
    committed and closed. Exceptions raised within the 'with' block using this contextmanager
    should be handled in the with block itself. They will not be caught by the 'except' here."""
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


def initialize_db(application_name: str, config: PostgreSQLConfig):
    """
    Initialize the database connection by creating the engine and configuring
    the default session maker.
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
    db_config: PostgreSQLConfig
    engine: Engine

    def __init__(self, postgres_config: PostgreSQLConfig):
        self.db_config = postgres_config

    def start(self):
        self.engine = initialize_db("omotes_orchestrator", self.db_config)

    def stop(self):
        if self.engine:
            self.engine.dispose()

    def put_new_job(
        self,
        job_id: uuid4,
        workflow_type: str,
        timeout_after: timedelta,
    ) -> None:
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

    def set_job_submitted(self, job_id: uuid4, celery_id: str) -> None:
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

    def set_job_running(self, job_id: uuid4) -> None:
        LOGGER.debug("Started job with id '%s'", job_id)
        with session_scope() as session:
            stmnt = (
                update(JobDB)
                .where(JobDB.job_id == job_id)
                .values(status=JobStatus.RUNNING, running_at=datetime.now())
            )
            session.execute(stmnt)

    def get_job_status(self, job_id: uuid4) -> JobStatus:
        LOGGER.debug("Retrieving job status for job with id '%s'", job_id)
        with session_scope(do_expunge=True) as session:
            stmnt = select(JobDB.status).where(JobDB.job_id == job_id)
            job_status = session.scalar(stmnt)
        return job_status

    def get_job_celery_id(self, job_id: uuid4) -> str:
        LOGGER.debug("Retrieving celery id for job with id '%s'", job_id)
        with session_scope(do_expunge=True) as session:
            stmnt = select(JobDB.celery_id).where(JobDB.job_id == job_id)
            celery_id = session.scalar(stmnt)
        return celery_id

    def get_job(self, job_id: uuid4) -> Optional[JobDB]:
        LOGGER.debug("Retrieving job data for job with id '%s'", job_id)
        session: Session
        with session_scope(do_expunge=True) as session:
            stmnt = select(JobDB).where(JobDB.job_id == job_id)
            job = session.scalar(stmnt)
        return job

    def delete_job(self, job_id: uuid4) -> bool:
        LOGGER.debug("Deleting job with id '%s'", job_id)
        session: Session
        with session_scope() as session:
            job_deleted = self.job_exists(job_id)
            if job_deleted:
                stmnt = delete(JobDB).where(JobDB.job_id == job_id)
                session.execute(stmnt)

        return job_deleted

    def job_exists(self, job_id: uuid4) -> bool:
        LOGGER.debug("Checking if job with id '%s' exists", job_id)
        session: Session
        with session_scope() as session:
            stmnt = select(1).where(JobDB.job_id == job_id)
            job_exists = bool(session.scalar(stmnt))
        return job_exists

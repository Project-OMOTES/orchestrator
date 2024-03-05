from contextlib import contextmanager
from datetime import datetime, timedelta
import logging
from typing import List, Generator
from uuid import uuid4

from sqlalchemy import select, update, delete, create_engine, orm
from sqlalchemy.orm import Session as SQLSession
from sqlalchemy.orm.strategy_options import load_only
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
        echo=True,
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
        celery_id: str,
        timeout_after: timedelta,
    ) -> None:
        with session_scope(do_expunge=False) as session:
            new_job = JobDB(
                job_id=job_id,
                celery_id=celery_id,
                status=JobStatus.SUBMITTED,
                registered_at=datetime.now(),
                timeout_after_ms=round(timeout_after.total_seconds() * 1000),
                is_cancelled=False,
            )
            session.add(new_job)
        LOGGER.debug("Job %s is submitted as new job in database", job_id)

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

    def get_job_logs(self, job_id: uuid4) -> str:
        LOGGER.debug("Retrieving job log for job with id '%s'", job_id)
        with session_scope() as session:
            stmnt = select(JobDB.logs).where(JobDB.job_id == job_id)
            job_logs: JobDB = session.scalar(stmnt)
        return job_logs

    def get_job(self, job_id: uuid4) -> JobDB:
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
            stmnt = select(JobDB).where(JobDB.job_id == job_id)
            job = session.scalars(stmnt).all()
            if job:
                stmnt = delete(JobDB).where(JobDB.job_id == job_id)
                session.execute(stmnt)
                job_deleted = True
            else:
                job_deleted = False
        return job_deleted

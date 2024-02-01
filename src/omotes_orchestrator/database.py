import logging
from contextlib import contextmanager
from typing import Generator

from sqlalchemy import create_engine, orm
from sqlalchemy.engine import URL
from sqlalchemy.orm import Session as SQLSession

from nwnsdk import PostgresConfig

LOGGER = logging.getLogger("nwnsdk")

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


def initialize_db(application_name: str, config: PostgresConfig):
    """
    Initialize the database connection by creating the engine and configuring
    the default session maker.
    """
    LOGGER.info("Connecting to PostgresDB at %s:%s as user %s", config.host, config.port, config.user_name)
    url = URL.create(
        "postgresql+psycopg2",
        username=config.user_name,
        password=config.password,
        host=config.host,
        port=config.port,
        database=config.database_name,
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

from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from utils.config import config

# Create base class for declarative models
Base = declarative_base()


class PSQLClient:
    def __init__(self):
        self.engine = create_engine(
            config.psql_uri,
            pool_size=config.db_pool_size,
            max_overflow=config.db_max_overflow,
            pool_timeout=config.db_pool_timeout,
            pool_recycle=config.db_pool_recycle,
            pool_pre_ping=config.db_pool_pre_ping,
        )
        self.SessionLocal = sessionmaker(
            autocommit=False, autoflush=False, bind=self.engine
        )

    def get_session(self):
        """Get a new database session"""
        return self.SessionLocal()

    @contextmanager
    def get_session_context(self):
        """Get a new database session as context manager"""
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def create_all(self):
        """Create all tables in the database"""
        Base.metadata.create_all(bind=self.engine)


# Global instance
db = PSQLClient()


def get_engine():
    """Get the database engine instance. For unit test."""
    return db.engine

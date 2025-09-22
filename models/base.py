"""Simple base service with consistent error handling and database operations."""

import logging
from typing import Callable, TypeVar

from fastapi import HTTPException
from sqlalchemy.exc import SQLAlchemyError

from utils.psql_client import db

logger = logging.getLogger(__name__)

T = TypeVar("T")


def with_db_session(
    operation: Callable, error_message: str = "Database operation failed"
) -> T:
    """Execute operation with database session and consistent error handling."""
    try:
        with db.get_session_context() as session:
            return operation(session)
    except HTTPException:
        # Re-raise HTTP exceptions as-is
        raise
    except SQLAlchemyError as e:
        logger.error(f"{error_message}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"{error_message}: {str(e)}")
    except Exception as e:
        logger.error(f"{error_message}: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


def with_db_transaction(
    operation: Callable, error_message: str = "Transaction failed"
) -> T:
    """Execute operation with database transaction and consistent error handling."""
    try:
        with db.get_session_context() as session:
            result = operation(session)
            session.commit()
            return result
    except HTTPException:
        # Re-raise HTTP exceptions as-is
        raise
    except SQLAlchemyError as e:
        logger.error(f"{error_message}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"{error_message}: {str(e)}")
    except Exception as e:
        logger.error(f"{error_message}: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")

from datetime import UTC, datetime
from typing import Optional

from pydantic import BaseModel
from sqlalchemy import Boolean, Column, DateTime, Index, Integer, String
from sqlalchemy.orm import relationship

from utils.psql_client import Base


# SQLAlchemy Models
class FileDB(Base):
    __tablename__ = "files"

    file_id = Column(String, primary_key=True)
    file_name = Column(String, nullable=False)
    file_hash = Column(String, nullable=False)
    size = Column(Integer, nullable=False)
    status = Column(String, nullable=False, default="processing")
    enabled = Column(Boolean, nullable=False, default=True)
    protective = Column(Boolean, nullable=False, default=False)
    kb_id = Column(String, nullable=False)
    folder_id = Column(String, nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(UTC))
    created_by = Column(String, nullable=False)



# Pydantic Models for API
class File(BaseModel):
    file_id: str
    file_name: str
    file_hash: str
    size: int
    status: str
    enabled: Optional[bool] = True
    protective: Optional[bool] = False
    kb_id: str
    folder_id: Optional[str] = None
    created_at: Optional[datetime] = None
    created_by: str

    model_config = {"from_attributes": True}

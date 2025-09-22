from datetime import UTC, datetime
from typing import Optional

from pydantic import BaseModel
from sqlalchemy import Column, DateTime, Index, String, Boolean
from sqlalchemy.orm import relationship

from utils.psql_client import Base


class FolderDB(Base):
    __tablename__ = "folders"

    folder_id = Column(String, primary_key=True)
    folder_name = Column(String, nullable=False)
    kb_id = Column(String, nullable=False)
    parent_folder_id = Column(String, nullable=True)
    path = Column(String, nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(UTC))
    created_by = Column(String, nullable=False)
    updated_at = Column(DateTime, default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC))
    updated_by = Column(String, nullable=False)
    enabled = Column(Boolean, nullable=False, default=True)


class Folder(BaseModel):
    folder_id: str
    folder_name: str
    kb_id: str
    parent_folder_id: Optional[str] = None
    path: Optional[str] = None
    created_at: Optional[datetime] = None
    created_by: str
    updated_at: Optional[datetime] = None
    updated_by: str
    enabled: Optional[bool] = True

    model_config = {"from_attributes": True}
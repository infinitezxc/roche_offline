"""
Utility functions for creating folders and files in the knowledge base system.
"""

import logging
from datetime import UTC, datetime

from sqlalchemy.orm import Session

from models.folder import FolderDB
from models.file import FileDB
from models.base import with_db_transaction
from utils.id_utils import generate_id

logger = logging.getLogger(__name__)


def delete_all_files_and_folders_in_kb(kb_id: str) -> dict:
    """
    Delete all files and folders in a specific knowledge base.

    Args:
        kb_id: The knowledge base ID to clear

    Returns:
        dict: Statistics about deleted items
    """

    def operation(session: Session) -> dict:
        # Delete all files in the KB
        files_deleted = session.query(FileDB).filter(FileDB.kb_id == kb_id).delete()

        # Delete all folders in the KB
        folders_deleted = session.query(FolderDB).filter(FolderDB.kb_id == kb_id).delete()

        session.commit()

        logger.info(f"Deleted {files_deleted} files and {folders_deleted} folders from KB: {kb_id}")
        return {
            "files_deleted": files_deleted,
            "folders_deleted": folders_deleted,
            "kb_id": kb_id
        }

    return with_db_transaction(operation, f"Error deleting files and folders from KB: {kb_id}")


def create_folders_batch(user_id: str, kb_id: str, folder_names: list[str]) -> dict[str, str]:
    """
    Create multiple folders in batch for a knowledge base.

    Args:
        user_id: The ID of the user creating the folders
        kb_id: The knowledge base ID where folders will be created
        folder_names: List of folder names to create

    Returns:
        dict: Mapping of folder_name -> folder_id for created folders
    """

    def operation(session: Session) -> dict[str, str]:
        folder_mapping = {}
        now = datetime.now(UTC)

        folder_objects = []
        for folder_name in folder_names:
            folder_id = generate_id('folders')
            folder_db = FolderDB(
                folder_id=folder_id,
                folder_name=folder_name,
                kb_id=kb_id,
                parent_folder_id=None,
                path=folder_name,
                created_at=now,
                created_by=user_id,
                updated_at=now,
                updated_by=user_id,
                enabled=True,
            )
            folder_objects.append(folder_db)
            folder_mapping[folder_name] = folder_id

        session.add_all(folder_objects)
        session.commit()

        logger.info(f"Created {len(folder_objects)} folders in KB: {kb_id}")
        return folder_mapping

    return with_db_transaction(operation, "Error creating folders in batch")


def create_files_batch(
    user_id: str,
    kb_id: str,
    file_data: list[dict]
) -> list[str]:
    """
    Create multiple file records in batch for a knowledge base.

    Args:
        user_id: The ID of the user creating the files
        kb_id: The knowledge base ID where files will be created
        file_data: List of dicts with keys: folder_id, file_name, file_hash, file_size, status

    Returns:
        list: List of created file IDs
    """

    def operation(session: Session) -> list[str]:
        file_ids = []
        now = datetime.now(UTC)

        file_objects = []
        for file_info in file_data:
            file_id = generate_id('files')
            file_db = FileDB(
                file_id=file_id,
                file_name=file_info["file_name"],
                file_hash=file_info["file_hash"],
                size=file_info["file_size"],
                status=file_info.get("status", "completed"),
                enabled=True,
                protective=False,
                kb_id=kb_id,
                folder_id=file_info["folder_id"],
                created_at=now,
                created_by=user_id,
            )
            file_objects.append(file_db)
            file_ids.append(file_id)

        session.add_all(file_objects)
        session.commit()

        logger.info(f"Created {len(file_objects)} files in KB: {kb_id}")
        return file_ids

    return with_db_transaction(operation, "Error creating files in batch")
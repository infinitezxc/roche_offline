"""Utility functions for generating standardized IDs with prefixes."""

from enum import Enum
from ulid import ULID


class TablePrefix(Enum):
    """Enum defining table name to ID prefix mapping."""
    USERS = "usr_"
    ORGANIZATIONS = "org_"
    FILES = "file_"
    USER_SESSIONS = "ses_"
    USER_MESSAGES = "msg_"
    USER_TASKS = "tsk_"
    KNOWLEDGE_BASES = "kb_"
    FOLDERS = "fld_"
    TAGS = "tag_"
    USER_GROUPS = "grp_"
    PERMISSIONS = "perm_"


def generate_id(table_name: str) -> str:
    """
    Generate a new ID with appropriate prefix based on table name.
    
    Args:
        table_name: The name of the database table (e.g., 'users', 'organizations')
    
    Returns:
        str: A prefixed ULID (e.g., 'usr_01ARZ3NDEKTSV4RRFFQ69G5FAV')
    
    Raises:
        ValueError: If table_name is not supported
    """
    table_name_upper = table_name.upper()
    
    try:
        prefix_enum = TablePrefix[table_name_upper]
        return prefix_enum.value + str(ULID())
    except KeyError:
        raise ValueError(f"Unsupported table name: {table_name}. Supported tables: {list(TablePrefix.__members__.keys())}")


# Convenience function for backward compatibility and easy access to supported tables
def get_supported_tables() -> list[str]:
    """Return list of supported table names."""
    return [name.lower() for name in TablePrefix.__members__.keys()]


def has_valid_prefix(id_value: str, table_name: str) -> bool:
    """
    Check if an ID has the correct prefix for its table type.
    
    Args:
        id_value: The ID to check
        table_name: The name of the database table
    
    Returns:
        bool: True if ID has correct prefix, False otherwise
    """
    table_name_upper = table_name.upper()
    try:
        prefix_enum = TablePrefix[table_name_upper]
        return id_value.startswith(prefix_enum.value)
    except KeyError:
        return False


def ensure_prefix(id_value: str, table_name: str) -> str:
    """
    Ensure an ID has the correct prefix, adding it if missing (for backward compatibility).
    
    Args:
        id_value: The ID that may or may not have a prefix
        table_name: The name of the database table
    
    Returns:
        str: ID with correct prefix
    
    Raises:
        ValueError: If table_name is not supported
    """
    if has_valid_prefix(id_value, table_name):
        return id_value
    
    table_name_upper = table_name.upper()
    try:
        prefix_enum = TablePrefix[table_name_upper]
        return prefix_enum.value + id_value
    except KeyError:
        raise ValueError(f"Unsupported table name: {table_name}. Supported tables: {list(TablePrefix.__members__.keys())}")



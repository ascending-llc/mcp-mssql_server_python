import re
from typing import List
from mssql_mcp_server.utils.exceptions import ValidationError


class SQLValidator:
    """SQL query validation utilities."""

    DANGEROUS_KEYWORDS = [
        'DROP', 'DELETE', 'TRUNCATE', 'ALTER', 'CREATE', 'EXEC', 'EXECUTE',
        'SHUTDOWN', 'BACKUP', 'RESTORE', 'DBCC', 'BULK', 'OPENROWSET'
    ]

    @classmethod
    def validate_table_name(cls, table_name: str, valid_tables: List[str]) -> bool:
        """Validate table name against list of valid tables."""
        if not table_name:
            raise ValidationError("Table name cannot be empty")

        if table_name not in valid_tables:
            raise ValidationError(f"Table '{table_name}' not found in database")

        return True

    @classmethod
    def validate_object_name(cls, object_name: str, valid_objects: List[str], object_type: str = "table") -> bool:
        """Validate object name against list of valid objects (tables or views)."""
        if not object_name:
            raise ValidationError(f"{object_type.title()} name cannot be empty")

        if object_name not in valid_objects:
            raise ValidationError(f"{object_type.title()} '{object_name}' not found in database")

        return True

    @classmethod
    def validate_sql_query(cls, query: str, allow_modifications: bool = False) -> bool:
        """Validate SQL query for safety."""
        if not query or not query.strip():
            raise ValidationError("Query cannot be empty")

        query_upper = query.upper().strip()

        if not allow_modifications:
            for keyword in cls.DANGEROUS_KEYWORDS:
                if keyword in query_upper:
                    raise ValidationError(f"Query contains dangerous keyword: {keyword}")

        return True

    @classmethod
    def sanitize_identifier(cls, identifier: str) -> str:
        """Sanitize SQL identifier (table/column names)."""
        # Remove any characters that aren't alphanumeric or underscore
        sanitized = re.sub(r'[^\w]', '', identifier)

        if not sanitized:
            raise ValidationError("Invalid identifier")

        return sanitized

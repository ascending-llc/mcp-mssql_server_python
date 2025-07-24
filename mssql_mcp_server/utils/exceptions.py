"""Custom exceptions for MSSQL MCP Server."""


class MSSQLMCPError(Exception):
    """Base exception for MSSQL MCP Server."""
    pass


class ConfigurationError(MSSQLMCPError):
    """Raised when configuration is invalid or missing."""
    pass


class DatabaseConnectionError(MSSQLMCPError):
    """Raised when database connection fails."""
    pass


class DatabaseOperationError(MSSQLMCPError):
    """Raised when database operation fails."""
    pass


class ValidationError(MSSQLMCPError):
    """Raised when data validation fails."""
    pass

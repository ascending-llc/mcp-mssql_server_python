import logging
import sys
from typing import Optional
from mssql_mcp_server.config.settings import settings


class Logger:
    """Logger configuration and management."""

    _instance: Optional[logging.Logger] = None

    @classmethod
    def get_logger(cls, name: str = "mssql_mcp_server") -> logging.Logger:
        """Get configured logger instance."""
        if cls._instance is None:
            cls._instance = cls._setup_logger(name)
        return cls._instance

    @staticmethod
    def _setup_logger(name: str) -> logging.Logger:
        """Setup and configure logger."""
        logger = logging.getLogger(name)

        # Remove existing handlers to avoid duplicates
        if logger.handlers:
            logger.handlers.clear()

        # Set log level
        log_level = getattr(logging, settings.server.log_level.upper(), logging.INFO)
        logger.setLevel(log_level)

        # Create console handler
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(log_level)

        # Create formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)

        # Add handler to logger
        logger.addHandler(handler)

        return logger

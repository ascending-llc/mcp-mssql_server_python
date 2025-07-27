import os
from dataclasses import dataclass
from typing import Optional
from dotenv import load_dotenv
from mssql_mcp_server.utils.exceptions import ConfigurationError

load_dotenv()


@dataclass
class DatabaseConfig:
    """Database configuration settings."""

    driver: str
    host: str
    user: str
    password: str
    database: str
    trusted_server_certificate: str
    trusted_connection: str
    timeout: int = 60

    @property
    def connection_string(self) -> str:
        """Generate ODBC connection string."""
        return (
            f"Driver={self.driver};"
            f"Server={self.host};"
            f"UID={self.user};"
            f"PWD={self.password};"
            f"Database={self.database};"
            f"TrustServerCertificate={self.trusted_server_certificate};"
            f"Trusted_Connection={self.trusted_connection};"
            f"Timeout={self.timeout};"
        )


@dataclass
class AsyncDatabaseConfig:
    """Async database configuration settings."""

    driver: str
    host: str
    user: str
    password: str
    database: str
    trusted_server_certificate: str
    trusted_connection: str
    timeout: int = 30
    pool_min_size: int = 2
    pool_max_size: int = 10

    @property
    def connection_string(self) -> str:
        """Generate ODBC connection string for async operations."""
        return (
            f"Driver={self.driver};"
            f"Server={self.host};"
            f"UID={self.user};"
            f"PWD={self.password};"
            f"Database={self.database};"
            f"TrustServerCertificate={self.trusted_server_certificate};"
            f"Trusted_Connection={self.trusted_connection};"
            f"Timeout={self.timeout};"
        )


@dataclass
class CacheConfig:
    """Cache configuration settings."""

    enabled: bool = True
    default_ttl: int = 300  # 5 minutes
    table_names_ttl: int = 600  # 10 minutes
    table_data_ttl: int = 120  # 2 minutes
    table_schema_ttl: int = 600  # 10 minutes
    max_entries: int = 1000


@dataclass
class ServerConfig:
    """Server configuration settings."""

    transport: str = "stdio"  # or "tcp"
    host: str = "127.0.0.1"  # Server host binding
    log_level: str = "INFO"  # Options: DEBUG, INFO, WARNING, ERROR, CRITICAL
    max_rows_limit: int = 10000
    mcp_port: int = 8000
    enable_async: bool = True
    enable_dynamic_resources: bool = True


class Settings:
    """Application settings manager."""

    def __init__(self):
        self._database_config: Optional[DatabaseConfig] = None
        self._async_database_config: Optional[AsyncDatabaseConfig] = None
        self._cache_config: Optional[CacheConfig] = None
        self._server_config: Optional[ServerConfig] = None

    @property
    def database(self) -> DatabaseConfig:
        """Get database configuration."""
        if self._database_config is None:
            self._database_config = self._load_database_config()
        return self._database_config

    @property
    def async_database(self) -> AsyncDatabaseConfig:
        """Get async database configuration."""
        if self._async_database_config is None:
            self._async_database_config = self._load_async_database_config()
        return self._async_database_config

    @property
    def cache(self) -> CacheConfig:
        """Get cache configuration."""
        if self._cache_config is None:
            self._cache_config = self._load_cache_config()
        return self._cache_config

    @property
    def server(self) -> ServerConfig:
        """Get server configuration."""
        if self._server_config is None:
            self._server_config = self._load_server_config()
        return self._server_config

    def _load_database_config(self) -> DatabaseConfig:
        """Load database configuration from environment variables."""
        required_vars = ["MSSQL_USER", "MSSQL_PASSWORD", "MSSQL_DATABASE"]
        missing_vars = [var for var in required_vars if not os.getenv(var)]

        if missing_vars:
            raise ConfigurationError(
                f"Missing required environment variables: {', '.join(missing_vars)}"
            )

        return DatabaseConfig(
            driver=os.getenv("MSSQL_DRIVER", "SQL Server"),
            host=os.getenv("MSSQL_HOST", "localhost"),
            user=os.getenv("MSSQL_USER"),
            password=os.getenv("MSSQL_PASSWORD"),
            database=os.getenv("MSSQL_DATABASE"),
            trusted_server_certificate=os.getenv("TRUST_SERVER_CERTIFICATE", "yes"),
            trusted_connection=os.getenv("TRUSTED_CONNECTION", "no"),
            timeout=int(os.getenv("DB_TIMEOUT", "60"))
        )

    def _load_async_database_config(self) -> AsyncDatabaseConfig:
        """Load async database configuration from environment variables."""
        required_vars = ["MSSQL_USER", "MSSQL_PASSWORD", "MSSQL_DATABASE"]
        missing_vars = [var for var in required_vars if not os.getenv(var)]

        if missing_vars:
            raise ConfigurationError(
                f"Missing required environment variables: {', '.join(missing_vars)}"
            )

        return AsyncDatabaseConfig(
            driver=os.getenv("MSSQL_DRIVER", "ODBC Driver 18 for SQL Server"),
            host=os.getenv("MSSQL_HOST", "localhost"),
            user=os.getenv("MSSQL_USER"),
            password=os.getenv("MSSQL_PASSWORD"),
            database=os.getenv("MSSQL_DATABASE"),
            trusted_server_certificate=os.getenv("TRUST_SERVER_CERTIFICATE", "yes"),
            trusted_connection=os.getenv("TRUSTED_CONNECTION", "no"),
            timeout=int(os.getenv("ASYNC_DB_TIMEOUT", "120")),
            pool_min_size=int(os.getenv("DB_POOL_MIN_SIZE", "2")),
            pool_max_size=int(os.getenv("DB_POOL_MAX_SIZE", "10"))
        )

    def _load_cache_config(self) -> CacheConfig:
        """Load cache configuration from environment variables."""
        return CacheConfig(
            enabled=os.getenv("CACHE_ENABLED", "true").lower() == "true",
            default_ttl=int(os.getenv("CACHE_DEFAULT_TTL", "300")),
            table_names_ttl=int(os.getenv("CACHE_TABLE_NAMES_TTL", "600")),
            table_data_ttl=int(os.getenv("CACHE_TABLE_DATA_TTL", "120")),
            table_schema_ttl=int(os.getenv("CACHE_TABLE_SCHEMA_TTL", "600")),
            max_entries=int(os.getenv("CACHE_MAX_ENTRIES", "1000"))
        )

    def _load_server_config(self) -> ServerConfig:
        """Load server configuration from environment variables."""
        return ServerConfig(
            transport=os.getenv("FASTMCP_TRANSPORT", "stdio"),
            host=os.getenv("FASTMCP_HOST", "127.0.0.1"),
            log_level=os.getenv("FASTMCP_LOG_LEVEL", "INFO"),
            max_rows_limit=int(os.getenv("MAX_ROWS_LIMIT", "100")),
            enable_async=os.getenv("ENABLE_ASYNC", "true").lower() == "true",
            enable_dynamic_resources=os.getenv("ENABLE_DYNAMIC_RESOURCES", "true").lower() == "true",
            mcp_port=int(os.getenv("FASTMCP_PORT", "8000"))
        )


settings = Settings()

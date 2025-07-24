import asyncio
from contextlib import asynccontextmanager
from typing import Optional, AsyncGenerator
import aioodbc

from mssql_mcp_server.config.settings import settings
from mssql_mcp_server.utils.logger import Logger
from mssql_mcp_server.utils.exceptions import DatabaseConnectionError

logger = Logger.get_logger(__name__)


class AsyncDatabasePool:
    """Async database connection pool manager."""

    def __init__(self):
        self._pool: Optional[aioodbc.Pool] = None
        self._initialized = False

    async def initialize(self) -> None:
        """Initialize the connection pool."""
        if self._initialized:
            logger.warning("Connection pool is already initialized")
            return

        try:
            config = settings.async_database
            logger.info(f"Initializing async connection pool with {config.pool_min_size}-{config.pool_max_size} connections")
            
            self._pool = await aioodbc.create_pool(
                dsn=config.connection_string,
                minsize=config.pool_min_size,
                maxsize=config.pool_max_size,
                timeout=config.timeout,
            )
            
            self._initialized = True
            logger.info("Async connection pool initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize connection pool: {e}")
            raise DatabaseConnectionError(f"Failed to initialize connection pool: {e}")

    async def close(self) -> None:
        """Close the connection pool."""
        if self._pool and self._initialized:
            try:
                self._pool.close()
                await self._pool.wait_closed()
                self._initialized = False
                logger.info("Connection pool closed successfully")
            except Exception as e:
                logger.error(f"Error closing connection pool: {e}")

    @asynccontextmanager
    async def get_connection(self) -> AsyncGenerator[aioodbc.Connection, None]:
        """Get a connection from the pool."""
        if not self._initialized or not self._pool:
            raise DatabaseConnectionError("Connection pool not initialized")

        connection = None
        try:
            logger.debug("Acquiring connection from pool")
            connection = await self._pool.acquire()
            logger.debug("Connection acquired successfully")
            yield connection
            
        except Exception as e:
            logger.error(f"Error with database connection: {e}")
            raise DatabaseConnectionError(f"Database connection error: {e}")
            
        finally:
            if connection:
                try:
                    await self._pool.release(connection)
                    logger.debug("Connection released back to pool")
                except Exception as e:
                    logger.warning(f"Error releasing connection: {e}")

    async def test_connection(self) -> bool:
        """Test if connection pool is working."""
        try:
            async with self.get_connection() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute("SELECT 1")
                    result = await cursor.fetchone()
                    return result is not None
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False

    @property
    def is_initialized(self) -> bool:
        """Check if pool is initialized."""
        return self._initialized

    @property
    def pool_info(self) -> dict:
        """Get pool information."""
        if not self._pool:
            return {"status": "not_initialized"}
        
        return {
            "status": "initialized" if self._initialized else "closed",
            "size": self._pool.size,
            "used": self._pool.size - self._pool.freesize,
            "free": self._pool.freesize,
            "minsize": self._pool.minsize,
            "maxsize": self._pool.maxsize
        }


# Global connection pool instance
_connection_pool: Optional[AsyncDatabasePool] = None


async def get_pool() -> AsyncDatabasePool:
    """Get the global connection pool instance."""
    global _connection_pool
    
    if _connection_pool is None:
        _connection_pool = AsyncDatabasePool()
        await _connection_pool.initialize()
    
    return _connection_pool


async def close_pool() -> None:
    """Close the global connection pool."""
    global _connection_pool
    
    if _connection_pool:
        await _connection_pool.close()
        _connection_pool = None 
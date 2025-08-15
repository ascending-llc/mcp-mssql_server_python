#!/usr/bin/env python3
import asyncio
from typing import Optional
from dotenv import load_dotenv
from fastapi import FastAPI, Query
from fastmcp import FastMCP
from fastmcp import Context
import uvicorn
from mssql_mcp_server.config.settings import settings
from mssql_mcp_server.database.async_connection import get_pool, close_pool
from mssql_mcp_server.handlers.async_resources import AsyncResourceHandlers
from mssql_mcp_server.handlers.async_tools import AsyncToolHandlers
from mssql_mcp_server.utils.logger import Logger
from mssql_mcp_server.utils.cache import cache_manager

load_dotenv()

logger = Logger.get_logger(__name__)

app = FastAPI()
mcp = FastMCP(name="mssql_mcp_server")


@app.get("/health")
async def health_check(timeout: Optional[int] = Query(default=None, description="睡眠时间（秒），每5秒打印一次日志。如果为空则直接返回结果")):
    """
    Health check endpoint for the FastAPI application.
    
    Args:
        timeout: 睡眠时间（秒），每5秒打印一次日志。如果为空则直接返回结果
        
    Returns:
        Health status response
    """
    start_time = asyncio.get_event_loop().time()
    
    # 如果timeout为空，直接返回结果
    if timeout is None:
        logger.info("健康检查请求 - 立即返回")
        return {"status": "ok"}
    
    logger.info(f"健康检查开始，超时时间: {timeout} 秒")
    
    if timeout > 0:
        elapsed = 0
        while elapsed < timeout:
            # 计算剩余时间
            remaining = timeout - elapsed
            # 等待时间：取剩余时间和5秒的较小值
            wait_time = min(5, remaining)
            
            await asyncio.sleep(wait_time)
            elapsed = int(asyncio.get_event_loop().time() - start_time)
            
            if elapsed < timeout:
                logger.info(f"健康检查运行中... 已用时: {elapsed} 秒，剩余: {timeout - elapsed} 秒")
    
    total_time = int(asyncio.get_event_loop().time() - start_time)
    logger.info(f"健康检查完成，总耗时: {total_time} 秒")
    
    return {
        "status": "ok",
        "timeout": timeout,
        "actual_duration": total_time
    }


# Static database-level resources
@mcp.resource("mssql://database/tables")
async def get_database_tables() -> str:
    """List all tables in the database."""
    try:
        logger.info("Listing database tables")
        return await AsyncResourceHandlers.list_database_tables()
    except Exception as e:
        logger.error(f"Error listing tables: {e}")
        return f"Error: {str(e)}"


@mcp.resource("mssql://database/views")
async def get_database_views() -> str:
    """List all views in the database."""
    try:
        logger.info("Listing database views")
        return await AsyncResourceHandlers.list_database_views()
    except Exception as e:
        logger.error(f"Error listing views: {e}")
        return f"Error: {str(e)}"


@mcp.resource("mssql://database/info")
async def get_database_info_resource() -> str:
    """Get general database information."""
    try:
        logger.info("Getting database info")
        return await AsyncResourceHandlers.get_database_info()
    except Exception as e:
        logger.error(f"Error getting database info: {e}")
        return f"Error: {str(e)}"


async def register_table_and_view_resources():
    """Dynamically register resources for each table and view."""
    try:
        # Get table and view names
        from mssql_mcp_server.database.async_operations import AsyncDatabaseOperations
        table_and_view_data = await AsyncDatabaseOperations.get_all_table_and_view_names()
        table_names = table_and_view_data["tables"]
        view_names = table_and_view_data["views"]

        logger.info(f"Registering resources for {len(table_names)} tables and {len(view_names)} views...")

        def create_object_data_resource(object_name: str, object_type: str):
            """Factory function to create object data resource."""
            schema, name = object_name.split('.', 1)
            limit = 100  # Default limit for data retrieval

            @mcp.resource(f"mssql://{object_type}/{schema}/{name}/data",
                          name=f"{object_type.title()} Data: {object_name}",
                          description=f"Data from {object_type} {object_name} (top {limit} rows)")
            async def get_object_data_func():
                try:
                    logger.info(f"Reading {object_type} data: {object_name}")
                    return await AsyncResourceHandlers.read_object_data(object_name, object_type, limit)
                except Exception as e:
                    logger.error(f"Error reading {object_type} data {object_name}: {e}")
                    return f"Error: {str(e)}"

            return get_object_data_func

        def create_object_schema_resource(object_name: str, object_type: str):
            """Factory function to create object schema resource."""
            schema, name = object_name.split('.', 1)

            @mcp.resource(f"mssql://{object_type}/{schema}/{name}/schema",
                          name=f"{object_type.title()} Schema: {object_name}",
                          description=f"Schema information for {object_type} {object_name}")
            async def get_object_schema_func():
                try:
                    logger.info(f"Reading {object_type} schema: {object_name}")
                    return await AsyncResourceHandlers.read_object_schema(object_name, object_type)
                except Exception as e:
                    logger.error(f"Error reading {object_type} schema {object_name}: {e}")
                    return f"Error: {str(e)}"

            return get_object_schema_func

        # Register resources for each table
        for table_name in table_names:
            create_object_data_resource(table_name, "table")
            create_object_schema_resource(table_name, "table")

        # Register resources for each view
        for view_name in view_names:
            create_object_data_resource(view_name, "view")
            create_object_schema_resource(view_name, "view")

        total_resources = (len(table_names) + len(view_names)) * 2  # 2 resources per object (data + schema)
        logger.info(
            f"Successfully registered {total_resources} resources ({len(table_names)} tables, {len(view_names)} views)")
        return total_resources

    except Exception as e:
        logger.error(f"Failed to register table and view resources: {e}")
        return 0


@mcp.tool()
async def execute_sql(query: str, allow_modifications: bool = False, ctx: Optional[Context] = None) -> str:
    """
    Execute an SQL query on the MSSQL server.
    
    Args:
        query: The SQL query to execute
        allow_modifications: Whether to allow modification queries (default: false)
        ctx: Optional context to use for executing queries (default: None)
    Returns:
        Query results or execution status
    """
    try:
        logger.info(f"Executing SQL: {query[:100]}...")
        return await AsyncToolHandlers.execute_sql(query, allow_modifications)
    except Exception as e:
        logger.error(f"Error executing SQL: {e}")
        return f"Error: {str(e)}"


@mcp.tool(enabled=False)
async def get_table_schema(table_name: str) -> str:
    """
    Get schema information for a specific table.
    
    Args:
        table_name: Name of the table to describe
    
    Returns:
        Table schema information
    """
    try:
        logger.info(f"Getting schema for table: {table_name}")
        return await AsyncToolHandlers.get_table_schema(table_name)
    except Exception as e:
        logger.error(f"Error getting table schema: {e}")
        return f"Error: {str(e)}"


@mcp.tool(enabled=False)
async def list_tables() -> str:
    """
    Get a list of all tables in the database.
    
    Returns:
        List of table names
    """
    try:
        logger.info("Listing tables")
        table_list = await AsyncToolHandlers.list_tables()
        return "\n".join(table_list) if isinstance(table_list, list) else str(table_list)
    except Exception as e:
        logger.error(f"Error listing tables: {e}")
        return f"Error: {str(e)}"


@mcp.tool(enabled=False)
async def get_table_data(table_name: str, limit: int = None) -> str:
    """
    Get data from a specific table.
    
    Args:
        table_name: Name of the table to read from
        limit: Maximum number of rows to return (optional)
    
    Returns:
        Table data in CSV format
    """
    try:
        logger.info(f"Getting data from table: {table_name}")
        return await AsyncToolHandlers.get_table_data(table_name, limit)
    except Exception as e:
        logger.error(f"Error getting table data: {e}")
        return f"Error: {str(e)}"


@mcp.tool(enabled=False)
async def test_connection() -> str:
    """
    Test the database connection and get connection info.
    
    Returns:
        Connection status and database information
    """
    try:
        logger.info("Testing database connection")
        return await AsyncToolHandlers.test_connection()
    except Exception as e:
        logger.error(f"Error testing connection: {e}")
        return f"Error: {str(e)}"


@mcp.tool(enabled=False)
async def get_database_info() -> str:
    """
    Get comprehensive database information.
    
    Returns:
        Database information in JSON format
    """
    try:
        logger.info("Getting database information")
        return await AsyncToolHandlers.get_database_info()
    except Exception as e:
        logger.error(f"Error getting database info: {e}")
        return f"Error: {str(e)}"


@mcp.tool(enabled=False)
async def clear_cache(pattern: str = "") -> str:
    """
    Clear cache entries.
    
    Args:
        pattern: Pattern to match for selective clearing (optional)
    
    Returns:
        Status message
    """
    try:
        logger.info(f"Clearing cache with pattern: '{pattern}'")
        return await AsyncToolHandlers.clear_cache(pattern)
    except Exception as e:
        logger.error(f"Error clearing cache: {e}")
        return f"Error: {str(e)}"


@mcp.tool(enabled=False)
async def invalidate_table_cache(table_name: str = None) -> str:
    """
    Invalidate cache for specific table or all tables.
    
    Args:
        table_name: Name of the table to invalidate cache for (optional, clears all if not specified)
    
    Returns:
        Status message
    """
    try:
        logger.info(f"Invalidating cache for table: {table_name if table_name else 'all tables'}")
        return await AsyncToolHandlers.invalidate_table_cache(table_name)
    except Exception as e:
        logger.error(f"Error invalidating cache: {e}")
        return f"Error: {str(e)}"


async def initialize_server() -> None:
    """Initialize server components."""
    try:
        logger.info("Initializing MSSQL MCP server...")

        # Display configuration
        logger.info(f"Server configuration:")
        logger.info(f"  - Database: {settings.async_database.host}/{settings.async_database.database}")
        logger.info(f"  - Pool size: {settings.async_database.pool_min_size}-{settings.async_database.pool_max_size}")
        logger.info(f"  - Cache enabled: {settings.cache.enabled}")

        # Initialize connection pool
        pool = await get_pool()
        logger.info(f"Connection pool status: {pool.pool_info}")

        # Test connection
        connection_ok = await pool.test_connection()
        if not connection_ok:
            raise RuntimeError("Database connection test failed")

        # Start cache cleanup task
        if settings.cache.enabled:
            await cache_manager.start_cleanup_task()
            logger.info("Cache cleanup task started")

        # Warm up caches by pre-loading table and view names
        from mssql_mcp_server.database.async_operations import AsyncDatabaseOperations
        table_and_view_data = await AsyncDatabaseOperations.get_all_table_and_view_names()
        table_names = table_and_view_data["tables"]
        view_names = table_and_view_data["views"]
        logger.info(f"Pre-loaded {len(table_names)} table names and {len(view_names)} view names into cache")

        # Dynamically register resources for each table and view
        total_resources = await register_table_and_view_resources()
        logger.info(f"Server will expose {total_resources} dynamic resources")
        logger.info("Server initialization completed successfully")

    except Exception as e:
        logger.error(f"Server initialization failed: {e}")
        raise


async def start_fastapi_server():
    """Start the FastAPI server."""
    try:
        fastapi_port = settings.server.fastapi_port
        host = settings.server.host
        logger.info(f"Starting FastAPI server on {host}:{fastapi_port}")
        
        config = uvicorn.Config(
            app=app,
            host=host,
            port=fastapi_port,
            log_level=settings.server.log_level.lower(),
            access_log=True
        )
        server = uvicorn.Server(config)
        await server.serve()
        
    except Exception as e:
        logger.error(f"FastAPI server error: {e}", exc_info=True)
        raise


async def start_mcp_server():
    """Start the MCP server."""
    try:
        transport = settings.server.transport
        host = settings.server.host
        port = settings.server.mcp_port
        logger.info(f"Starting MCP server with transport: {transport}")

        if transport in ["http", "tcp", "sse"]:
            logger.info(f"Using host: {host}, port: {port}")
            await mcp.run_async(transport=transport, host=host, port=port, uvicorn_config={
                "workers": 1,
                "timeout_keep_alive": 300,
                "timeout_notify": 300,
                "backlog": 2048,
                "limit_concurrency": 50,
                "limit_max_requests": None,
            })
        else:
            logger.info(f"Using {transport} transport")
            await mcp.run_async(transport=transport)
            
    except Exception as e:
        logger.error(f"MCP server error: {e}", exc_info=True)
        raise


async def cleanup_server() -> None:
    """Clean up server resources."""
    try:
        logger.info("Cleaning up server resources...")

        # Stop cache cleanup task
        if settings.cache.enabled:
            await cache_manager.stop_cleanup_task()

        # Close connection pool
        await close_pool()

        logger.info("Server cleanup completed")

    except Exception as e:
        logger.error(f"Error during server cleanup: {e}")


async def main():
    """Main entry point to run both MCP and FastAPI servers concurrently."""
    try:
        # Initialize all components
        await initialize_server()

        # Start both servers concurrently
        logger.info("Starting both MCP and FastAPI servers concurrently...")
        
        # Create tasks for both servers
        tasks = []
        
        # Always start FastAPI server
        fastapi_task = asyncio.create_task(start_fastapi_server())
        tasks.append(fastapi_task)
        
        # Start MCP server if not using stdio transport
        transport = settings.server.transport
        if transport != "stdio":
            mcp_task = asyncio.create_task(start_mcp_server())
            tasks.append(mcp_task)
        else:
            logger.info("MCP server using stdio transport - starting inline")
            await start_mcp_server()
            return
        
        # Wait for any task to complete or fail
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        
        # Cancel pending tasks
        for task in pending:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        
        # Check if any task failed
        for task in done:
            if task.exception():
                raise task.exception()

    except KeyboardInterrupt:
        logger.info("Server shutdown requested by user")
    except Exception as e:
        logger.error(f"Server error: {e}", exc_info=True)
        raise
    finally:
        await cleanup_server()


if __name__ == "__main__":
    """
    Run the MSSQL MCP server.
    
    Environment variables for configuration:
    - CACHE_ENABLED=true/false (default: true)
    - CACHE_TABLE_NAMES_TTL=600 (10 minutes)
    - CACHE_TABLE_DATA_TTL=120 (2 minutes) 
    - CACHE_TABLE_SCHEMA_TTL=600 (10 minutes)
    - DB_POOL_MIN_SIZE=2 (minimum connections)
    - DB_POOL_MAX_SIZE=10 (maximum connections)
    - ENABLE_ASYNC=true/false (default: true)
    - ENABLE_DYNAMIC_RESOURCES=true/false (default: true)
    """

    # Display startup banner
    logger.info("🚀 MSSQL MCP Server")
    logger.info("━" * 40)
    logger.info("Features enabled:")
    logger.info(f"  ⚡ Async operations: {settings.server.enable_async}")
    logger.info(f"  🔄 Smart caching: {settings.cache.enabled}")
    logger.info(f"  🎯 Dynamic resources: {settings.server.enable_dynamic_resources}")
    logger.info(
        f"  🏊 Connection pooling: {settings.async_database.pool_min_size}-{settings.async_database.pool_max_size}")
    logger.info("━" * 40)

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("\n👋 Server shutdown complete")
    except Exception as e:
        logger.error(f"\n❌ Server failed to start: {e}")
        exit(1)

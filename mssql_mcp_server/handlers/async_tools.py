import json
import asyncio
from typing import List
from fastmcp.server.dependencies import get_context
from mssql_mcp_server.database.async_operations import AsyncDatabaseOperations
from mssql_mcp_server.config.settings import settings
from mssql_mcp_server.utils.logger import Logger
from mssql_mcp_server.utils.exceptions import DatabaseOperationError

logger = Logger.get_logger(__name__)


class AsyncToolHandlers:
    """Async MCP tool handlers."""

    @staticmethod
    async def execute_sql(query: str, allow_modifications: bool = False) -> str:
        """Execute an SQL query on the MSSQL server with optional timeout."""
        try:
            logger.info(f"Executing SQL query: {query[:100]}...")
            ctx = get_context()
            result = await AsyncDatabaseOperations.execute_query(query, allow_modifications)

            if result.query_type in ["select", "show_tables", "cached_select"]:
                if result.row_count == 0:
                    return "Query executed successfully but returned no results."
                csv_data = result.to_csv()
                logger.info(f"Query returned {result.row_count} rows in {result.execution_time:.3f}s")
                await ctx.report_progress(progress=result.row_count, total=result.row_count,
                                          message="Query executed successfully, length: {result.row_count}")
                return csv_data

            elif result.query_type == "modification":
                message = f"Query executed successfully. Rows affected: {result.row_count}"
                if result.execution_time > 0:
                    message += f" (Execution time: {result.execution_time:.3f}s)"
                return message

            else:
                return "Query executed successfully."

        except asyncio.TimeoutError:
            error_msg = f"Query execution timed out after {timeout_seconds} seconds"
            logger.error(error_msg)
            return error_msg
        except DatabaseOperationError as e:
            error_msg = f"Database error executing query: {str(e)}"
            logger.error(error_msg)
            return error_msg
        except Exception as e:
            error_msg = f"Unexpected error executing query: {str(e)}"
            logger.error(error_msg)
            return error_msg

    @staticmethod
    async def get_table_schema(table_name: str) -> str:
        """Get the schema information for a specific table."""
        try:
            logger.info(f"Getting schema for table: {table_name}")

            schema_info = await AsyncDatabaseOperations.get_table_schema(table_name)

            if not schema_info:
                return f"No schema information found for table '{table_name}'"

            # Format schema information as CSV
            headers = [
                "Column Name", "Data Type", "Is Nullable", "Default Value",
                "Max Length", "Precision", "Scale"
            ]
            result_lines = [",".join(headers)]

            for col in schema_info:
                row = [
                    col["column_name"],
                    col["data_type"],
                    col["is_nullable"],
                    str(col["default_value"]) if col["default_value"] is not None else "",
                    str(col["max_length"]) if col["max_length"] is not None else "",
                    str(col["numeric_precision"]) if col["numeric_precision"] is not None else "",
                    str(col["numeric_scale"]) if col["numeric_scale"] is not None else ""
                ]
                result_lines.append(",".join(row))

            return "\n".join(result_lines)

        except DatabaseOperationError as e:
            error_msg = f"Database error getting schema for table '{table_name}': {str(e)}"
            logger.error(error_msg)
            return error_msg
        except Exception as e:
            error_msg = f"Unexpected error getting schema for table '{table_name}': {str(e)}"
            logger.error(error_msg)
            return error_msg

    @staticmethod
    async def list_tables() -> List[str]:
        """Get a list of all tables in the database."""
        try:
            table_names = await AsyncDatabaseOperations.get_table_names()
            logger.info(f"Found {len(table_names)} tables")
            return table_names

        except DatabaseOperationError as e:
            logger.error(f"Error listing tables: {str(e)}")
            return [f"Error: {str(e)}"]
        except Exception as e:
            logger.error(f"Unexpected error listing tables: {str(e)}")
            return [f"Unexpected error: {str(e)}"]

    @staticmethod
    async def get_table_data(table_name: str, limit: int = None) -> str:
        """Get data from a specific table."""
        try:
            if limit is None:
                limit = settings.server.max_rows_limit

            logger.info(f"Getting data from table: {table_name} (limit: {limit})")

            result = await AsyncDatabaseOperations.get_table_data(table_name, limit)

            if result.row_count == 0:
                return f"Table '{table_name}' is empty."

            csv_data = result.to_csv()
            logger.info(f"Retrieved {result.row_count} rows from table {table_name} in {result.execution_time:.3f}s")
            return csv_data

        except DatabaseOperationError as e:
            error_msg = f"Database error getting data from table '{table_name}': {str(e)}"
            logger.error(error_msg)
            return error_msg
        except Exception as e:
            error_msg = f"Unexpected error getting data from table '{table_name}': {str(e)}"
            logger.error(error_msg)
            return error_msg

    @staticmethod
    async def test_connection() -> str:
        """Test the database connection."""
        try:
            logger.info("Testing database connection")

            is_connected = await AsyncDatabaseOperations.test_connection()

            if is_connected:
                # Get additional connection info
                db_info = await AsyncDatabaseOperations.get_database_info()
                return json.dumps({
                    "status": "connected",
                    "message": "Database connection successful",
                    "database_info": db_info
                }, indent=2)
            else:
                return json.dumps({
                    "status": "failed",
                    "message": "Database connection failed"
                }, indent=2)

        except Exception as e:
            error_msg = f"Connection test failed: {str(e)}"
            logger.error(error_msg)
            return json.dumps({
                "status": "error",
                "message": error_msg
            }, indent=2)

    @staticmethod
    async def get_database_info() -> str:
        """Get comprehensive database information."""
        try:
            logger.info("Getting database information")

            db_info = await AsyncDatabaseOperations.get_database_info()
            return json.dumps(db_info, indent=2)

        except DatabaseOperationError as e:
            error_msg = f"Database error getting info: {str(e)}"
            logger.error(error_msg)
            return json.dumps({"error": error_msg}, indent=2)
        except Exception as e:
            error_msg = f"Unexpected error getting database info: {str(e)}"
            logger.error(error_msg)
            return json.dumps({"error": error_msg}, indent=2)

    @staticmethod
    async def clear_cache(pattern: str = "") -> str:
        """Clear cache entries."""
        try:
            from mssql_mcp_server.utils.cache import cache_manager

            logger.info(f"Clearing cache with pattern: '{pattern}'")

            if pattern:
                # Clear specific pattern
                cleared_count = 0
                for cache in [cache_manager.table_names_cache, cache_manager.table_data_cache,
                              cache_manager.table_schema_cache, cache_manager.query_cache]:
                    cleared_count += await cache.clear_pattern(pattern)
                return f"Cleared {cleared_count} cache entries matching pattern: '{pattern}'"
            else:
                # Clear all caches
                await cache_manager.table_names_cache.clear()
                await cache_manager.table_data_cache.clear()
                await cache_manager.table_schema_cache.clear()
                await cache_manager.query_cache.clear()
                return "Cleared all cache entries"

        except Exception as e:
            error_msg = f"Error clearing cache: {str(e)}"
            logger.error(error_msg)
            return error_msg

    @staticmethod
    async def invalidate_table_cache(table_name: str = None) -> str:
        """Invalidate cache for specific table or all tables."""
        try:
            logger.info(f"Invalidating cache for table: {table_name if table_name else 'all tables'}")

            await AsyncDatabaseOperations.invalidate_caches(table_name)

            if table_name:
                return f"Invalidated cache for table: {table_name}"
            else:
                return "Invalidated cache for all tables"

        except Exception as e:
            error_msg = f"Error invalidating cache: {str(e)}"
            logger.error(error_msg)
            return error_msg

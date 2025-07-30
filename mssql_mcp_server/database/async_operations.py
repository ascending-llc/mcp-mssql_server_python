import time
from typing import List, Tuple, Any, Dict, Optional
from dataclasses import dataclass

from fastmcp.server.dependencies import get_context

from mssql_mcp_server.database.async_connection import get_pool
from mssql_mcp_server.config.settings import settings
from mssql_mcp_server.utils.logger import Logger
from mssql_mcp_server.utils.exceptions import DatabaseOperationError
from mssql_mcp_server.utils.validators import SQLValidator
from mssql_mcp_server.utils.cache import cache_manager

logger = Logger.get_logger(__name__)


@dataclass
class QueryResult:
    """Result of a database query."""

    columns: List[str]
    rows: List[List[Any]]
    row_count: int
    execution_time: float
    query_type: str

    def to_csv(self) -> str:
        """Convert result to CSV format."""
        if not self.rows:
            return ""

        lines = [",".join(self.columns)]
        for row in self.rows:
            formatted_row = []
            for cell in row:
                if cell is None:
                    formatted_row.append("")
                else:
                    cell_str = str(cell)
                    # Escape commas and quotes
                    if "," in cell_str or '"' in cell_str:
                        cell_str = f'"{cell_str.replace(chr(34), chr(34) + chr(34))}"'
                    formatted_row.append(cell_str)
            lines.append(",".join(formatted_row))

        return "\n".join(lines)


class AsyncDatabaseOperations:
    """Async database operations handler."""

    @staticmethod
    async def get_table_names() -> List[str]:
        """Get list of all table names in the database with caching."""
        return await AsyncDatabaseOperations._get_object_names("table")

    @staticmethod
    async def get_view_names() -> List[str]:
        """Get list of all view names in the database with caching."""
        return await AsyncDatabaseOperations._get_object_names("view")

    @staticmethod
    async def _get_object_names(object_type: str) -> List[str]:
        """Internal method to get table or view names with schema information and caching."""
        if object_type == "table":
            cached_objects = await cache_manager.get_table_names()
        else:
            cached_objects = await cache_manager.get_view_names()

        if cached_objects is not None:
            logger.debug(f"Using cached {object_type} names: {len(cached_objects)} {object_type}s")
            return cached_objects

        try:
            pool = await get_pool()
            async with pool.get_connection() as conn:
                async with conn.cursor() as cursor:
                    if object_type == "table":
                        query = """
                                SELECT SCHEMA_NAME(schema_id) + '.' + name as full_name
                                FROM sys.tables
                                ORDER BY SCHEMA_NAME(schema_id), name \
                                """
                    else:  # view
                        query = """
                                SELECT SCHEMA_NAME(schema_id) + '.' + name as full_name
                                FROM sys.views
                                ORDER BY SCHEMA_NAME(schema_id), name \
                                """

                    await cursor.execute(query)
                    objects = await cursor.fetchall()
                    object_names = [obj[0] for obj in objects]

                    # Cache the result
                    if object_type == "table":
                        await cache_manager.set_table_names(object_names)
                    else:
                        await cache_manager.set_view_names(object_names)

                    logger.info(f"Fetched and cached {len(object_names)} {object_type} names with schemas")
                    return object_names

        except Exception as e:
            logger.error(f"Failed to get {object_type} names: {e}")
            raise DatabaseOperationError(f"Failed to retrieve {object_type} names: {e}")

    @staticmethod
    async def get_all_table_and_view_names() -> Dict[str, List[str]]:
        """Get both tables and views with their schemas."""
        try:
            tables = await AsyncDatabaseOperations.get_table_names()
            views = await AsyncDatabaseOperations.get_view_names()

            return {
                "tables": tables,
                "views": views
            }
        except Exception as e:
            logger.error(f"Failed to get tables and views: {e}")
            raise DatabaseOperationError(f"Failed to retrieve tables and views: {e}")

    @staticmethod
    async def get_table_data(table_name: str, limit: Optional[int] = None) -> QueryResult:
        """Get data from a specific table with caching."""
        return await AsyncDatabaseOperations.get_object_data(table_name, "table", limit)

    @staticmethod
    async def get_view_data(view_name: str, limit: Optional[int] = None) -> QueryResult:
        """Get data from a specific view with caching."""
        return await AsyncDatabaseOperations.get_object_data(view_name, "view", limit)

    @staticmethod
    async def get_object_data(object_name: str, object_type: str = "table", limit: Optional[int] = None) -> QueryResult:
        """Get data from a table or view with caching.
        
        Args:
            object_name: Full object name in format 'schema.objectname' (e.g., 'dbo.Users')
            object_type: Either 'table' or 'view'
            limit: Maximum number of rows to return
        """
        if limit is None or limit > settings.server.max_rows_limit:
            limit = settings.server.max_rows_limit

        # Validate that object_name contains schema
        if '.' not in object_name:
            raise DatabaseOperationError(
                f"Object name must include schema: '{object_name}' should be 'schema.{object_name}'")

        schema_name, table_name = object_name.split('.', 1)

        # Check cache first
        cache_key = f"{object_type}_{object_name}_{limit}" if object_type == "view" else f"{object_name}_{limit}"
        cached_data = await cache_manager.get_table_data(cache_key)
        if cached_data is not None:
            logger.debug(f"Using cached data for {object_type}: {object_name}")
            # Parse cached CSV back to QueryResult
            lines = cached_data.split('\n')
            if lines:
                columns = lines[0].split(',')
                rows = [line.split(',') for line in lines[1:]] if len(lines) > 1 else []
                return QueryResult(
                    columns=columns,
                    rows=rows,
                    row_count=len(rows),
                    execution_time=0.0,  # Cached result
                    query_type="cached_select"
                )
        ctx = get_context()
        start_time = time.time()

        # Validate object exists
        if object_type == "table":
            valid_objects = await AsyncDatabaseOperations.get_table_names()
        else:  # view
            valid_objects = await AsyncDatabaseOperations.get_view_names()

        if object_name not in valid_objects:
            raise DatabaseOperationError(
                f"{object_type.title()} '{object_name}' not found. Available {object_type}s: {', '.join(valid_objects[:10])}")

        try:
            pool = await get_pool()
            async with pool.get_connection() as conn:
                async with conn.cursor() as cursor:
                    # Use proper schema.object notation
                    query = f"SELECT TOP {limit} * FROM [{schema_name}].[{table_name}]"
                    logger.debug(f"Executing query: {query}")
                    await cursor.execute(query)

                    # Get column names
                    columns = [desc[0] for desc in cursor.description]

                    # 懒加载：分批获取数据
                    rows_list = await AsyncDatabaseOperations._fetch_rows_lazy(cursor, max_rows=limit)

                    execution_time = time.time() - start_time
                    result = QueryResult(
                        columns=columns,
                        rows=rows_list,
                        row_count=len(rows_list),
                        execution_time=execution_time,
                        query_type="select"
                    )

                    # Cache the result
                    await cache_manager.set_table_data(cache_key, result.to_csv())
                    await ctx.report_progress(progress=result.row_count, total=result.row_count)
                    return result

        except Exception as e:
            logger.error(f"Failed to get {object_type} data for {object_name}: {e}")
            raise DatabaseOperationError(f"Failed to retrieve data from {object_type} '{object_name}': {e}")

    @staticmethod
    async def execute_query(query: str, allow_modifications: bool = False) -> QueryResult:
        """Execute an SQL query and return results."""
        start_time = time.time()

        # Validate query
        SQLValidator.validate_sql_query(query, allow_modifications)

        try:
            pool = await get_pool()
            async with pool.get_connection() as conn:
                async with conn.cursor() as cursor:
                    logger.info(f"Executing query: {query[:100]}...")

                    await cursor.execute(query)

                    # Handle different query types
                    query_upper = query.strip().upper()

                    if query_upper == "SHOW TABLES":
                        # Special handling for SHOW TABLES
                        table_names = await AsyncDatabaseOperations.get_table_names()
                        execution_time = time.time() - start_time
                        return QueryResult(
                            columns=[f"Tables_in_{settings.async_database.database}"],
                            rows=[[table] for table in table_names],
                            row_count=len(table_names),
                            execution_time=execution_time,
                            query_type="show_tables"
                        )

                    elif query_upper.startswith("SELECT"):
                        # SELECT queries
                        columns = [desc[0] for desc in cursor.description] if cursor.description else []

                        # 懒加载SELECT结果
                        rows_list = await AsyncDatabaseOperations._fetch_rows_lazy(cursor)

                        # Calculate execution time AFTER fetching all rows
                        execution_time = time.time() - start_time

                        return QueryResult(
                            columns=columns,
                            rows=rows_list,
                            row_count=len(rows_list),
                            execution_time=execution_time,
                            query_type="select"
                        )

                    else:
                        # Modification queries (INSERT, UPDATE, DELETE, etc.)
                        if allow_modifications:
                            await conn.commit()

                            # Invalidate related caches for DDL operations
                            if any(keyword in query_upper for keyword in ["CREATE", "DROP", "ALTER"]):
                                await cache_manager.invalidate_table_related()
                                logger.info("Invalidated table caches due to DDL operation")

                            row_count = cursor.rowcount if hasattr(cursor, 'rowcount') else 0
                            execution_time = time.time() - start_time
                            return QueryResult(
                                columns=["rows_affected"],
                                rows=[[row_count]],
                                row_count=row_count,
                                execution_time=execution_time,
                                query_type="modification"
                            )
                        else:
                            raise DatabaseOperationError("Modification queries are not allowed")

        except Exception as e:
            logger.error(f"Database error executing query: {e}")
            raise DatabaseOperationError(f"Query execution failed: {e}")

    @staticmethod
    async def get_table_schema(table_name: str) -> List[Dict[str, Any]]:
        """Get schema information for a specific table with caching."""
        return await AsyncDatabaseOperations.get_object_schema(table_name, "table")

    @staticmethod
    async def get_view_schema(view_name: str) -> List[Dict[str, Any]]:
        """Get schema information for a specific view with caching."""
        return await AsyncDatabaseOperations.get_object_schema(view_name, "view")

    @staticmethod
    async def get_object_schema(object_name: str, object_type: str = "table") -> List[Dict[str, Any]]:
        """Get schema information for a table or view with caching.
        
        Args:
            object_name: Full object name in format 'schema.objectname' (e.g., 'dbo.Users')
            object_type: Either 'table' or 'view'
        """
        # Validate that object_name contains schema
        if '.' not in object_name:
            raise DatabaseOperationError(
                f"Object name must include schema: '{object_name}' should be 'schema.{object_name}'")

        schema_name, table_name = object_name.split('.', 1)

        # Check cache first
        cache_key = f"{object_type}_schema_{object_name}" if object_type == "view" else f"table_schema_{object_name}"
        cached_schema = await cache_manager.get_table_schema(cache_key)
        if cached_schema is not None:
            logger.debug(f"Using cached schema for {object_type}: {object_name}")
            # Parse cached CSV back to schema info
            lines = cached_schema.split('\n')
            if len(lines) > 1:
                headers = lines[0].split(',')
                schema_info = []
                for line in lines[1:]:
                    values = line.split(',')
                    if len(values) >= len(headers):
                        schema_info.append({
                            "column_name": values[0],
                            "data_type": values[1],
                            "is_nullable": values[2],
                            "default_value": values[3] if values[3] else None,
                            "max_length": int(values[4]) if values[4] and values[4].isdigit() else None,
                            "numeric_precision": int(values[5]) if values[5] and values[5].isdigit() else None,
                            "numeric_scale": int(values[6]) if values[6] and values[6].isdigit() else None
                        })
                return schema_info

        # Validate object exists
        if object_type == "table":
            valid_objects = await AsyncDatabaseOperations.get_table_names()
        else:  # view
            valid_objects = await AsyncDatabaseOperations.get_view_names()

        if object_name not in valid_objects:
            raise DatabaseOperationError(
                f"{object_type.title()} '{object_name}' not found. Available {object_type}s: {', '.join(valid_objects[:10])}")

        try:
            pool = await get_pool()
            async with pool.get_connection() as conn:
                async with conn.cursor() as cursor:
                    schema_query = """
                                   SELECT COLUMN_NAME,
                                          DATA_TYPE,
                                          IS_NULLABLE,
                                          COLUMN_DEFAULT,
                                          CHARACTER_MAXIMUM_LENGTH,
                                          NUMERIC_PRECISION,
                                          NUMERIC_SCALE
                                   FROM INFORMATION_SCHEMA.COLUMNS
                                   WHERE TABLE_SCHEMA = ?
                                     AND TABLE_NAME = ?
                                   ORDER BY ORDINAL_POSITION \
                                   """

                    await cursor.execute(schema_query, (schema_name, table_name))
                    columns = await cursor.fetchall()

                    if not columns:
                        raise DatabaseOperationError(
                            f"No schema information found for {object_type} '{object_name}' in schema '{schema_name}'")

                    schema_info = []
                    csv_lines = ["Column Name,Data Type,Is Nullable,Default Value,Max Length,Precision,Scale"]

                    for col in columns:
                        schema_dict = {
                            "column_name": col[0],
                            "data_type": col[1],
                            "is_nullable": col[2],
                            "default_value": col[3],
                            "max_length": col[4],
                            "numeric_precision": col[5],
                            "numeric_scale": col[6]
                        }
                        schema_info.append(schema_dict)

                        # Create CSV line for caching
                        csv_line = [
                            str(col[0]),
                            str(col[1]),
                            str(col[2]),
                            str(col[3]) if col[3] is not None else "",
                            str(col[4]) if col[4] is not None else "",
                            str(col[5]) if col[5] is not None else "",
                            str(col[6]) if col[6] is not None else ""
                        ]
                        csv_lines.append(",".join(csv_line))

                    # Cache the result
                    await cache_manager.set_table_schema(cache_key, "\n".join(csv_lines))

                    return schema_info

        except Exception as e:
            logger.error(f"Failed to get schema for {object_type} {object_name}: {e}")
            raise DatabaseOperationError(f"Failed to retrieve schema for {object_type} '{object_name}': {e}")

    @staticmethod
    async def test_connection() -> bool:
        """Test database connection."""
        try:
            pool = await get_pool()
            return await pool.test_connection()
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False

    @staticmethod
    async def get_database_info() -> Dict[str, Any]:
        """Get general database information."""
        try:
            pool = await get_pool()
            async with pool.get_connection() as conn:
                async with conn.cursor() as cursor:
                    # Get database version
                    await cursor.execute("SELECT @@VERSION")
                    version_info = await cursor.fetchone()

                    # Get database name
                    await cursor.execute("SELECT DB_NAME()")
                    db_name = await cursor.fetchone()

                    # Get table and view counts
                    table_names = await AsyncDatabaseOperations.get_table_names()
                    view_names = await AsyncDatabaseOperations.get_view_names()

                    return {
                        "database_name": db_name[0] if db_name else "Unknown",
                        "version": version_info[0] if version_info else "Unknown",
                        "table_count": len(table_names),
                        "view_count": len(view_names),
                        "total_objects": len(table_names) + len(view_names),
                        "connection_pool_info": pool.pool_info
                    }

        except Exception as e:
            logger.error(f"Failed to get database info: {e}")
            raise DatabaseOperationError(f"Failed to get database information: {e}")

    @staticmethod
    async def invalidate_caches(table_name: Optional[str] = None) -> None:
        """Invalidate caches for database changes."""
        await cache_manager.invalidate_table_related(table_name)
        logger.info(f"Caches invalidated for table: {table_name if table_name else 'all tables'}")

    @staticmethod
    async def _fetch_rows_lazy(cursor, max_rows: int = None) -> List[List[Any]]:
        """Lazy loading helper function for fetching rows in batches."""
        if max_rows is None:
            max_rows = settings.server.max_rows_limit
        batch_rows_size = settings.server.batch_rows_size

        # 智能选择策略：小数据集直接获取，大数据集分批获取
        if max_rows <= batch_rows_size:
            logger.info(f"Small dataset ({max_rows} rows), using direct fetch")
            rows = await cursor.fetchmany(max_rows)
            rows_list = [list(row) for row in rows]
            logger.info(f"Direct fetch completed: {len(rows_list)} rows loaded")
            return rows_list

        # 大数据集使用分批加载
        rows_list = []
        # 使用配置的batch_rows_size，但不超过max_rows，确保合理的批次数量
        batch_size = min(batch_rows_size, max_rows)
        total_rows = 0

        logger.info(f"Large dataset ({max_rows} rows), "
                    f"using lazy fetch with batch size {batch_size}"
                    f" (configured: {batch_rows_size})")
        ctx = get_context()

        while total_rows < max_rows:
            # 计算本次实际需要获取的行数
            remaining = max_rows - total_rows
            current_batch_size = min(batch_size, remaining)

            batch = await cursor.fetchmany(current_batch_size)
            if not batch:
                break
            batch_list = [list(row) for row in batch]
            rows_list.extend(batch_list)
            total_rows += len(batch)
            await ctx.report_progress(progress=total_rows, total=max_rows,
                                      message=f"Loaded {total_rows / max_rows * 100:.1f}%")
            logger.info(f" Loaded {total_rows / max_rows * 100:.1f}% of total rows ({total_rows} loaded)")
            time.sleep(5)
        logger.info(f"Lazy fetch completed: {total_rows} rows loaded")
        return rows_list

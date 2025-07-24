import time
from typing import List, Tuple, Any, Dict, Optional
from dataclasses import dataclass

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
                        cell_str = f'"{cell_str.replace(chr(34), chr(34)+chr(34))}"'
                    formatted_row.append(cell_str)
            lines.append(",".join(formatted_row))
        
        return "\n".join(lines)


class AsyncDatabaseOperations:
    """Async database operations handler."""

    @staticmethod
    async def get_table_names() -> List[str]:
        """Get list of all table names in the database with caching."""
        # Check cache first
        cached_tables = await cache_manager.get_table_names()
        if cached_tables is not None:
            logger.debug(f"Using cached table names: {len(cached_tables)} tables")
            return cached_tables

        try:
            pool = await get_pool()
            async with pool.get_connection() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute(
                        "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
                        "WHERE TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_NAME"
                    )
                    tables = await cursor.fetchall()
                    table_names = [table[0] for table in tables]
                    
                    # Cache the result
                    await cache_manager.set_table_names(table_names)
                    logger.info(f"Fetched and cached {len(table_names)} table names")
                    return table_names

        except Exception as e:
            logger.error(f"Failed to get table names: {e}")
            raise DatabaseOperationError(f"Failed to retrieve table names: {e}")

    @staticmethod
    async def get_table_data(table_name: str, limit: Optional[int] = None) -> QueryResult:
        """Get data from a specific table with caching."""
        if limit is None:
            limit = settings.server.max_rows_limit

        # Check cache first
        cache_key = f"{table_name}_{limit}"
        cached_data = await cache_manager.get_table_data(cache_key)
        if cached_data is not None:
            logger.debug(f"Using cached data for table: {table_name}")
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

        start_time = time.time()
        
        # Validate table name
        valid_tables = await AsyncDatabaseOperations.get_table_names()
        SQLValidator.validate_table_name(table_name, valid_tables)

        try:
            pool = await get_pool()
            async with pool.get_connection() as conn:
                async with conn.cursor() as cursor:
                    query = f"SELECT TOP {limit} * FROM [{table_name}]"
                    await cursor.execute(query)

                    # Get column names
                    columns = [desc[0] for desc in cursor.description]

                    # Get data rows
                    rows = await cursor.fetchall()
                    rows_list = [list(row) for row in rows]

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
                    
                    return result

        except Exception as e:
            logger.error(f"Failed to get table data for {table_name}: {e}")
            raise DatabaseOperationError(f"Failed to retrieve data from table '{table_name}': {e}")

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
                    execution_time = time.time() - start_time

                    if query_upper == "SHOW TABLES":
                        # Special handling for SHOW TABLES
                        table_names = await AsyncDatabaseOperations.get_table_names()
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
                        rows = await cursor.fetchall()
                        rows_list = [list(row) for row in rows]
                        
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
            execution_time = time.time() - start_time
            logger.error(f"Database error executing query: {e}")
            raise DatabaseOperationError(f"Query execution failed: {e}")

    @staticmethod
    async def get_table_schema(table_name: str) -> List[Dict[str, Any]]:
        """Get schema information for a specific table with caching."""
        # Check cache first
        cached_schema = await cache_manager.get_table_schema(table_name)
        if cached_schema is not None:
            logger.debug(f"Using cached schema for table: {table_name}")
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

        # Validate table name
        valid_tables = await AsyncDatabaseOperations.get_table_names()
        SQLValidator.validate_table_name(table_name, valid_tables)

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
                    WHERE TABLE_NAME = ?
                    ORDER BY ORDINAL_POSITION
                    """

                    await cursor.execute(schema_query, (table_name,))
                    columns = await cursor.fetchall()

                    if not columns:
                        raise DatabaseOperationError(f"No schema information found for table '{table_name}'")

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
                    await cache_manager.set_table_schema(table_name, "\n".join(csv_lines))
                    
                    return schema_info

        except Exception as e:
            logger.error(f"Failed to get schema for table {table_name}: {e}")
            raise DatabaseOperationError(f"Failed to retrieve schema for table '{table_name}': {e}")

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
                    
                    # Get table count
                    table_names = await AsyncDatabaseOperations.get_table_names()
                    
                    return {
                        "database_name": db_name[0] if db_name else "Unknown",
                        "version": version_info[0] if version_info else "Unknown",
                        "table_count": len(table_names),
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
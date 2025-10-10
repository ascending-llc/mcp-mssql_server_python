import json
from pathlib import Path
from mssql_mcp_server.database.async_operations import AsyncDatabaseOperations
from mssql_mcp_server.utils.logger import Logger
from mssql_mcp_server.utils.exceptions import DatabaseOperationError

logger = Logger.get_logger(__name__)
current_dir = Path(__file__).parent.parent.parent
column_resources_path = current_dir / "data" / "das-column-resources.sql"
table_resources_path = current_dir / "data" / "das-table-resources.sql"


class AsyncResourceHandlers:
    """Async MCP resource handlers with dynamic resource generation."""

    @staticmethod
    async def get_ai_views_column_descriptions():
        sql = column_resources_path.read_text()
        logger.info(f"Getting AI views column descriptions: {sql}")
        return await AsyncDatabaseOperations.execute_query(sql)

    @staticmethod
    async def get_ai_views_table_descriptions():
        sql = table_resources_path.read_text()
        logger.info(f"Getting AI views table descriptions: {sql}")
        return await AsyncDatabaseOperations.execute_query(sql)

    @staticmethod
    async def read_object_data(object_name: str, object_type: str = "table", limit: int = 100) -> str:
        """Read data from a specific table or view."""
        try:
            logger.info(f"Reading data from {object_type}: {object_name}")

            result = await AsyncDatabaseOperations.get_object_data(object_name, object_type, limit)

            if result.row_count == 0:
                return f"{object_type.title()} '{object_name}' is empty."

            csv_data = result.to_csv()
            logger.info(
                f"Retrieved {result.row_count} rows from {object_type} {object_name} in {result.execution_time:.3f}s")
            return csv_data

        except DatabaseOperationError as e:
            logger.error(f"Failed to read {object_type} {object_name}: {e}")
            return f"Error: {str(e)}"
        except Exception as e:
            logger.error(f"Unexpected error reading {object_type} {object_name}: {e}")
            return f"Unexpected error: {str(e)}"

    @staticmethod
    async def read_object_schema(object_name: str, object_type: str = "table") -> str:
        """Read schema information for a specific table or view."""
        try:
            logger.info(f"Reading schema for {object_type}: {object_name}")

            schema_info = await AsyncDatabaseOperations.get_object_schema(object_name, object_type)

            if not schema_info:
                return f"No schema information found for {object_type} '{object_name}'"

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
            logger.error(f"Failed to get schema for {object_type} {object_name}: {e}")
            return f"Error: {str(e)}"
        except Exception as e:
            logger.error(f"Unexpected error getting schema for {object_type} {object_name}: {e}")
            return f"Unexpected error: {str(e)}"

    @staticmethod
    async def list_database_tables() -> str:
        """List all tables in the database."""
        try:
            table_names = await AsyncDatabaseOperations.get_table_names()
            logger.info(f"Found {len(table_names)} tables")

            if not table_names:
                return "No tables found in the database."

            return "\n".join([f"Table: {table}" for table in table_names])

        except DatabaseOperationError as e:
            logger.error(f"Failed to list tables: {e}")
            return f"Error: {str(e)}"
        except Exception as e:
            logger.error(f"Unexpected error listing tables: {e}")
            return f"Unexpected error: {str(e)}"

    @staticmethod
    async def list_database_views() -> str:
        """List all views in the database."""
        try:
            view_names = await AsyncDatabaseOperations.get_view_names()
            logger.info(f"Found {len(view_names)} views")

            if not view_names:
                return "No views found in the database."

            return "\n".join([f"View: {view}" for view in view_names])

        except DatabaseOperationError as e:
            logger.error(f"Failed to list views: {e}")
            return f"Error: {str(e)}"
        except Exception as e:
            logger.error(f"Unexpected error listing views: {e}")
            return f"Unexpected error: {str(e)}"

    @staticmethod
    async def get_database_info() -> str:
        """Get general database information."""
        try:
            db_info = await AsyncDatabaseOperations.get_database_info()
            return json.dumps(db_info, indent=2)

        except DatabaseOperationError as e:
            logger.error(f"Failed to get database info: {e}")
            return json.dumps({"error": str(e)}, indent=2)
        except Exception as e:
            logger.error(f"Unexpected error getting database info: {e}")
            return json.dumps({"error": f"Unexpected error: {str(e)}"}, indent=2)

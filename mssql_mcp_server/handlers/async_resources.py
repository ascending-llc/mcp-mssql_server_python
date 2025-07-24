from typing import List, Dict, Any, Optional
import json

from mssql_mcp_server.database.async_operations import AsyncDatabaseOperations
from mssql_mcp_server.config.settings import settings
from mssql_mcp_server.utils.logger import Logger
from mssql_mcp_server.utils.exceptions import DatabaseOperationError

logger = Logger.get_logger(__name__)


class AsyncResourceHandlers:
    """Async MCP resource handlers with dynamic resource generation."""

    @staticmethod
    async def generate_dynamic_resources() -> List[Dict[str, Any]]:
        """Generate dynamic resources for all tables and database info."""
        try:
            table_names = await AsyncDatabaseOperations.get_table_names()
            resources = []
            
            # Create resources for each table
            for table_name in table_names:
                # Table data resource
                resources.append({
                    "uri": f"mssql://table/{table_name}/data",
                    "name": f"Table Data: {table_name}",
                    "mimeType": "text/csv",
                    "description": f"Data from table {table_name} (top {settings.server.max_rows_limit} rows)"
                })
                
                # Table schema resource
                resources.append({
                    "uri": f"mssql://table/{table_name}/schema",
                    "name": f"Table Schema: {table_name}",
                    "mimeType": "text/plain",
                    "description": f"Schema information for table {table_name}"
                })
            
            # Database-level resources
            resources.extend([
                {
                    "uri": "mssql://database/tables",
                    "name": "Database Tables",
                    "mimeType": "text/plain",
                    "description": "List of all tables in the database"
                },
                {
                    "uri": "mssql://database/info",
                    "name": "Database Information",
                    "mimeType": "application/json",
                    "description": "General database information including version and connection details"
                }
            ])
            
            logger.info(f"Generated {len(resources)} dynamic resources")
            return resources
            
        except Exception as e:
            logger.error(f"Failed to generate dynamic resources: {e}")
            return []

    @staticmethod
    async def read_table_data(table_name: str) -> str:
        """Read data from a specific table."""
        try:
            logger.info(f"Reading data from table: {table_name}")
            
            result = await AsyncDatabaseOperations.get_table_data(table_name)
            
            if result.row_count == 0:
                return f"Table '{table_name}' is empty."
            
            csv_data = result.to_csv()
            logger.info(f"Retrieved {result.row_count} rows from table {table_name} in {result.execution_time:.3f}s")
            return csv_data

        except DatabaseOperationError as e:
            logger.error(f"Failed to read table {table_name}: {e}")
            return f"Error: {str(e)}"
        except Exception as e:
            logger.error(f"Unexpected error reading table {table_name}: {e}")
            return f"Unexpected error: {str(e)}"

    @staticmethod
    async def read_table_schema(table_name: str) -> str:
        """Read schema information for a specific table."""
        try:
            logger.info(f"Reading schema for table: {table_name}")
            
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
            logger.error(f"Failed to get schema for table {table_name}: {e}")
            return f"Error: {str(e)}"
        except Exception as e:
            logger.error(f"Unexpected error getting schema for table {table_name}: {e}")
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

    @staticmethod
    async def route_resource_request(uri: str) -> str:
        """Route resource requests to appropriate handlers."""
        try:
            logger.debug(f"Routing resource request: {uri}")
            
            if not uri.startswith("mssql://"):
                return "Error: Invalid URI scheme. Expected 'mssql://'."
            
            # Remove scheme and parse path
            path = uri[8:]  # Remove "mssql://"
            path_parts = path.split('/')
            
            if len(path_parts) < 2:
                return "Error: Invalid URI format."
            
            resource_type = path_parts[0]
            
            if resource_type == "table":
                if len(path_parts) < 3:
                    return "Error: Table resource requires table name and resource type."
                
                table_name = path_parts[1]
                resource_subtype = path_parts[2]
                
                if resource_subtype == "data":
                    return await AsyncResourceHandlers.read_table_data(table_name)
                elif resource_subtype == "schema":
                    return await AsyncResourceHandlers.read_table_schema(table_name)
                else:
                    return f"Error: Unknown table resource subtype: {resource_subtype}"
            
            elif resource_type == "database":
                if len(path_parts) < 2:
                    return "Error: Database resource requires resource type."
                
                database_subtype = path_parts[1]
                
                if database_subtype == "tables":
                    return await AsyncResourceHandlers.list_database_tables()
                elif database_subtype == "info":
                    return await AsyncResourceHandlers.get_database_info()
                else:
                    return f"Error: Unknown database resource subtype: {database_subtype}"
            
            else:
                return f"Error: Unknown resource type: {resource_type}"
        
        except Exception as e:
            logger.error(f"Error routing resource request for {uri}: {e}")
            return f"Error: Failed to process resource request: {str(e)}"

    @staticmethod
    async def validate_resource_access(uri: str) -> bool:
        """Validate if a resource can be accessed."""
        try:
            if not uri.startswith("mssql://"):
                return False
            
            path = uri[8:]
            path_parts = path.split('/')
            
            if len(path_parts) < 2:
                return False
            
            resource_type = path_parts[0]
            
            if resource_type == "table":
                if len(path_parts) < 3:
                    return False
                
                table_name = path_parts[1]
                resource_subtype = path_parts[2]
                
                # Check if table exists
                table_names = await AsyncDatabaseOperations.get_table_names()
                if table_name not in table_names:
                    return False
                
                # Check if resource subtype is valid
                return resource_subtype in ["data", "schema"]
            
            elif resource_type == "database":
                if len(path_parts) < 2:
                    return False
                
                database_subtype = path_parts[1]
                return database_subtype in ["tables", "info"]
            
            return False
            
        except Exception as e:
            logger.error(f"Error validating resource access for {uri}: {e}")
            return False

    @staticmethod
    async def get_resource_metadata(uri: str) -> Optional[Dict[str, Any]]:
        """Get metadata for a specific resource."""
        try:
            if not await AsyncResourceHandlers.validate_resource_access(uri):
                return None
            
            path = uri[8:]
            path_parts = path.split('/')
            resource_type = path_parts[0]
            
            if resource_type == "table":
                table_name = path_parts[1]
                resource_subtype = path_parts[2]
                
                # Get table-specific metadata
                table_names = await AsyncDatabaseOperations.get_table_names()
                if table_name not in table_names:
                    return None
                
                base_metadata = {
                    "table_name": table_name,
                    "resource_type": resource_subtype,
                    "uri": uri
                }
                
                if resource_subtype == "data":
                    # Could add row count, size estimates, etc.
                    base_metadata.update({
                        "description": f"Data from table {table_name}",
                        "mime_type": "text/csv"
                    })
                elif resource_subtype == "schema":
                    base_metadata.update({
                        "description": f"Schema for table {table_name}",
                        "mime_type": "text/plain"
                    })
                
                return base_metadata
            
            elif resource_type == "database":
                database_subtype = path_parts[1]
                return {
                    "resource_type": database_subtype,
                    "uri": uri,
                    "description": f"Database {database_subtype} information",
                    "mime_type": "application/json" if database_subtype == "info" else "text/plain"
                }
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting resource metadata for {uri}: {e}")
            return None 
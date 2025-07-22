import logging
import os
from typing import List, Dict, Any, cast

from fastmcp.server.server import Transport
from pyodbc import connect, Error
from fastmcp import FastMCP
from dotenv import load_dotenv

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("fastmcp_mssql_server")


def get_db_config():
    """Get database configuration from environment variables."""
    config = {
        "driver": os.getenv("MSSQL_DRIVER", "SQL Server"),
        "server": os.getenv("MSSQL_HOST", "localhost"),
        "user": os.getenv("MSSQL_USER"),
        "password": os.getenv("MSSQL_PASSWORD"),
        "database": os.getenv("MSSQL_DATABASE"),
        "trusted_server_certificate": os.getenv("TrustServerCertificate", "yes"),
        "trusted_connection": os.getenv("Trusted_Connection", "no")
    }
    if not all([config["user"], config["password"], config["database"]]):
        logger.error("Missing required database configuration. Please check environment variables:")
        logger.error("MSSQL_USER, MSSQL_PASSWORD, and MSSQL_DATABASE are required")
        raise ValueError("Missing required database configuration")

    connection_string = f"Driver={config['driver']};Server={config['server']};UID={config['user']};PWD={config['password']};Database={config['database']};TrustServerCertificate={config['trusted_server_certificate']};Trusted_Connection={config['trusted_connection']};"
    return config, connection_string


def get_table_names() -> List[str]:
    """Get list of table names from the database."""
    try:
        config, connection_string = get_db_config()
        with connect(connection_string, timeout=60) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE';")
                tables = cursor.fetchall()
                return [table[0] for table in tables]
    except Error as e:
        logger.error(f"Failed to get table names: {str(e)}")
        return []


# Initialize FastMCP server
mcp = FastMCP("mssql_mcp_server")


@mcp.resource("mssql://tables")
def list_database_tables() -> str:
    """List all tables in the database."""
    try:
        table_names = get_table_names()
        logger.info(f"Found tables: {table_names}")
        return "\n".join([f"Table: {table}" for table in table_names])
    except Exception as e:
        logger.error(f"Failed to list tables: {str(e)}")
        return f"Error: {str(e)}"


@mcp.resource("mssql://table/{table_name}")
def read_table_data(table_name: str) -> str:
    """Read data from a specific table (limited to first 100 rows)."""
    try:
        config, connection_string = get_db_config()
        logger.info(f"Reading data from table: {table_name}")

        with connect(connection_string, timeout=60) as conn:
            with conn.cursor() as cursor:
                # Use parameterized query to prevent SQL injection
                # Note: table names cannot be parameterized, so we validate against existing tables
                table_names = get_table_names()
                if table_name not in table_names:
                    raise ValueError(f"Table '{table_name}' not found in database")

                cursor.execute(f"SELECT TOP 100 * FROM [{table_name}]")
                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()

                # Format as CSV
                result_lines = [",".join(columns)]
                for row in rows:
                    result_lines.append(",".join([str(cell) if cell is not None else "" for cell in row]))

                return "\n".join(result_lines)

    except Error as e:
        logger.error(f"Database error reading table {table_name}: {str(e)}")
        return f"Database error: {str(e)}"
    except Exception as e:
        logger.error(f"Error reading table {table_name}: {str(e)}")
        return f"Error: {str(e)}"


@mcp.tool()
def execute_sql(query: str) -> str:
    """
    Execute an SQL query on the MSSQL server.

    Args:
        query: The SQL query to execute

    Returns:
        Query results or execution status
    """
    try:
        config, connection_string = get_db_config()
        logger.info(f"Executing SQL query: {query}")

        with connect(connection_string, timeout=60) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)

                # Special handling for listing tables in MSSQL
                if query.strip().upper() == "SHOW TABLES":
                    cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE';")
                    tables = cursor.fetchall()
                    result = [f"Tables_in_{config['database']}"]  # Header
                    result.extend([table[0] for table in tables])
                    return "\n".join(result)

                # Regular SELECT queries
                elif query.strip().upper().startswith("SELECT"):
                    columns = [desc[0] for desc in cursor.description]
                    rows = cursor.fetchall()

                    # Format as CSV
                    result_lines = [",".join(columns)]
                    for row in rows:
                        result_lines.append(",".join([str(cell) if cell is not None else "" for cell in row]))

                    return "\n".join(result_lines)

                # Non-SELECT queries (INSERT, UPDATE, DELETE, etc.)
                else:
                    conn.commit()
                    return f"Query executed successfully. Rows affected: {cursor.rowcount}"

    except Error as e:
        error_msg = f"Database error executing query '{query}': {str(e)}"
        logger.error(error_msg)
        return error_msg
    except Exception as e:
        error_msg = f"Error executing query '{query}': {str(e)}"
        logger.error(error_msg)
        return error_msg


@mcp.tool()
def get_table_schema(table_name: str) -> str:
    """
    Get the schema information for a specific table.

    Args:
        table_name: Name of the table to describe

    Returns:
        Table schema information
    """
    try:
        config, connection_string = get_db_config()
        logger.info(f"Getting schema for table: {table_name}")

        # Validate table exists
        table_names = get_table_names()
        if table_name not in table_names:
            return f"Error: Table '{table_name}' not found in database"

        with connect(connection_string, timeout=60) as conn:
            with conn.cursor() as cursor:
                schema_query = """
                               SELECT COLUMN_NAME, \
                                      DATA_TYPE, \
                                      IS_NULLABLE, \
                                      COLUMN_DEFAULT, \
                                      CHARACTER_MAXIMUM_LENGTH
                               FROM INFORMATION_SCHEMA.COLUMNS
                               WHERE TABLE_NAME = ?
                               ORDER BY ORDINAL_POSITION \
                               """
                cursor.execute(schema_query, (table_name,))
                columns = cursor.fetchall()

                if not columns:
                    return f"No schema information found for table '{table_name}'"

                # Format schema information
                result_lines = ["Column Name,Data Type,Is Nullable,Default Value,Max Length"]
                for col in columns:
                    col_name, data_type, is_nullable, default_val, max_length = col
                    max_length_str = str(max_length) if max_length is not None else ""
                    default_str = str(default_val) if default_val is not None else ""
                    result_lines.append(f"{col_name},{data_type},{is_nullable},{default_str},{max_length_str}")

                return "\n".join(result_lines)

    except Error as e:
        error_msg = f"Database error getting schema for table '{table_name}': {str(e)}"
        logger.error(error_msg)
        return error_msg
    except Exception as e:
        error_msg = f"Error getting schema for table '{table_name}': {str(e)}"
        logger.error(error_msg)
        return error_msg


@mcp.tool()
def list_tables() -> List[str]:
    """
    Get a list of all tables in the database.

    Returns:
        List of table names
    """
    try:
        table_names = get_table_names()
        logger.info(f"Found {len(table_names)} tables")
        return table_names
    except Exception as e:
        logger.error(f"Error listing tables: {str(e)}")
        return [f"Error: {str(e)}"]


def main():
    # Validate database configuration on startup
    try:
        config, _ = get_db_config()
        logger.info(f"Starting FastMCP MSSQL server...")
        logger.info(f"Database config: {config['server']}/{config['database']} as {config['user']}")

        # Test database connection
        table_names = get_table_names()
        logger.info(f"Successfully connected to database with {len(table_names)} tables")

        # Run the FastMCP server
        transport = cast(Transport, os.getenv("MCP_TRANSPORT", "stdio"))
        mcp.run(transport=transport)

    except Exception as e:
        logger.error(f"Failed to start server: {str(e)}")
        raise

if __name__ == '__main__':
    main()
#!/usr/bin/env python3
"""
Main entry point for MSSQL MCP Server.
"""

import sys
import asyncio
from mssql_mcp_server.utils.logger import Logger

logger = Logger.get_logger(__name__)


def main():
    """Main entry point."""
    try:
        # Import and run the server
        from mssql_mcp_server.server import main as server_main
        
        logger.info("Starting MSSQL MCP Server...")
        asyncio.run(server_main())
        
    except KeyboardInterrupt:
        logger.info("Server stopped by user")
        print("\n👋 Server shutdown complete")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        print(f"\n❌ Server failed to start: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

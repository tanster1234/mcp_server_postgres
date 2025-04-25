# server/app.py
import logging

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("pg-mcp")

import sys
import os
sys.path.append("/app") 
# Configure logging
# configure_logging(level="DEBUG")
# logger = get_logger("pg-mcp")

root_logger = logging.getLogger()
root_logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stderr)
handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
root_logger.addHandler(handler)

# Import mcp instance
from server.config import mcp

# Import registration functions
from server.resources.schema import register_schema_resources
from server.resources.data import register_data_resources
from server.resources.extensions import register_extension_resources
from server.tools.connection import register_connection_tools
from server.tools.query import register_query_tools

# Register tools and resources with the MCP server
register_schema_resources()   # Schema-related resources (schemas, tables, columns)
register_extension_resources()
register_data_resources()     # Data-related resources (sample, rowcount, etc.)
register_connection_tools()  # Connection management tools
register_query_tools()

if __name__ == "__main__":
    logger.info("Starting MCP server with SSE transport")
    mcp.run(transport="sse")
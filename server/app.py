# server/app.py
from mcp.server.fastmcp.utilities.logging import configure_logging, get_logger
import logging
import sys

# Configure logging
configure_logging(level="DEBUG")
logger = get_logger("pg-mcp")

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
from server.tools.query import register_query_tool

# Register tools and resources with the MCP server
register_schema_resources()
register_data_resources()
register_query_tool()

if __name__ == "__main__":
    logger.info("Starting MCP server with SSE transport")
    mcp.run(transport="sse")
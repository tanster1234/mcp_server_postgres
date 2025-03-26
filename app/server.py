# app/server.py
from mcp.server.fastmcp import FastMCP
import logging
import sys
from contextlib import asynccontextmanager
from mcp.server.fastmcp.utilities.logging import configure_logging, get_logger

# Import database and registration functions
from app.database import Database
from app.resources.schema import register_schema_resources
from app.resources.data import register_data_resources
from app.tools.query import register_query_tool

# Configure logging
configure_logging(level="DEBUG")
logger = get_logger("pg-mcp")

root_logger = logging.getLogger()
root_logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stderr)
handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
root_logger.addHandler(handler)

# Create database manager without initializing any connections
db = Database()

# Define lifespan context manager for initialization and cleanup
@asynccontextmanager
async def lifespan(app: FastMCP):
    # Startup logic
    logger.info("Database manager initialized (no connections established yet)")
    
    yield  # Server runs here
    
    # Shutdown logic
    logger.info("Shutting down all database connections")
    await db.close()  # Close all connections

# Create MCP instance with lifespan
mcp = FastMCP("pg-mcp", debug=True, lifespan=lifespan)

# Register tools and resources with dependency injection
register_schema_resources(mcp, db)
register_data_resources(mcp, db)
register_query_tool(mcp, db)

if __name__ == "__main__":
    logger.info("Starting MCP server with SSE transport")
    mcp.run(
        transport="sse"
    )
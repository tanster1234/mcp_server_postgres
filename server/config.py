# server/config.py
from mcp.server.fastmcp import FastMCP
from contextlib import asynccontextmanager
from collections.abc import AsyncIterator
from server.database import Database
from mcp.server.fastmcp.utilities.logging import get_logger

logger = get_logger("pg-mcp.instance")

# Define lifespan context manager for initialization and cleanup
@asynccontextmanager
async def app_lifespan(server: FastMCP) -> AsyncIterator[dict]:
    """Manage application lifecycle."""
    # Startup logic
    db = Database()
    logger.info("Database manager initialized (no connections established yet)")
    mcp.state = {"db": db}
    
    try:
        yield {"db": db}
    finally:
        # Shutdown logic
        logger.info("Shutting down all database connections")
        await db.close()  # Close all connections

# Create and expose the MCP instance
mcp = FastMCP(
    "pg-mcp", 
    debug=True, 
    lifespan=app_lifespan,
    dependencies=["asyncpg", "mcp"]
)
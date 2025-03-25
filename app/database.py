# app/database.py
import asyncpg
import os
from contextlib import asynccontextmanager
from mcp.server.fastmcp.utilities.logging import get_logger

logger = get_logger("pg-mcp.database")

class Database:
    def __init__(self, connection_string=None):
        self._pool = None
        self._connection_string = connection_string or os.getenv("DATABASE_URL")
        if not self._connection_string:
            raise ValueError("Database connection string is required")
    
    async def initialize(self):
        """Initialize the connection pool."""
        if self._pool is None:
            logger.info("Creating new database connection pool")
            self._pool = await asyncpg.create_pool(
                self._connection_string,
                min_size=2,
                max_size=10,
                command_timeout=60.0,
                # Read-only mode
                server_settings={"default_transaction_read_only": "true"}
            )
        return self
    
    @asynccontextmanager
    async def get_connection(self):
        """Get a database connection from the pool."""
        if self._pool is None:
            await self.initialize()
        
        async with self._pool.acquire() as conn:
            yield conn
    
    async def close(self):
        """Close the database connection pool."""
        if self._pool:
            logger.info("Closing database connection pool")
            await self._pool.close()
            self._pool = None
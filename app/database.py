# app/database.py
import asyncpg
from contextlib import asynccontextmanager
from mcp.server.fastmcp.utilities.logging import get_logger

logger = get_logger("pg-mcp.database")

class Database:
    def __init__(self):
        """Initialize the database manager with no default connections."""
        self._pools = {}  # Dictionary to store connection pools by connection string
    
    async def initialize(self, connection_string):
        """Initialize a connection pool for the given connection string."""
        if not connection_string:
            raise ValueError("Database connection string is required")
            
        if connection_string not in self._pools:
            logger.info(f"Creating new database connection pool for {connection_string[:10]}...")
            self._pools[connection_string] = await asyncpg.create_pool(
                connection_string,
                min_size=2,
                max_size=10,
                command_timeout=60.0,
                # Read-only mode
                server_settings={"default_transaction_read_only": "true"}
            )
        
        return self
    
    @asynccontextmanager
    async def get_connection(self, connection_string):
        """Get a database connection from the pool for the given connection string."""
        if not connection_string:
            raise ValueError("Database connection string is required")
            
        if connection_string not in self._pools:
            await self.initialize(connection_string)
        
        async with self._pools[connection_string].acquire() as conn:
            yield conn
    
    async def close(self, connection_string=None):
        """
        Close a specific or all database connection pools.
        
        Args:
            connection_string: If provided, close only this specific connection pool.
                              If None, close all connection pools.
        """
        if connection_string:
            if connection_string in self._pools:
                logger.info(f"Closing database connection pool for {connection_string[:10]}...")
                await self._pools[connection_string].close()
                del self._pools[connection_string]
        else:
            # Close all connection pools
            logger.info("Closing all database connection pools")
            for conn_string, pool in list(self._pools.items()):
                await pool.close()
                del self._pools[conn_string]
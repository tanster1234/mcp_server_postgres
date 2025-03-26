# app/tools/query.py
from mcp.server.fastmcp.utilities.logging import get_logger

logger = get_logger("pg-mcp.tools.query")

async def execute_query(db, query, connection_string, params=None):
    """Execute a read-only query and return the results."""
    async with db.get_connection(connection_string) as conn:
        # Ensure we're in read-only mode
        await conn.execute("SET TRANSACTION READ ONLY")
        
        # Execute the query
        try:
            records = await conn.fetch(query, *(params or []))
            return [dict(record) for record in records]
        except Exception as e:
            # Log the error but don't couple to specific error types
            logger.error(f"Query execution error: {e}")
            raise

def register_query_tool(mcp, db):
    """Register database query tools with the MCP server."""
    logger.debug("Registering query tool")
    
    @mcp.tool()
    async def pg_query(query, connection_string, params=None):
        """
        Execute a read-only SQL query against the PostgreSQL database.
        
        Args:
            query: The SQL query to execute (must be read-only)
            connection_string: PostgreSQL connection string (required)
            params: Parameters for the query (optional)
            
        Returns:
            Query results as a list of dictionaries
        """
        # Make sure connection_string starts with postgresql://
        if connection_string and not connection_string.startswith("postgresql://"):
            connection_string = f"postgresql://{connection_string}"
            
        logger.info(f"Executing query on connection {connection_string[:30]}...: {query}")
        return await execute_query(db, query, connection_string, params)
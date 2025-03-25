# app/tools/query.py
from mcp.server.fastmcp.utilities.logging import get_logger

logger = get_logger("pg-mcp.tools.query")

async def execute_query(db, query, params=None):
    """Execute a read-only query and return the results."""
    async with db.get_connection() as conn:
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
    async def pg_query(query, params=None):
        """
        Execute a read-only SQL query against the PostgreSQL database.
        
        Args:
            query: The SQL query to execute (must be read-only)
            params: Parameters for the query (optional)
            
        Returns:
            Query results as a list of dictionaries
        """
        logger.info(f"Executing query: {query}")
        return await execute_query(db, query, params)
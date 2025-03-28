# server/tools/query.py
from server.config import mcp
from mcp.server.fastmcp import Context
from mcp.server.fastmcp.utilities.logging import get_logger

logger = get_logger("pg-mcp.tools.query")

async def execute_query(query: str, conn_id: str, params=None, ctx=None):
    """
    Execute a read-only SQL query against the PostgreSQL database.
    
    Args:
        query: The SQL query to execute (must be read-only)
        conn_id: Connection ID (required)
        params: Parameters for the query (optional)
        ctx: Optional request context
        
    Returns:
        Query results as a list of dictionaries
    """
    
    # Access the database from the request context
    # Access the database from either context or MCP state
    if ctx is not None and hasattr(ctx, 'request_context'):
        db = ctx.request_context.lifespan_context["db"]
    else:
        from server.config import mcp
        db = mcp.state["db"]
        if db is None:
            raise ValueError("Database connection not available in context or MCP state.")
        
    logger.info(f"Executing query on connection ID {conn_id}: {query}")
    
    async with db.get_connection(conn_id) as conn:
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

def register_query_tool():
    """Register database query tools with the MCP server."""
    logger.debug("Registering query tool")
    
    @mcp.tool()
    async def pg_query(query: str, connection_string: str, params=None, *, ctx: Context):
        """
        Execute a read-only SQL query against the PostgreSQL database.
        
        Args:
            query: The SQL query to execute (must be read-only)
            connection_string: PostgreSQL connection string (required)
            params: Parameters for the query (optional)
            ctx: Request context (injected by the framework)
            
        Returns:
            Query results as a list of dictionaries
        """
        # Get database from context
        db = ctx.request_context.lifespan_context["db"]
        
        # Register the connection to get a connection ID
        conn_id = db.register_connection(connection_string)
        
        # Execute the query using the connection ID
        return await execute_query(query, conn_id, params, ctx)
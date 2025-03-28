# server/resources/data.py
from server.config import mcp
from mcp.server.fastmcp import Context
from mcp.server.fastmcp.utilities.logging import get_logger
from server.tools.query import execute_query

logger = get_logger("pg-mcp.resources.data")

def register_data_resources():
    """Register database data resources with the MCP server."""
    logger.debug("Registering data resources")
    
    @mcp.resource("pgmcp://{conn_id}/tables/{schema}/{table}/sample")
    async def sample_table_data(conn_id: str, schema: str, table: str):
        """Get a sample of data from a specific table."""
        # First, sanitize the schema and table names
        sanitize_query = "SELECT quote_ident($1) AS schema_ident, quote_ident($2) AS table_ident"
        identifiers = await execute_query(sanitize_query, conn_id, [schema, table])
        
        schema_ident = identifiers[0]['schema_ident']
        table_ident = identifiers[0]['table_ident']
        
        # Build the sample query with sanitized identifiers
        sample_query = f"SELECT * FROM {schema_ident}.{table_ident} LIMIT 10"
        return await execute_query(sample_query, conn_id)
    
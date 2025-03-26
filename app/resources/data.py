# app/resources/data.py
from mcp.server.fastmcp.utilities.logging import get_logger

logger = get_logger("pg-mcp.resources.data")

async def get_sample_data(db, schema, table, connection_string, limit=10):
    """Get sample data from a specific table."""
    async with db.get_connection(connection_string) as conn:
        # Sanitize schema and table names to prevent SQL injection
        # PostgreSQL identifiers can't be parameterized directly
        schema_ident = await conn.fetchval(
            "SELECT quote_ident($1)", schema
        )
        table_ident = await conn.fetchval(
            "SELECT quote_ident($1)", table
        )
        
        # Build and execute query
        query = f"SELECT * FROM {schema_ident}.{table_ident} LIMIT $1"
        return await conn.fetch(query, limit)

def register_data_resources(mcp, db):
    """Register database data resources with the MCP server."""
    logger.debug("Registering data resources")
    
    @mcp.resource("postgresql://{connection_string}/tables/{schema}/{table}/sample")
    async def sample_table_data(connection_string, schema, table):
        """Get a sample of data from a specific table."""
        full_connection_string = f"postgresql://{connection_string}"
        return await get_sample_data(db, schema, table, full_connection_string, 10)
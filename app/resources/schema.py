# app/resources/schema.py
from mcp.server.fastmcp.utilities.logging import get_logger

logger = get_logger("pg-mcp.resources.schema")

async def get_tables(db, connection_string):
    """Get all tables in the database with their descriptions."""
    async with db.get_connection(connection_string) as conn:
        return await conn.fetch("""
            SELECT 
                t.table_schema,
                t.table_name,
                obj_description(format('%s.%s', t.table_schema, t.table_name)::regclass::oid) as description
            FROM information_schema.tables t
            WHERE 
                t.table_schema NOT IN ('pg_catalog', 'information_schema') 
                AND t.table_type = 'BASE TABLE'
            ORDER BY t.table_schema, t.table_name
        """)

async def get_columns(db, schema, table, connection_string):
    """Get all columns for a table with their descriptions."""
    async with db.get_connection(connection_string) as conn:
        return await conn.fetch("""
            SELECT
                c.column_name,
                c.data_type,
                c.is_nullable,
                c.column_default,
                pgd.description
            FROM pg_catalog.pg_statio_all_tables AS st
            INNER JOIN pg_catalog.pg_description pgd ON (pgd.objoid = st.relid)
            INNER JOIN information_schema.columns c ON (
                pgd.objsubid = c.ordinal_position AND
                c.table_schema = st.schemaname AND
                c.table_name = st.relname
            )
            WHERE
                c.table_schema = $1 AND
                c.table_name = $2
            ORDER BY c.ordinal_position
        """, schema, table)

def register_schema_resources(mcp, db):
    """Register database schema resources with the MCP server."""
    logger.debug("Registering schema resources")
    
    
    @mcp.resource("postgresql://{connection_string}/tables")
    async def list_tables(connection_string):
        """List all tables in the database with their descriptions."""
        full_connection_string = f"postgresql://{connection_string}"
        return await get_tables(db, full_connection_string)
    
    @mcp.resource("postgresql://{connection_string}/tables/{schema}/{table}/columns")
    async def get_table_columns(connection_string, schema, table):
        """Get columns for a specific table with their descriptions."""
        full_connection_string = f"postgresql://{connection_string}"
        return await get_columns(db, schema, table, full_connection_string)
    